#include <Storages/MergeTree/VerticalInsertTask.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/TTLDescription.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Core/Settings.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils.h>
#include <Common/Stopwatch.h>
#include <base/scope_guard.h>

#include <deque>

namespace ProfileEvents
{
    extern const Event MergeTreeDataWriterRows;
    extern const Event MergeTreeDataWriterUncompressedBytes;
    extern const Event MergeTreeDataWriterCompressedBytes;
    extern const Event VerticalInsertRows;
    extern const Event VerticalInsertBytes;
    extern const Event VerticalInsertMergingColumns;
    extern const Event VerticalInsertGatheringColumns;
    extern const Event VerticalInsertWritersCreated;
    extern const Event VerticalInsertWriterFlushes;
    extern const Event VerticalInsertTotalMilliseconds;
    extern const Event VerticalInsertHorizontalPhaseMilliseconds;
    extern const Event VerticalInsertVerticalPhaseMilliseconds;
    extern const Event VerticalInsertSkipped;
}

namespace DB
{

namespace
{

void validateColumnRows(const ColumnWithTypeAndName & column, size_t expected_rows, const String & context)
{
    const size_t column_rows = column.column ? column.column->size() : 0;
    if (column_rows != expected_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Vertical insert {}: column {} has {} rows, expected {}",
            context,
            column.name,
            column_rows,
            expected_rows);
}

void addColumnNameOrBase(const String & name, const NameSet & storage_columns_name_set, NameSet & key_columns)
{
    if (storage_columns_name_set.contains(name))
        key_columns.insert(name);
    else
        key_columns.insert(String(Nested::getColumnFromSubcolumn(name, storage_columns_name_set)));
}

void addColumnsFromTTLDescription(const TTLDescription & ttl, const NameSet & storage_columns_name_set, NameSet & key_columns)
{
    for (const auto & column : ttl.expression_columns)
        addColumnNameOrBase(column.name, storage_columns_name_set, key_columns);

    for (const auto & column : ttl.where_expression_columns)
        addColumnNameOrBase(column.name, storage_columns_name_set, key_columns);

    for (const auto & name : ttl.group_by_keys)
        addColumnNameOrBase(name, storage_columns_name_set, key_columns);
}

void addColumnsFromTTLDescriptions(const TTLDescriptions & ttls, const NameSet & storage_columns_name_set, NameSet & key_columns)
{
    for (const auto & ttl : ttls)
        addColumnsFromTTLDescription(ttl, storage_columns_name_set, key_columns);
}

void addColumnsFromTTLColumnsDescription(const TTLColumnsDescription & ttls, const NameSet & storage_columns_name_set, NameSet & key_columns)
{
    for (const auto & [name, ttl] : ttls)
    {
        addColumnNameOrBase(name, storage_columns_name_set, key_columns);
        addColumnsFromTTLDescription(ttl, storage_columns_name_set, key_columns);
    }
}

void addNestedKeyColumns(const String & name, const NamesAndTypesList & storage_columns, NameSet & key_columns)
{
    const String nested_table = Nested::extractTableName(name);
    if (nested_table.empty())
        return;

    const String prefix = nested_table + ".";
    for (const auto & column : storage_columns)
    {
        if (startsWith(column.name, prefix))
            key_columns.insert(column.name);
    }
}

}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 enable_vertical_insert_algorithm;
    extern const MergeTreeSettingsUInt64 vertical_insert_algorithm_min_rows_to_activate;
    extern const MergeTreeSettingsUInt64 vertical_insert_algorithm_min_columns_to_activate;
    extern const MergeTreeSettingsUInt64 vertical_insert_algorithm_min_bytes_to_activate;
    extern const MergeTreeSettingsUInt64 vertical_insert_algorithm_columns_batch_size;
    extern const MergeTreeSettingsUInt64 min_bytes_for_wide_part;
    extern const MergeTreeSettingsUInt64 min_rows_for_wide_part;
    extern const MergeTreeSettingsBool fsync_after_insert;
}

namespace Setting
{
    extern const SettingsUInt64 max_insert_delayed_streams_for_parallel_write;
}

VerticalInsertTask::VerticalInsertTask(
    MergeTreeData & data_,
    MergeTreeSettingsPtr data_settings_,
    const StorageMetadataPtr & metadata_snapshot_,
    MergeTreeMutableDataPartPtr new_data_part_,
    Block block_,
    const IColumn::Permutation * permutation_,
    MergeTreeIndices skip_indexes_,
    ColumnsStatistics statistics_,
    CompressionCodecPtr codec_,
    MergeTreeIndexGranularityPtr index_granularity_,
    const Settings & settings_,
    ContextPtr context_)
    : data(data_)
    , data_settings(std::move(data_settings_))
    , metadata_snapshot(metadata_snapshot_)
    , new_data_part(std::move(new_data_part_))
    , block(std::move(block_))
    , block_rows(0)
    , block_bytes(0)
    , permutation(permutation_)
    , has_permutation(false)
    , permutation_is_identity(true)
    , skip_indexes(std::move(skip_indexes_))
    , statistics(std::move(statistics_))
    , codec(std::move(codec_))
    , index_granularity(std::move(index_granularity_))
    , settings(settings_)
    , context(context_)
    , log(getLogger("VerticalInsertTask"))
{
    block_rows = block.rows();
    block_bytes = block.bytes();
    if (permutation_ && !permutation_->empty())
    {
        permutation_is_identity = true;
        for (size_t i = 0, size = permutation_->size(); i < size; ++i)
        {
            if ((*permutation_)[i] != i)
            {
                permutation_is_identity = false;
                break;
            }
        }
        has_permutation = !permutation_is_identity;
    }

    all_column_names = new_data_part->getColumns().getNames();
    classifyColumns();

    LOG_DEBUG(log, "Vertical insert for part {}: {} merging columns, {} gathering columns, batch_size={}, has_permutation={}, block_rows={}, block_bytes={}",
        new_data_part->name,
        merging_columns.size(),
        gathering_columns.size(),
        settings.columns_batch_size,
        has_permutation,
        block_rows,
        block_bytes);
}

void VerticalInsertTask::classifyColumns()
{
    /// Get sorting key columns
    const auto & sorting_key_expr = metadata_snapshot->getSortingKey().expression;
    Names sort_key_columns_vec = sorting_key_expr->getRequiredColumns();

    /// Collect columns used in the sorting key expressions
    NameSet key_columns;
    auto storage_columns = metadata_snapshot->getColumns().getAllPhysical();
    auto storage_columns_name_set = storage_columns.getNameSet();

    for (const auto & name : sort_key_columns_vec)
    {
        if (storage_columns_name_set.contains(name))
            key_columns.insert(name);
        else
            key_columns.insert(String(Nested::getColumnFromSubcolumn(name, storage_columns_name_set)));

        addNestedKeyColumns(name, storage_columns, key_columns);
    }

    /// Force sign column for Collapsing mode and VersionedCollapsing mode
    const auto & merging_params = data.merging_params;
    if (!merging_params.sign_column.empty())
        key_columns.emplace(merging_params.sign_column);

    /// Force is_deleted column for Replacing mode
    if (!merging_params.is_deleted_column.empty())
        key_columns.emplace(merging_params.is_deleted_column);

    /// Force version column for Replacing mode and VersionedCollapsing mode
    if (!merging_params.version_column.empty())
        key_columns.emplace(merging_params.version_column);

    /// Force all columns params of Graphite mode
    if (merging_params.mode == MergeTreeData::MergingParams::Graphite)
    {
        key_columns.emplace(merging_params.graphite_params.path_column_name);
        key_columns.emplace(merging_params.graphite_params.time_column_name);
        key_columns.emplace(merging_params.graphite_params.value_column_name);
        key_columns.emplace(merging_params.graphite_params.version_column_name);
    }

    /// Force to merge at least one column in case of empty key
    if (key_columns.empty())
        key_columns.emplace(storage_columns.front().name);

    /// Include partition key columns to keep minmax index inputs in horizontal phase
    const auto minmax_columns = MergeTreeData::getMinMaxColumnsNames(metadata_snapshot->getPartitionKey());
    for (const auto & name : minmax_columns)
        addColumnNameOrBase(name, storage_columns_name_set, key_columns);

    /// Include TTL expression columns to keep TTL inputs in horizontal phase
    addColumnsFromTTLDescription(metadata_snapshot->getRowsTTL(), storage_columns_name_set, key_columns);
    addColumnsFromTTLDescriptions(metadata_snapshot->getRowsWhereTTLs(), storage_columns_name_set, key_columns);
    addColumnsFromTTLDescriptions(metadata_snapshot->getMoveTTLs(), storage_columns_name_set, key_columns);
    addColumnsFromTTLDescriptions(metadata_snapshot->getRecompressionTTLs(), storage_columns_name_set, key_columns);
    addColumnsFromTTLDescriptions(metadata_snapshot->getGroupByTTLs(), storage_columns_name_set, key_columns);
    addColumnsFromTTLColumnsDescription(metadata_snapshot->getColumnTTLs(), storage_columns_name_set, key_columns);

    /// Classify skip indexes: single-column ones go to vertical phase,
    /// multi-column ones go to horizontal phase
    for (const auto & index : skip_indexes)
    {
        const auto & index_desc = index->index;
        auto index_columns = index_desc.expression->getRequiredColumns();

        if (index_desc.type == "text")
        {
            if (index_columns.size() == 1)
            {
                /// Single-column text index can be built in vertical phase
                const auto & column_name = index_columns.front();
                String actual_column = storage_columns_name_set.contains(column_name)
                    ? column_name
                    : String(Nested::getColumnFromSubcolumn(column_name, storage_columns_name_set));
                skip_indexes_by_column[actual_column].push_back(index);
            }
            else
            {
                /// Text index on expression requires multiple columns, handle in horizontal phase
                for (const auto & index_column : index_columns)
                    addColumnNameOrBase(index_column, storage_columns_name_set, key_columns);
                multi_column_skip_indexes.push_back(index);
            }
        }
        else if (index_columns.size() == 1)
        {
            /// Single-column index can be built in vertical phase
            const auto & column_name = index_columns.front();
            String actual_column = storage_columns_name_set.contains(column_name)
                ? column_name
                : String(Nested::getColumnFromSubcolumn(column_name, storage_columns_name_set));
            skip_indexes_by_column[actual_column].push_back(index);
        }
        else
        {
            /// Multi-column index must be in horizontal phase
            for (const auto & index_column : index_columns)
            {
                if (storage_columns_name_set.contains(index_column))
                    key_columns.insert(index_column);
                else
                    key_columns.insert(String(Nested::getColumnFromSubcolumn(index_column, storage_columns_name_set)));
            }
            multi_column_skip_indexes.push_back(index);
        }
    }

    /// Classify statistics by column
    for (const auto & stat : statistics)
    {
        const String & stat_column = stat->getColumnName();
        statistics_by_column[stat_column].push_back(stat);
    }

    /// Partition columns into merging and gathering
    for (const auto & column : storage_columns)
    {
        if (!block.has(column.name))
            continue;  /// Skip columns not in this block

        if (key_columns.contains(column.name))
        {
            merging_columns.emplace_back(column);

            /// If column is in horizontal stage, move its indexes there too
            auto it = skip_indexes_by_column.find(column.name);
            if (it != skip_indexes_by_column.end())
            {
                for (auto & idx : it->second)
                    multi_column_skip_indexes.push_back(std::move(idx));
                skip_indexes_by_column.erase(it);
            }

            auto stat_it = statistics_by_column.find(column.name);
            if (stat_it != statistics_by_column.end())
            {
                for (auto & stat : stat_it->second)
                    merging_statistics.push_back(std::move(stat));
                statistics_by_column.erase(stat_it);
            }
        }
        else
        {
            gathering_columns.emplace_back(column);
        }
    }
}

void VerticalInsertTask::cancel() noexcept
{
    try
    {
        if (horizontal_output)
            horizontal_output->cancel();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Vertical insert: failed to cancel horizontal output");
    }

    try
    {
        if (active_column_writer)
            active_column_writer->cancel();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Vertical insert: failed to cancel active column writer");
    }
    active_column_writer = nullptr;

    for (auto & entry : delayed_streams)
    {
        try
        {
            if (entry.stream)
                entry.stream->cancel();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Vertical insert: failed to cancel delayed column writer");
        }
    }
    delayed_streams.clear();
}

void VerticalInsertTask::execute()
{
    if (has_permutation && permutation && permutation->size() != block_rows)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Vertical insert: permutation size {} does not match block rows {}",
            permutation->size(),
            block_rows);
    }

    ProfileEvents::increment(ProfileEvents::VerticalInsertRows, block_rows);
    ProfileEvents::increment(ProfileEvents::VerticalInsertBytes, block_bytes);
    ProfileEvents::increment(ProfileEvents::VerticalInsertMergingColumns, merging_columns.size());
    ProfileEvents::increment(ProfileEvents::VerticalInsertGatheringColumns, gathering_columns.size());

    ProfileEventTimeIncrement<Time::Milliseconds> total_watch(ProfileEvents::VerticalInsertTotalMilliseconds);
    bool completed = false;
    SCOPE_EXIT({
        if (!completed)
            cancel();
    });

    /// Phase 1: Write merging columns (PK + special)
    {
        ProfileEventTimeIncrement<Time::Milliseconds> phase_watch(ProfileEvents::VerticalInsertHorizontalPhaseMilliseconds);
        executeHorizontalPhase();
    }

    /// Phase 2: Write gathering columns in batches
    {
        ProfileEventTimeIncrement<Time::Milliseconds> phase_watch(ProfileEvents::VerticalInsertVerticalPhaseMilliseconds);
        executeVerticalPhase();
    }

    completed = true;
}

void VerticalInsertTask::executeHorizontalPhase()
{
    if (!index_granularity)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index granularity is not set for vertical insert");

    /// Extract only merging columns from block
    Block merging_block;
    for (const auto & column : merging_columns)
    {
        if (auto * col = block.findByName(column.name))
            merging_block.insert({col->column, col->type, col->name});
    }

    /// Create the horizontal output stream for merging columns
    horizontal_output = std::make_unique<MergedBlockOutputStream>(
        new_data_part,
        data_settings,
        metadata_snapshot,
        merging_columns,
        multi_column_skip_indexes,
        merging_statistics,
        codec,
        index_granularity,
        context->getCurrentTransaction() ? context->getCurrentTransaction()->tid : Tx::PrehistoricTID,
        block_bytes,
        /*reset_columns=*/ false,
        /*blocks_are_granules_size=*/ false,
        context->getWriteSettings(),
        &written_offset_columns);

    if (!horizontal_output->getIndexGranularity())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index granularity is not initialized for horizontal output in vertical insert");

    /// Write merging columns with permutation
    if (has_permutation)
        horizontal_output->writeWithPermutation(merging_block, permutation);
    else
        horizontal_output->write(merging_block);

    /// Release merging columns from original block to free memory
    for (const auto & column : merging_columns)
    {
        if (auto * col = block.findByName(column.name))
            col->column.reset();
    }
}

void VerticalInsertTask::executeVerticalPhase()
{
    const size_t batch_size = std::max<size_t>(1, settings.columns_batch_size);
    size_t batch_count = 0;
    const bool need_sync = (*data_settings)[MergeTreeSetting::fsync_after_insert];

    if (!horizontal_output)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Horizontal output stream is not initialized for vertical insert");
    if (!horizontal_output->getIndexGranularity())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index granularity is not initialized for vertical insert column writer");

    column_block.clear();
    column_list.clear();

    MergeTreeIndices batch_skip_indexes;
    ColumnsStatistics batch_statistics;
    chassert(delayed_streams.empty());
    active_column_writer = nullptr;

    /// Lower default than regular insert (100) to reduce peak memory for wide tables
    static constexpr size_t DEFAULT_VERTICAL_INSERT_DELAYED_STREAMS = 16;

    size_t max_delayed_streams = 0;
    const auto & query_settings = context->getSettingsRef();
    if (query_settings[Setting::max_insert_delayed_streams_for_parallel_write].changed)
        max_delayed_streams = query_settings[Setting::max_insert_delayed_streams_for_parallel_write];
    else if (new_data_part->getDataPartStorage().supportParallelWrite())
        max_delayed_streams = DEFAULT_VERTICAL_INSERT_DELAYED_STREAMS;

    auto flush_batch = [&]()
    {
        if (column_list.empty())
            return;

        if (column_block.rows() != block_rows)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Vertical insert batch validation: block has {} rows, batch has {} rows",
                block_rows,
                column_block.rows());
        }

        auto batch_out = std::make_unique<MergedColumnOnlyOutputStream>(
            new_data_part,
            data_settings,
            metadata_snapshot,
            column_list,
            batch_skip_indexes,
            batch_statistics,
            codec,
            horizontal_output->getIndexGranularity(),  /// Reuse granularity from horizontal phase
            block_bytes,
            &written_offset_columns);

        ProfileEvents::increment(ProfileEvents::VerticalInsertWritersCreated);
        ProfileEvents::increment(ProfileEvents::VerticalInsertWriterFlushes);

        active_column_writer = batch_out.get();
        if (has_permutation)
            batch_out->writeWithPermutation(column_block, permutation);
        else
            batch_out->write(column_block);
        active_column_writer = nullptr;

        DelayedColumnStream delayed;
        delayed.stream = std::move(batch_out);
        auto changed_checksums = delayed.stream->fillChecksums(new_data_part, vertical_checksums);
        delayed.checksums.add(std::move(changed_checksums));
        delayed.substreams = delayed.stream->getColumnsSubstreams();

        if (max_delayed_streams > 0)
        {
            delayed_streams.emplace_back(std::move(delayed));
            while (delayed_streams.size() > max_delayed_streams)
            {
                auto & front = delayed_streams.front();
                finalizeDelayedStream(front, need_sync);
                delayed_streams.pop_front();
            }
        }
        else
        {
            finalizeDelayedStream(delayed, need_sync);
        }

        column_block.clear();
        column_list.clear();
        batch_skip_indexes.clear();
        batch_statistics.clear();
        batch_count = 0;
    };

    for (const auto & column : gathering_columns)
    {
        auto * col_data = block.findByName(column.name);
        if (!col_data || !col_data->column)
            continue;

        validateColumnRows(*col_data, block_rows, "column validation");

        /// Release original column immediately after capturing to reduce peak memory
        DataTypePtr col_type = col_data->type;
        String col_name = col_data->name;
        ColumnPtr column_data = col_data->column;
        col_data->column.reset();

        column_block.insert({column_data, col_type, std::move(col_name)});
        column_list.push_back(column);

        auto it_idx = skip_indexes_by_column.find(column.name);
        if (it_idx != skip_indexes_by_column.end())
        {
            for (auto & idx : it_idx->second)
                batch_skip_indexes.push_back(std::move(idx));
            skip_indexes_by_column.erase(it_idx);
        }

        auto it_stat = statistics_by_column.find(column.name);
        if (it_stat != statistics_by_column.end())
        {
            for (auto & stat : it_stat->second)
                batch_statistics.push_back(std::move(stat));
            statistics_by_column.erase(it_stat);
        }

        ++batch_count;
        if (batch_count >= batch_size)
            flush_batch();
    }

    flush_batch();

    /// Release memory from structures no longer needed
    block.clear();
    skip_indexes_by_column.clear();
    statistics_by_column.clear();

    for (auto & stream : delayed_streams)
        finalizeDelayedStream(stream, need_sync);
    delayed_streams.clear();
}

void VerticalInsertTask::finalizeDelayedStream(DelayedColumnStream & delayed, bool need_sync)
{
    delayed.stream->finish(need_sync);
    vertical_checksums.add(std::move(delayed.checksums));
    if (!delayed.substreams.empty())
    {
        vertical_columns_substreams = ColumnsSubstreams::merge(
            vertical_columns_substreams,
            delayed.substreams,
            all_column_names);
    }
}

bool shouldUseVerticalInsert(
    const Block & block,
    const NamesAndTypesList & storage_columns,
    const MergeTreeIndices & skip_indexes,
    const MergeTreeSettingsPtr & settings,
    const StorageMetadataPtr & metadata_snapshot,
    MergeTreeData::DataPart::Type part_type)
{
    /// Check master switch
    if (!(*settings)[MergeTreeSetting::enable_vertical_insert_algorithm])
    {
        ProfileEvents::increment(ProfileEvents::VerticalInsertSkipped);
        return false;
    }

    /// Only wide parts support vertical insert
    if (part_type != MergeTreeData::DataPart::Type::Wide)
    {
        ProfileEvents::increment(ProfileEvents::VerticalInsertSkipped);
        return false;
    }

    /// Respect wide-part thresholds to avoid surprising behavior
    const size_t rows = block.rows();
    const size_t bytes = block.bytes();
    const size_t min_rows_for_wide_part = (*settings)[MergeTreeSetting::min_rows_for_wide_part];
    const size_t min_bytes_for_wide_part = (*settings)[MergeTreeSetting::min_bytes_for_wide_part];
    if (rows < min_rows_for_wide_part && bytes < min_bytes_for_wide_part)
    {
        ProfileEvents::increment(ProfileEvents::VerticalInsertSkipped);
        return false;
    }

    /// Check row threshold
    const size_t min_rows = (*settings)[MergeTreeSetting::vertical_insert_algorithm_min_rows_to_activate];
    if (rows < min_rows)
    {
        ProfileEvents::increment(ProfileEvents::VerticalInsertSkipped);
        return false;
    }

    /// Check byte threshold (if set)
    const size_t min_bytes = (*settings)[MergeTreeSetting::vertical_insert_algorithm_min_bytes_to_activate];
    if (min_bytes > 0 && bytes < min_bytes)
    {
        ProfileEvents::increment(ProfileEvents::VerticalInsertSkipped);
        return false;
    }

    /// Count gathering columns
    size_t gathering_count = countGatheringColumns(storage_columns, metadata_snapshot, skip_indexes);

    const size_t min_columns = (*settings)[MergeTreeSetting::vertical_insert_algorithm_min_columns_to_activate];
    if (gathering_count < min_columns)
    {
        ProfileEvents::increment(ProfileEvents::VerticalInsertSkipped);
        return false;
    }

    return true;
}

size_t countGatheringColumns(
    const NamesAndTypesList & storage_columns,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeIndices & skip_indexes)
{
    /// Get sorting key columns
    const auto & sorting_key_expr = metadata_snapshot->getSortingKey().expression;
    Names sort_key_columns_vec = sorting_key_expr->getRequiredColumns();

    NameSet key_columns;
    auto storage_columns_name_set = storage_columns.getNameSet();

    for (const auto & name : sort_key_columns_vec)
    {
        addColumnNameOrBase(name, storage_columns_name_set, key_columns);
        addNestedKeyColumns(name, storage_columns, key_columns);
    }

    /// Include partition key columns to keep minmax index inputs in horizontal phase
    const auto minmax_columns = MergeTreeData::getMinMaxColumnsNames(metadata_snapshot->getPartitionKey());
    for (const auto & name : minmax_columns)
        addColumnNameOrBase(name, storage_columns_name_set, key_columns);

    /// Include TTL expression columns to keep TTL inputs in horizontal phase
    addColumnsFromTTLDescription(metadata_snapshot->getRowsTTL(), storage_columns_name_set, key_columns);
    addColumnsFromTTLDescriptions(metadata_snapshot->getRowsWhereTTLs(), storage_columns_name_set, key_columns);
    addColumnsFromTTLDescriptions(metadata_snapshot->getMoveTTLs(), storage_columns_name_set, key_columns);
    addColumnsFromTTLDescriptions(metadata_snapshot->getRecompressionTTLs(), storage_columns_name_set, key_columns);
    addColumnsFromTTLDescriptions(metadata_snapshot->getGroupByTTLs(), storage_columns_name_set, key_columns);
    addColumnsFromTTLColumnsDescription(metadata_snapshot->getColumnTTLs(), storage_columns_name_set, key_columns);

    /// Add columns from multi-column skip indexes
    for (const auto & index : skip_indexes)
    {
        const auto & index_desc = index->index;
        auto index_columns = index_desc.expression->getRequiredColumns();
        if (index_columns.size() > 1)
        {
            for (const auto & col : index_columns)
            {
                if (storage_columns_name_set.contains(col))
                    key_columns.insert(col);
                else
                    key_columns.insert(String(Nested::getColumnFromSubcolumn(col, storage_columns_name_set)));
            }
        }
    }

    /// Count non-key columns
    size_t count = 0;
    for (const auto & column : storage_columns)
    {
        if (!key_columns.contains(column.name))
            ++count;
    }

    return count;
}

} // namespace DB
