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

#include <algorithm>
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

void addSubcolumnsFromExpression(const ExpressionActionsPtr & expr, Block & block)
{
    for (const auto & required_column : expr->getRequiredColumns())
    {
        if (!block.has(required_column))
            block.insert(block.getSubcolumnByName(required_column));
    }
}

struct IndexColumnsPlan
{
    Names columns;
    ExpressionActionsPtr expression;
};

IndexColumnsPlan buildIndexColumnsPlan(
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeIndices & multi_column_skip_indexes,
    MergeTreeData & data,
    const Block & block)
{
    /// Build index columns list and compute key expressions only if needed.
    /// Mirrors non-vertical insert, which materializes key/index expressions into the block before writing.
    IndexColumnsPlan plan;
    plan.columns = metadata_snapshot->getPrimaryKeyColumns();
    if (plan.columns.empty())
        plan.columns = metadata_snapshot->getSortingKeyColumns();

    NameSet index_column_set(plan.columns.begin(), plan.columns.end());
    for (const auto & index : multi_column_skip_indexes)
    {
        for (const auto & name : index->index.column_names)
        {
            if (index_column_set.emplace(name).second)
                plan.columns.push_back(name);
        }
    }

    bool need_index_expr = false;
    for (const auto & name : plan.columns)
    {
        if (!block.has(name))
        {
            need_index_expr = true;
            break;
        }
    }

    if (need_index_expr)
    {
        if (metadata_snapshot->hasPrimaryKey())
            plan.expression = data.getPrimaryKeyAndSkipIndicesExpression(metadata_snapshot, multi_column_skip_indexes);
        else
            plan.expression = data.getSortingKeyAndSkipIndicesExpression(metadata_snapshot, multi_column_skip_indexes);
    }

    return plan;
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
    /// Decide which physical columns stay in horizontal phase vs. vertical phase.
    /// Key/index expression columns are materialized later, right before writing.
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
    vertical_substreams_by_column.clear();
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

    /// Phase 1: write key/special columns and build indices.
    {
        ProfileEventTimeIncrement<Time::Milliseconds> phase_watch(ProfileEvents::VerticalInsertHorizontalPhaseMilliseconds);
        executeHorizontalPhase();
    }

    /// Phase 2: write remaining columns in batches, releasing memory early.
    {
        ProfileEventTimeIncrement<Time::Milliseconds> phase_watch(ProfileEvents::VerticalInsertVerticalPhaseMilliseconds);
        executeVerticalPhase();
    }

    completed = true;
}

void VerticalInsertTask::executeHorizontalPhase()
{
    /// Prepare a minimal block for key/skip-index writes and release those columns.
    if (!index_granularity)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index granularity is not set for vertical insert");

    /// Ensure primary key and skip index expression columns are present for index calculation.
    auto index_plan = buildIndexColumnsPlan(metadata_snapshot, multi_column_skip_indexes, data, block);
    if (index_plan.expression)
    {
        addSubcolumnsFromExpression(index_plan.expression, block);
        index_plan.expression->execute(block);
    }

    /// Extract only merging columns from block
    Block merging_block;
    for (const auto & column : merging_columns)
    {
        if (auto * col = block.findByName(column.name))
            merging_block.insert({col->column, col->type, col->name});
    }

    /// Add primary key/skip index expression columns if they were computed in the block
    for (const auto & name : index_plan.columns)
    {
        if (merging_block.has(name))
            continue;
        if (auto * col = block.findByName(name))
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

    /// Release primary key/skip index expression columns from original block to free memory
    for (const auto & name : index_plan.columns)
    {
        if (auto * col = block.findByName(name))
            col->column.reset();
    }
}

struct VerticalInsertTask::VerticalPhaseState
{
    VerticalInsertTask & task;
    const size_t batch_size;
    const size_t batch_bytes_budget;
    size_t batch_count = 0;
    size_t batch_bytes = 0;
    const bool need_sync;
    MergeTreeIndices batch_skip_indexes;
    ColumnsStatistics batch_statistics;
    size_t max_delayed_streams = 0;
    size_t delayed_open_streams = 0;

    explicit VerticalPhaseState(
        VerticalInsertTask & task_,
        size_t batch_size_,
        size_t batch_bytes_budget_,
        bool need_sync_)
        : task(task_)
        , batch_size(batch_size_)
        , batch_bytes_budget(batch_bytes_budget_)
        , need_sync(need_sync_)
    {
    }

    void init()
    {
        /// Reset reusable batch state and read delayed-stream policy once.
        task.column_block.clear();
        task.column_list.clear();
        task.vertical_substreams_by_column.clear();
        task.active_column_writer = nullptr;
        batch_skip_indexes.clear();
        batch_statistics.clear();
        task.delayed_streams.clear();

        const auto & query_settings = task.context->getSettingsRef();
        if (query_settings[Setting::max_insert_delayed_streams_for_parallel_write].changed)
            max_delayed_streams = query_settings[Setting::max_insert_delayed_streams_for_parallel_write];
    }

    void resetBatch()
    {
        /// Reuse buffers between batches.
        task.column_block.clear();
        task.column_list.clear();
        batch_skip_indexes.clear();
        batch_statistics.clear();
        batch_count = 0;
        batch_bytes = 0;
    }

    void enqueueDelayed(DelayedColumnStream && delayed)
    {
        /// Either finalize immediately or cap concurrent open streams.
        if (max_delayed_streams == 0)
        {
            task.finalizeDelayedStream(delayed, need_sync);
            return;
        }

        task.delayed_streams.emplace_back(std::move(delayed));
        delayed_open_streams += task.delayed_streams.back().open_streams;
        while (delayed_open_streams > max_delayed_streams)
        {
            auto & front = task.delayed_streams.front();
            delayed_open_streams -= front.open_streams;
            task.finalizeDelayedStream(front, need_sync);
            task.delayed_streams.pop_front();
        }
    }

    void flushBatch()
    {
        /// Write current batch and move stream to delayed/finalized state.
        if (task.column_list.empty())
            return;

        if (task.column_block.rows() != task.block_rows)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Vertical insert batch validation: block has {} rows, batch has {} rows",
                task.block_rows,
                task.column_block.rows());
        }

        auto batch_out = std::make_unique<MergedColumnOnlyOutputStream>(
            task.new_data_part,
            task.data_settings,
            task.metadata_snapshot,
            task.column_list,
            batch_skip_indexes,
            batch_statistics,
            task.codec,
            task.horizontal_output->getIndexGranularity(),
            task.block_bytes,
            &task.written_offset_columns);

        ProfileEvents::increment(ProfileEvents::VerticalInsertWritersCreated);
        ProfileEvents::increment(ProfileEvents::VerticalInsertWriterFlushes);

        task.active_column_writer = batch_out.get();
        if (task.has_permutation)
            batch_out->writeWithPermutation(task.column_block, task.permutation);
        else
            batch_out->write(task.column_block);
        task.active_column_writer = nullptr;

        DelayedColumnStream delayed;
        delayed.stream = std::move(batch_out);
        auto changed_checksums = delayed.stream->fillChecksums(task.new_data_part, task.vertical_checksums);
        delayed.checksums.add(std::move(changed_checksums));
        delayed.substreams = delayed.stream->getColumnsSubstreams();
        delayed.open_streams = delayed.stream->getNumberOfOpenStreams();

        enqueueDelayed(std::move(delayed));
        resetBatch();
    }

    void addColumn(const NameAndTypePair & column)
    {
        /// Move column into batch and flush on size/bytes budget.
        auto * col_data = task.block.findByName(column.name);
        if (!col_data || !col_data->column)
            return;

        validateColumnRows(*col_data, task.block_rows, "column validation");

        DataTypePtr col_type = col_data->type;
        String col_name = col_data->name;
        ColumnPtr column_data = col_data->column;
        col_data->column.reset();

        const size_t column_bytes = column_data ? column_data->byteSize() : 0;
        if (batch_bytes_budget > 0 && batch_bytes > 0 && batch_bytes + column_bytes > batch_bytes_budget)
            flushBatch();

        task.column_block.insert({column_data, col_type, col_name});
        task.column_list.push_back(column);
        batch_bytes += column_bytes;

        auto it_idx = task.skip_indexes_by_column.find(column.name);
        if (it_idx != task.skip_indexes_by_column.end())
        {
            for (auto & idx : it_idx->second)
                batch_skip_indexes.push_back(std::move(idx));
            task.skip_indexes_by_column.erase(it_idx);
        }

        auto it_stat = task.statistics_by_column.find(column.name);
        if (it_stat != task.statistics_by_column.end())
        {
            for (auto & stat : it_stat->second)
                batch_statistics.push_back(std::move(stat));
            task.statistics_by_column.erase(it_stat);
        }

        ++batch_count;
        if (batch_count >= batch_size || (batch_bytes_budget > 0 && batch_bytes >= batch_bytes_budget))
            flushBatch();
    }

    void finish()
    {
        /// Flush remaining batch and finalize delayed streams/substreams.
        flushBatch();

        task.block.clear();
        task.skip_indexes_by_column.clear();
        task.statistics_by_column.clear();

        for (auto & stream : task.delayed_streams)
            task.finalizeDelayedStream(stream, need_sync);
        task.delayed_streams.clear();

        if (!task.vertical_substreams_by_column.empty())
        {
            ColumnsSubstreams merged;
            for (const auto & column_name : task.all_column_names)
            {
                auto it = task.vertical_substreams_by_column.find(column_name);
                if (it == task.vertical_substreams_by_column.end())
                    continue;
                merged.addColumn(column_name);
                merged.addSubstreamsToLastColumn(it->second);
            }
            task.vertical_columns_substreams = std::move(merged);
        }
        task.vertical_substreams_by_column.clear();
    }
};

void VerticalInsertTask::executeVerticalPhase()
{
    /// Stream non-key columns in batches with optional delayed finalization.
    const size_t batch_size = std::max<size_t>(1, settings.columns_batch_size);
    const size_t batch_bytes_budget = settings.columns_batch_bytes;
    const bool need_sync = (*data_settings)[MergeTreeSetting::fsync_after_insert];

    if (!horizontal_output)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Horizontal output stream is not initialized for vertical insert");
    if (!horizontal_output->getIndexGranularity())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index granularity is not initialized for vertical insert column writer");

    chassert(delayed_streams.empty());

    VerticalPhaseState state(*this, batch_size, batch_bytes_budget, need_sync);
    state.init();

    for (const auto & column : gathering_columns)
        state.addColumn(column);

    state.finish();
}

void VerticalInsertTask::finalizeDelayedStream(DelayedColumnStream & delayed, bool need_sync)
{
    delayed.stream->finish(need_sync);
    vertical_checksums.add(std::move(delayed.checksums));
    if (!delayed.substreams.empty())
    {
        for (const auto & [column_name, substreams] : delayed.substreams.getColumnsSubstreams())
        {
            auto [it, inserted] = vertical_substreams_by_column.emplace(column_name, substreams);
            if (!inserted)
            {
                auto & existing = it->second;
                for (const auto & substream : substreams)
                {
                    if (std::find(existing.begin(), existing.end(), substream) == existing.end())
                        existing.emplace_back(substream);
                }
            }
        }
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
