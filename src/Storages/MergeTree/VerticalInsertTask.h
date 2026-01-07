#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/Statistics/Statistics.h>
#include <Common/Logger.h>

#include <deque>
#include <unordered_map>

namespace DB
{

/// Performs vertical (column-by-column) insert to reduce memory usage
/// on wide tables. Mirrors vertical merge architecture.
///
/// Two-phase write:
/// 1. Horizontal phase: Write PK/sorting key columns with permutation, then release them from block
/// 2. Vertical phase: Write remaining columns in batches, releasing each batch after writing
///
/// This reduces peak additional memory during write from O(rows * all_columns) to O(rows * batch_size)
/// by progressively releasing columns from the original block after they are written to disk.
class VerticalInsertTask
{
public:
    struct Settings
    {
        size_t columns_batch_size;
        size_t columns_batch_bytes;
    };

    VerticalInsertTask(
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
        ContextPtr context_);

    /// Execute the two-phase write
    void execute();

    /// Cancel in-flight streams and clean up on failure
    void cancel() noexcept;

    /// Get the output stream for finalization (transfers ownership)
    std::unique_ptr<MergedBlockOutputStream> releaseOutputStream() { return std::move(horizontal_output); }

    /// Get checksums accumulated from vertical phase
    MergeTreeData::DataPart::Checksums & getVerticalChecksums() { return vertical_checksums; }

    /// Get column substreams accumulated from vertical phase
    ColumnsSubstreams & getVerticalColumnsSubstreams() { return vertical_columns_substreams; }

private:
    struct DelayedColumnStream
    {
        std::unique_ptr<MergedColumnOnlyOutputStream> stream;
        MergeTreeData::DataPart::Checksums checksums;
        ColumnsSubstreams substreams;
        size_t open_streams = 0;
    };

    /// Classify columns into merging (PK) and gathering (rest)
    void classifyColumns();

    /// Phase 1: Write PK/sorting key columns
    void executeHorizontalPhase();

    /// Phase 2: Write remaining columns in batches
    void executeVerticalPhase();
    void finalizeDelayedStream(DelayedColumnStream & delayed, bool need_sync);

    MergeTreeData & data;
    MergeTreeSettingsPtr data_settings;
    StorageMetadataPtr metadata_snapshot;
    MergeTreeMutableDataPartPtr new_data_part;
    Block block;
    size_t block_rows;
    size_t block_bytes;
    const IColumn::Permutation * permutation;
    bool has_permutation;
    bool permutation_is_identity;
    MergeTreeIndices skip_indexes;
    ColumnsStatistics statistics;
    CompressionCodecPtr codec;
    MergeTreeIndexGranularityPtr index_granularity;
    Settings settings;
    ContextPtr context;

    /// Column classification
    NamesAndTypesList merging_columns;
    NamesAndTypesList gathering_columns;

    /// Skip indexes that can be built per-column (single-column indexes)
    std::unordered_map<String, MergeTreeIndices> skip_indexes_by_column;
    /// Skip indexes that must be built in horizontal phase (multi-column indexes)
    MergeTreeIndices multi_column_skip_indexes;

    /// Statistics that can be built per-column
    std::unordered_map<String, ColumnsStatistics> statistics_by_column;
    /// Statistics that should be built in horizontal phase
    ColumnsStatistics merging_statistics;

    /// State for horizontal phase output
    std::unique_ptr<MergedBlockOutputStream> horizontal_output;
    MergedColumnOnlyOutputStream * active_column_writer = nullptr;

    /// Checksums accumulated from vertical phase
    MergeTreeData::DataPart::Checksums vertical_checksums;
    ColumnsSubstreams vertical_columns_substreams;
    std::unordered_map<String, std::vector<String>> vertical_substreams_by_column;

    /// Track written offset columns for Nested types
    IMergedBlockOutputStream::WrittenOffsetColumns written_offset_columns;

    /// Reused single-column containers to reduce allocations
    Block column_block;
    NamesAndTypesList column_list;
    Names all_column_names;

    std::deque<DelayedColumnStream> delayed_streams;

    LoggerPtr log;
};

/// Check if vertical insert should be used for the given block
bool shouldUseVerticalInsert(
    const Block & block,
    const NamesAndTypesList & storage_columns,
    const MergeTreeIndices & skip_indexes,
    const MergeTreeSettingsPtr & settings,
    const StorageMetadataPtr & metadata_snapshot,
    MergeTreeData::DataPart::Type part_type);

/// Count the number of gathering columns (non-PK columns)
size_t countGatheringColumns(
    const NamesAndTypesList & storage_columns,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeIndices & skip_indexes);

} // namespace DB
