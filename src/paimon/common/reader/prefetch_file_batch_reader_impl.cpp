/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include "paimon/common/reader/prefetch_file_batch_reader_impl.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <limits>
#include <thread>

#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/io/cache_input_stream.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/read_ahead_cache.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/format/reader_builder.h"
#include "paimon/fs/file_system.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

struct PrefetchMetricsState {
    std::atomic<uint64_t> read_ranges_total{0};
    std::atomic<uint64_t> read_ranges_after_bitmap{0};
    std::atomic<uint64_t> seek_count{0};
    std::atomic<uint64_t> produced_batches{0};
    std::atomic<uint64_t> consumed_batches{0};
    std::atomic<uint64_t> discarded_batches{0};
    std::atomic<uint64_t> errors{0};
    std::atomic<uint64_t> adaptive_disabled_count{0};
    std::atomic<uint64_t> queue_full_count{0};
    std::atomic<uint64_t> queue_depth{0};
    std::atomic<uint64_t> queue_depth_max{0};
    std::atomic<bool> enabled{false};
    std::shared_ptr<MetricsImpl> histograms = std::make_shared<MetricsImpl>();
};

struct PrefetchIoMetricsState {
    std::atomic<uint64_t> read_requests{0};
    std::atomic<uint64_t> read_requested_bytes{0};
    std::atomic<uint64_t> read_physical_bytes{0};
    std::atomic<uint64_t> read_failed{0};
    std::atomic<uint64_t> read_latency_count{0};
    std::atomic<uint64_t> read_latency_sum_us{0};
    std::atomic<uint64_t> async_requests{0};
    std::atomic<uint64_t> async_requested_bytes{0};
    std::atomic<uint64_t> async_physical_bytes{0};
    std::atomic<uint64_t> async_completed{0};
    std::atomic<uint64_t> async_failed{0};
    std::atomic<uint64_t> async_pending{0};
    std::atomic<uint64_t> async_latency_count{0};
    std::atomic<uint64_t> async_latency_sum_us{0};
};

namespace {

// Metrics do not synchronize reader state. A concurrently collected snapshot may be approximate.
constexpr std::memory_order kMetricsMemoryOrder = std::memory_order_relaxed;

uint64_t ElapsedMicros(const std::chrono::steady_clock::time_point& start) {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                     std::chrono::steady_clock::now() - start)
                                     .count());
}

void UpdateMax(std::atomic<uint64_t>* target, uint64_t value) {
    uint64_t current = target->load(kMetricsMemoryOrder);
    while (current < value && !target->compare_exchange_weak(current, value, kMetricsMemoryOrder,
                                                             kMetricsMemoryOrder)) {
    }
}

void RecordLatency(uint64_t latency_us, std::atomic<uint64_t>* count,
                   std::atomic<uint64_t>* sum_us) {
    count->fetch_add(1, kMetricsMemoryOrder);
    sum_us->fetch_add(latency_us, kMetricsMemoryOrder);
}

class MetricsInputStream : public InputStream {
 public:
    MetricsInputStream(const std::shared_ptr<InputStream>& stream,
                       const std::shared_ptr<PrefetchIoMetricsState>& metrics)
        : stream_(stream), metrics_(metrics) {}

    MetricsInputStream(std::unique_ptr<InputStream>&& stream,
                       const std::shared_ptr<PrefetchIoMetricsState>& metrics)
        : stream_(std::move(stream)), metrics_(metrics) {}

    Status Seek(int64_t offset, SeekOrigin origin) override {
        return stream_->Seek(offset, origin);
    }

    Result<int64_t> GetPos() const override {
        return stream_->GetPos();
    }

    Result<int64_t> Read(char* buffer, int64_t size) override {
        return RecordRead([&]() { return stream_->Read(buffer, size); }, size);
    }

    Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        return RecordRead([&]() { return stream_->Read(buffer, size, offset); }, size);
    }

    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(Status)>&& callback) override {
        metrics_->async_requests.fetch_add(1, kMetricsMemoryOrder);
        metrics_->async_requested_bytes.fetch_add(static_cast<uint64_t>(std::max<int64_t>(0, size)),
                                                  kMetricsMemoryOrder);
        metrics_->async_pending.fetch_add(1, kMetricsMemoryOrder);
        std::shared_ptr<PrefetchIoMetricsState> metrics = metrics_;
        const auto start = std::chrono::steady_clock::now();
        stream_->ReadAsync(
            buffer, size, offset,
            [metrics, size, start, callback = std::move(callback)](Status status) mutable {
                metrics->async_pending.fetch_sub(1, kMetricsMemoryOrder);
                if (status.ok()) {
                    metrics->async_completed.fetch_add(1, kMetricsMemoryOrder);
                    metrics->async_physical_bytes.fetch_add(
                        static_cast<uint64_t>(std::max<int64_t>(0, size)), kMetricsMemoryOrder);
                } else {
                    metrics->async_failed.fetch_add(1, kMetricsMemoryOrder);
                }
                RecordLatency(ElapsedMicros(start), &metrics->async_latency_count,
                              &metrics->async_latency_sum_us);
                callback(status);
            });
    }

    Status Close() override {
        return stream_->Close();
    }

    Result<std::string> GetUri() const override {
        return stream_->GetUri();
    }

    Result<int64_t> Length() const override {
        return stream_->Length();
    }

 private:
    template <typename ReadFunction>
    Result<int64_t> RecordRead(ReadFunction&& read, int64_t size) {
        metrics_->read_requests.fetch_add(1, kMetricsMemoryOrder);
        metrics_->read_requested_bytes.fetch_add(static_cast<uint64_t>(std::max<int64_t>(0, size)),
                                                 kMetricsMemoryOrder);
        const auto start = std::chrono::steady_clock::now();
        Result<int64_t> result = read();
        if (result.ok()) {
            metrics_->read_physical_bytes.fetch_add(
                static_cast<uint64_t>(std::max<int64_t>(0, result.value())), kMetricsMemoryOrder);
        } else {
            metrics_->read_failed.fetch_add(1, kMetricsMemoryOrder);
        }
        RecordLatency(ElapsedMicros(start), &metrics_->read_latency_count,
                      &metrics_->read_latency_sum_us);
        return result;
    }

    std::shared_ptr<InputStream> stream_;
    std::shared_ptr<PrefetchIoMetricsState> metrics_;
};

std::pair<int64_t, int64_t> ComputeBatchSliceByReadRange(
    const std::vector<uint64_t>& global_row_ids, const std::pair<uint64_t, uint64_t>& read_range) {
    auto begin_it =
        std::lower_bound(global_row_ids.begin(), global_row_ids.end(), read_range.first);
    auto end_it = std::lower_bound(global_row_ids.begin(), global_row_ids.end(), read_range.second);
    return {static_cast<int64_t>(std::distance(global_row_ids.begin(), begin_it)),
            static_cast<int64_t>(std::distance(global_row_ids.begin(), end_it))};
}

}  // namespace

Result<std::unique_ptr<PrefetchFileBatchReaderImpl>> PrefetchFileBatchReaderImpl::Create(
    const std::string& data_file_path, int64_t data_file_size, const ReaderBuilder* reader_builder,
    const std::shared_ptr<FileSystem>& fs, uint32_t prefetch_max_parallel_num, int32_t batch_size,
    uint32_t prefetch_batch_count, bool enable_adaptive_prefetch_strategy,
    const std::shared_ptr<Executor>& executor, bool initialize_read_ranges,
    bool read_ahead_cache_enabled, const CacheConfig& cache_config, bool enable_io_metrics,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (prefetch_max_parallel_num == 0) {
        return Status::Invalid("prefetch max parallel num should be greater than 0.");
    }
    if (prefetch_batch_count == 0) {
        return Status::Invalid("prefetch batch count should be greater than 0.");
    }
    if (batch_size <= 0) {
        return Status::Invalid("batch size should be greater than 0.");
    }
    if (reader_builder == nullptr) {
        return Status::Invalid("reader_builder should not be nullptr.");
    }
    if (fs == nullptr) {
        return Status::Invalid("file system should not be nullptr.");
    }
    if (executor == nullptr) {
        return Status::Invalid("executor should not be nullptr.");
    }

    std::shared_ptr<PrefetchIoMetricsState> io_metrics;
    if (enable_io_metrics) {
        io_metrics = std::make_shared<PrefetchIoMetricsState>();
    }
    std::shared_ptr<ReadAheadCache> cache;
    if (read_ahead_cache_enabled) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream,
                               fs->Open(FileStatus(data_file_path, data_file_size)));
        if (io_metrics) {
            input_stream = std::make_shared<MetricsInputStream>(input_stream, io_metrics);
        }
        cache = std::make_shared<ReadAheadCache>(input_stream, cache_config, pool);
    }
    std::vector<std::future<Result<std::unique_ptr<FileBatchReader>>>> futures;
    for (uint32_t i = 0; i < prefetch_max_parallel_num; i++) {
        futures.push_back(Via(
            executor.get(),
            [&fs, &data_file_path, data_file_size, &reader_builder, &cache,
             io_metrics]() -> Result<std::unique_ptr<FileBatchReader>> {
                PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> input_stream,
                                       fs->Open(FileStatus(data_file_path, data_file_size)));
                if (io_metrics) {
                    input_stream =
                        std::make_unique<MetricsInputStream>(std::move(input_stream), io_metrics);
                }
                auto cache_input_stream =
                    std::make_shared<CacheInputStream>(std::move(input_stream), cache);
                return reader_builder->Build(cache_input_stream);
            }));
    }
    std::vector<std::shared_ptr<PrefetchFileBatchReader>> readers;
    for (auto& file_batch_reader : CollectAll(futures)) {
        if (!file_batch_reader.ok()) {
            return file_batch_reader.status();
        }
        std::shared_ptr<FileBatchReader> reader = std::move(file_batch_reader).value();
        auto prefetch_file_batch_reader =
            std::dynamic_pointer_cast<PrefetchFileBatchReader>(reader);
        if (prefetch_file_batch_reader == nullptr) {
            return Status::Invalid(
                "failed to cast to prefetch file batch reader. file format not support prefetch");
        }
        readers.emplace_back(prefetch_file_batch_reader);
    }
    if (prefetch_batch_count < readers.size()) {
        prefetch_batch_count = readers.size();
    }
    uint32_t prefetch_queue_capacity = prefetch_batch_count / readers.size();

    auto reader = std::unique_ptr<PrefetchFileBatchReaderImpl>(new PrefetchFileBatchReaderImpl(
        readers, batch_size, prefetch_queue_capacity, enable_adaptive_prefetch_strategy, executor,
        cache, io_metrics, arrow_pool));
    if (initialize_read_ranges) {
        // normally initialize read ranges should be false, as set read schema will refresh read
        // ranges, and set read schema will always be called before read.
        PAIMON_RETURN_NOT_OK(reader->RefreshReadRanges());
    }
    return reader;
}

PrefetchFileBatchReaderImpl::PrefetchFileBatchReaderImpl(
    const std::vector<std::shared_ptr<PrefetchFileBatchReader>>& readers, int32_t batch_size,
    uint32_t prefetch_queue_capacity, bool enable_adaptive_prefetch_strategy,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<ReadAheadCache>& cache,
    const std::shared_ptr<PrefetchIoMetricsState>& io_metrics,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : readers_(std::move(readers)),
      batch_size_(batch_size),
      executor_(executor),
      cache_(cache),
      arrow_pool_(arrow_pool),
      prefetch_queue_capacity_(prefetch_queue_capacity),
      enable_adaptive_prefetch_strategy_(enable_adaptive_prefetch_strategy),
      prefetch_metrics_(std::make_shared<PrefetchMetricsState>()),
      io_metrics_(io_metrics) {
    for (size_t i = 0; i < readers_.size(); i++) {
        prefetch_queues_.emplace_back(std::make_unique<ThreadsafeQueue<PrefetchBatch>>());
        readers_pos_.emplace_back(std::make_unique<std::atomic<uint64_t>>(0));
        reader_is_working_.emplace_back(false);
    }
    parallel_num_ = readers_.size();
}

PrefetchFileBatchReaderImpl::~PrefetchFileBatchReaderImpl() {
    (void)CleanUp();
}

Status PrefetchFileBatchReaderImpl::SetReadSchema(
    ::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    PAIMON_RETURN_NOT_OK(CleanUp());
    if (cache_) {
        cache_->Reset();
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                      arrow::ImportSchema(read_schema));
    for (const auto& reader : readers_) {
        auto c_schema = std::make_unique<::ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, c_schema.get()));
        PAIMON_RETURN_NOT_OK(reader->SetReadSchema(c_schema.get(), predicate, selection_bitmap));
    }
    selection_bitmap_ = selection_bitmap;
    predicate_ = predicate;
    return RefreshReadRangesAfterCleanUp();
}

Status PrefetchFileBatchReaderImpl::RefreshReadRanges() {
    PAIMON_RETURN_NOT_OK(CleanUp());
    if (cache_) {
        cache_->Reset();
    }
    return RefreshReadRangesAfterCleanUp();
}

Status PrefetchFileBatchReaderImpl::RefreshReadRangesAfterCleanUp() {
    bool need_prefetch;
    PAIMON_ASSIGN_OR_RAISE(auto read_ranges, readers_[0]->GenReadRanges(&need_prefetch));
    const bool format_requested_prefetch = need_prefetch;

    if (!enable_adaptive_prefetch_strategy_) {
        need_prefetch = true;
    } else if (need_prefetch && enable_adaptive_prefetch_strategy_ && !read_ranges.empty()) {
        uint64_t batch_count_in_range =
            (read_ranges[0].second - read_ranges[0].first) / batch_size_;
        if (batch_count_in_range > static_cast<uint64_t>(prefetch_queue_capacity_)) {
            need_prefetch = false;
        }
    }

    if (format_requested_prefetch && !need_prefetch) {
        prefetch_metrics_->adaptive_disabled_count.fetch_add(1, kMetricsMemoryOrder);
    }
    need_prefetch_ = need_prefetch;
    prefetch_metrics_->enabled.store(need_prefetch_, kMetricsMemoryOrder);
    prefetch_metrics_->read_ranges_total.fetch_add(read_ranges.size(), kMetricsMemoryOrder);
    std::vector<std::pair<uint64_t, uint64_t>> filtered_ranges =
        FilterReadRanges(read_ranges, selection_bitmap_);
    prefetch_metrics_->read_ranges_after_bitmap.fetch_add(filtered_ranges.size(),
                                                          kMetricsMemoryOrder);
    PAIMON_RETURN_NOT_OK(SetReadRanges(filtered_ranges));
    return Status::OK();
}

std::vector<std::pair<uint64_t, uint64_t>> PrefetchFileBatchReaderImpl::FilterReadRanges(
    const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    if (!selection_bitmap) {
        return read_ranges;
    }
    std::vector<std::pair<uint64_t, uint64_t>> result;
    for (const auto& read_range : read_ranges) {
        if (selection_bitmap.value().ContainsAny(read_range.first, read_range.second)) {
            result.push_back(read_range);
        }
    }
    return result;
}

Status PrefetchFileBatchReaderImpl::SetReadRanges(
    const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) {
    // push down read ranges for reducing IO amplification
    read_ranges_in_group_ = DispatchReadRanges(read_ranges, readers_.size());
    if (need_prefetch_ && readers_.size() > 1) {
        // if prefetching isn't necessary, then setting read ranges won't be needed either.
        std::vector<std::future<Status>> futures;
        for (size_t i = 0; i < readers_.size(); i++) {
            futures.push_back(Via(executor_.get(), [this, i]() -> Status {
                return readers_[i]->SetReadRanges(read_ranges_in_group_[i]);
            }));
        }
        for (const auto& status : CollectAll(futures)) {
            if (!status.ok()) {
                return status;
            }
        }
    }
    for (const auto& read_range : read_ranges) {
        read_ranges_.push_back(read_range);
    }
    // Note: add a special read range out of file row count, for trigger an EOF access.
    std::pair<uint64_t, uint64_t> eof_range;
    PAIMON_ASSIGN_OR_RAISE(eof_range, EofRange());
    read_ranges_.push_back(eof_range);
    for (auto& read_ranges : read_ranges_in_group_) {
        read_ranges.push_back(eof_range);
    }
    read_ranges_freshed_ = true;
    return Status::OK();
}

std::vector<std::vector<std::pair<uint64_t, uint64_t>>>
PrefetchFileBatchReaderImpl::DispatchReadRanges(
    const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges, size_t group_count) {
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> read_ranges_in_group;
    read_ranges_in_group.resize(group_count);
    for (size_t i = 0; i < read_ranges.size(); i++) {
        read_ranges_in_group[i % group_count].push_back(read_ranges[i]);
    }
    return read_ranges_in_group;
}

Status PrefetchFileBatchReaderImpl::CleanUp() {
    auto clean_prefetch_queue = [this]() {
        for (auto& prefetch_queue : prefetch_queues_) {
            while (true) {
                std::optional<PrefetchBatch> batch = prefetch_queue->try_pop();
                {
                    std::unique_lock<std::mutex> lock(working_mutex_);
                    cv_.notify_one();
                }
                if (batch == std::nullopt) {
                    break;
                }
                prefetch_metrics_->discarded_batches.fetch_add(1, kMetricsMemoryOrder);
                ReaderUtils::ReleaseReadBatch(std::move(batch.value().batch.first));
            }
        }
    };
    // Clear the existing read ranges and prefetch queue
    {
        std::unique_lock<std::mutex> lock(working_mutex_);
        is_shutdown_ = true;  // set is shutdown and check shutdown to avoid block at queue.push
        cv_.notify_one();
    }
    // Join and reset the background thread if it exists
    if (background_thread_) {
        if (background_thread_->joinable()) {
            background_thread_->join();
            background_thread_.reset();
        } else {
            return Status::Invalid("background thread is not joinable");
        }
    }

    read_ranges_.clear();
    read_ranges_in_group_.clear();
    current_batch_global_row_ids_.clear();
    read_ranges_freshed_ = false;
    clean_prefetch_queue();
    prefetch_metrics_->queue_depth.store(0, kMetricsMemoryOrder);
    for (size_t i = 0; i < readers_pos_.size(); i++) {
        readers_pos_[i]->store(0);
        reader_is_working_[i] = false;
    }
    is_shutdown_ = false;
    SetReadStatus(Status::OK());
    return Status::OK();
}

void PrefetchFileBatchReaderImpl::Workloop() {
    std::vector<std::future<void>> futures;
    futures.resize(readers_.size());
    if (cache_) {
        auto read_ranges = readers_[0]->PreBufferRange();
        if (read_ranges.ok()) {
            std::vector<ByteRange> ranges;
            for (const auto& read_range : read_ranges.value()) {
                ranges.emplace_back(read_range.first, read_range.second);
            }
            auto s = cache_->Init(std::move(ranges));
            if (!s.ok()) {
                SetReadStatus(s);
            } else {
                // Init() only registers the ranges, so without this the first
                // cache fetch races the readers' first reads instead of running
                // ahead of them.
                cache_->Warmup();
            }
        } else {
            SetReadStatus(read_ranges.status());
        }
    }

    while (true) {
        if (!GetReadStatus().ok()) {
            break;
        }
        if (is_shutdown_) {
            break;
        }
        bool all_finished = true;
        for (const auto& reader_pos : readers_pos_) {
            if (reader_pos->load() != std::numeric_limits<uint64_t>::max()) {
                all_finished = false;
            }
        }
        if (all_finished) {
            break;
        }

        bool made_progress_this_iteration = false;
        for (size_t reader_idx = 0; reader_idx < readers_.size(); reader_idx++) {
            if (!futures[reader_idx].valid() ||
                (futures[reader_idx].wait_for(std::chrono::microseconds(0)) ==
                 std::future_status::ready)) {
                if (futures[reader_idx].valid()) {
                    futures[reader_idx].get();
                }
                if (prefetch_queues_[reader_idx]->size() >= prefetch_queue_capacity_) {
                    // queue is full, skip
                    prefetch_metrics_->queue_full_count.fetch_add(1, kMetricsMemoryOrder);
                    continue;
                }
                if (readers_pos_[reader_idx]->load() != std::numeric_limits<uint64_t>::max()) {
                    futures[reader_idx] =
                        Via(executor_.get(), [this, reader_idx]() { ReadBatch(reader_idx); });
                    made_progress_this_iteration = true;
                }
            }
        }
        if (!made_progress_this_iteration) {
            std::unique_lock<std::mutex> lock(working_mutex_);
            cv_.wait(lock, [this] {
                if (is_shutdown_) {
                    return true;
                }
                for (size_t i = 0; i < reader_is_working_.size(); i++) {
                    if (reader_is_working_[i]) {
                        continue;
                    }
                    if (prefetch_queues_[i]->size() >= prefetch_queue_capacity_) {
                        continue;
                    }
                    if (readers_pos_[i]->load() == std::numeric_limits<uint64_t>::max()) {
                        continue;
                    }
                    return true;
                }
                return false;
            });
        }
    }
    Wait(futures);
}

void PrefetchFileBatchReaderImpl::ReadBatch(size_t reader_idx) {
    Status status = DoReadBatch(reader_idx);
    if (!status.ok()) {
        SetReadStatus(status);
    }
}

std::optional<std::pair<uint64_t, uint64_t>> PrefetchFileBatchReaderImpl::GetCurrentReadRange(
    size_t reader_idx) const {
    const auto& read_ranges = read_ranges_in_group_[reader_idx];
    const auto& current_pos = readers_pos_[reader_idx];
    uint64_t current_pos_value = current_pos->load();

    for (const auto& range : read_ranges) {
        if (current_pos_value < range.second) {
            return range;
        }
    }
    return std::nullopt;
}

Status PrefetchFileBatchReaderImpl::EnsureReaderPosition(
    size_t reader_idx, const std::pair<uint64_t, uint64_t>& current_read_range) const {
    uint64_t pos = std::max(readers_pos_[reader_idx]->load(), current_read_range.first);
    PAIMON_ASSIGN_OR_RAISE(uint64_t next_row_to_read, readers_[reader_idx]->GetNextRowToRead());
    if (next_row_to_read != pos) {
        prefetch_metrics_->seek_count.fetch_add(1, kMetricsMemoryOrder);
        return readers_[reader_idx]->SeekToRow(pos);
    }
    return Status::OK();
}

std::optional<std::pair<uint64_t, uint64_t>> PrefetchFileBatchReaderImpl::FindReadRangeContaining(
    size_t reader_idx, uint64_t row_id) const {
    for (const auto& range : read_ranges_in_group_[reader_idx]) {
        if (row_id >= range.first && row_id < range.second) {
            return range;
        }
    }
    return std::nullopt;
}

Status PrefetchFileBatchReaderImpl::HandleReadResult(
    size_t reader_idx, const std::pair<uint64_t, uint64_t>& read_range,
    ReadBatchWithBitmap&& read_batch_with_bitmap) {
    auto& prefetch_queue = prefetch_queues_[reader_idx];
    if (!BatchReader::IsEofBatch(read_batch_with_bitmap)) {
        auto& [read_batch, bitmap] = read_batch_with_bitmap;
        auto& [c_array, c_schema] = read_batch;
        std::vector<uint64_t> global_row_ids;
        global_row_ids.reserve(c_array->length);
        for (int64_t i = 0; i < c_array->length; ++i) {
            PAIMON_ASSIGN_OR_RAISE(uint64_t global_row_id,
                                   readers_[reader_idx]->GetPreviousBatchFileRowId(i));
            global_row_ids.push_back(global_row_id);
        }
        if (global_row_ids.empty()) {
            prefetch_metrics_->discarded_batches.fetch_add(1, kMetricsMemoryOrder);
            ReaderUtils::ReleaseReadBatch(std::move(read_batch));
            return Status::OK();
        }
        auto [slice_begin, slice_end] = ComputeBatchSliceByReadRange(global_row_ids, read_range);
        // slice_begin should always be 0, records before read_range.first have been consumed or
        // filtered out.
        if (slice_begin != 0) {
            return Status::Invalid(fmt::format("Slice begin is {}, which is not 0.", slice_begin));
        }

        if (0 == slice_end) {
            // fully out of range, data before global_row_ids has been filtered out
            // find the read range that contains the first row id and put it into queue in advance.
            std::optional<std::pair<uint64_t, uint64_t>> owner_range =
                FindReadRangeContaining(reader_idx, global_row_ids[0]);
            if (owner_range == std::nullopt) {
                readers_pos_[reader_idx]->store(global_row_ids[0]);
                prefetch_metrics_->discarded_batches.fetch_add(1, kMetricsMemoryOrder);
                ReaderUtils::ReleaseReadBatch(std::move(read_batch));
                return Status::OK();
            }
            // Recurses at most once: global_row_ids[0] is within owner_range, so the recursive
            // call cannot compute a zero slice end again.
            return HandleReadResult(reader_idx, owner_range.value(),
                                    std::move(read_batch_with_bitmap));
        } else if (slice_end < c_array->length) {
            // partially out of range, data before read_range.second has been effectively consumed
            readers_pos_[reader_idx]->store(read_range.second);
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                              arrow::ImportArray(c_array.get(), c_schema.get()));
            std::shared_ptr<arrow::Array> sliced_array = array->Slice(/*offset=*/0, slice_end);
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::Array> normalized_array,
                ArrowUtils::NormalizeArrayOffsets(sliced_array, arrow_pool_.get()));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                arrow::ExportArray(*normalized_array, c_array.get(), c_schema.get()));
            PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(c_array.get(), c_schema.get(), arrow_pool_));
            bitmap.RemoveRange(slice_end, array->length());
            global_row_ids =
                std::vector<uint64_t>(global_row_ids.begin(), global_row_ids.begin() + slice_end);
        } else {
            // all within the range, data before readers_[reader_idx]->GetNextRowToRead() has been
            // effectively consumed
            PAIMON_ASSIGN_OR_RAISE(uint64_t next_row_to_read,
                                   readers_[reader_idx]->GetNextRowToRead());
            readers_pos_[reader_idx]->store(next_row_to_read);
        }
        if (bitmap.IsEmpty()) {
            prefetch_metrics_->discarded_batches.fetch_add(1, kMetricsMemoryOrder);
            ReaderUtils::ReleaseReadBatch(std::move(read_batch));
            return Status::OK();
        }
        prefetch_queue->push(
            {read_range, std::move(read_batch_with_bitmap), std::move(global_row_ids)});
        prefetch_metrics_->produced_batches.fetch_add(1, kMetricsMemoryOrder);
        const uint64_t queue_depth =
            prefetch_metrics_->queue_depth.fetch_add(1, kMetricsMemoryOrder) + 1;
        UpdateMax(&prefetch_metrics_->queue_depth_max, queue_depth);
    } else {
        std::pair<uint64_t, uint64_t> eof_range;
        PAIMON_ASSIGN_OR_RAISE(eof_range, EofRange());
        prefetch_queue->push({eof_range, std::move(read_batch_with_bitmap), {}});
        const uint64_t queue_depth =
            prefetch_metrics_->queue_depth.fetch_add(1, kMetricsMemoryOrder) + 1;
        UpdateMax(&prefetch_metrics_->queue_depth_max, queue_depth);
        readers_pos_[reader_idx]->store(std::numeric_limits<uint64_t>::max());
    }
    return Status::OK();
}

Status PrefetchFileBatchReaderImpl::DoReadBatch(size_t reader_idx) {
    PAIMON_RETURN_NOT_OK(GetReadStatus());
    if (is_shutdown_) {
        return Status::OK();
    }
    std::optional<std::pair<uint64_t, uint64_t>> current_read_range =
        GetCurrentReadRange(reader_idx);
    if (current_read_range == std::nullopt) {
        // No more read ranges for this reader, gracefully exit.
        return Status::OK();
    }
    ScopeGuard guard([&]() {
        std::unique_lock<std::mutex> lock(working_mutex_);
        reader_is_working_[reader_idx] = false;
        cv_.notify_one();
    });
    {
        std::unique_lock<std::mutex> lock(working_mutex_);
        reader_is_working_[reader_idx] = true;
    }

    const auto& read_range = current_read_range.value();
    FileBatchReader* reader = readers_[reader_idx].get();
    PAIMON_RETURN_NOT_OK(EnsureReaderPosition(reader_idx, read_range));

    const auto read_start = std::chrono::steady_clock::now();
    Result<ReadBatchWithBitmap> read_result = reader->NextBatchWithBitmap();
    prefetch_metrics_->histograms->ObserveHistogram(PrefetchMetrics::READER_READ_LATENCY_US,
                                                    ElapsedMicros(read_start));
    PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap read_batch_with_bitmap, std::move(read_result));

    return HandleReadResult(reader_idx, read_range, std::move(read_batch_with_bitmap));
}

void PrefetchFileBatchReaderImpl::EnsureBackgroundThread() {
    if (!background_thread_) {
        background_thread_ =
            std::make_unique<std::thread>(&PrefetchFileBatchReaderImpl::Workloop, this);
    }
}

Status PrefetchFileBatchReaderImpl::Warmup() {
    // Not an error: a reader whose ranges are not set yet, or one already shut down, has nothing
    // to warm up. NextBatchWithBitmap still rejects the former, so a genuinely unprepared read is
    // not hidden by returning OK here.
    if (!read_ranges_freshed_ || is_shutdown_) {
        return Status::OK();
    }
    EnsureBackgroundThread();
    return Status::OK();
}

Result<BatchReader::ReadBatchWithBitmap> PrefetchFileBatchReaderImpl::NextBatchWithBitmap() {
    if (!read_ranges_freshed_) {
        return Status::Invalid("prefetch reader read ranges are not initialized");
    }
    EnsureBackgroundThread();

    const auto wait_start = std::chrono::steady_clock::now();
    while (true) {
        PAIMON_RETURN_NOT_OK(GetReadStatus());
        if (is_shutdown_) {
            return Status::Invalid(
                "prefetch reader has inconsistent state, maybe read while closing reader or change "
                "read schema");
        }
        std::optional<std::pair<uint64_t, uint64_t>> min_range;
        size_t eof_count = 0;
        size_t value_count = 0;
        for (auto& prefetch_queue : prefetch_queues_) {
            PAIMON_RETURN_NOT_OK(GetReadStatus());
            const PrefetchBatch* peek_batch = prefetch_queue->try_front();
            if (!peek_batch) {
                continue;
            }
            if (min_range == std::nullopt) {
                min_range = peek_batch->read_range;
            } else {
                if (peek_batch->read_range.first < min_range.value().first) {
                    min_range = peek_batch->read_range;
                }
            }
            value_count++;
            PAIMON_ASSIGN_OR_RAISE(bool is_eof_range, IsEofRange(peek_batch->read_range));
            if (is_eof_range) {
                eof_count++;
                continue;
            }

            const auto& current_read_range = read_ranges_.front();
            if (peek_batch->read_range == current_read_range) {
                auto prefetch_batch = prefetch_queue->try_pop();
                {
                    std::unique_lock<std::mutex> lock(working_mutex_);
                    cv_.notify_one();
                }
                current_batch_global_row_ids_ = std::move(prefetch_batch.value().global_row_ids);
                prefetch_metrics_->consumed_batches.fetch_add(1, kMetricsMemoryOrder);
                prefetch_metrics_->queue_depth.fetch_sub(1, kMetricsMemoryOrder);
                prefetch_metrics_->histograms->ObserveHistogram(
                    PrefetchMetrics::CONSUMER_WAIT_LATENCY_US, ElapsedMicros(wait_start));
                return std::move(prefetch_batch).value().batch;
            }
        }
        if (eof_count == prefetch_queues_.size()) {
            const PrefetchBatch* peek_batch = prefetch_queues_[0]->try_front();
            if (peek_batch == nullptr) {
                assert(false);
                return Status::Invalid("peek batch not suppose to be nullptr");
            }
            current_batch_global_row_ids_.clear();
            prefetch_metrics_->histograms->ObserveHistogram(
                PrefetchMetrics::CONSUMER_WAIT_LATENCY_US, ElapsedMicros(wait_start));
            return BatchReader::MakeEofBatchWithBitmap();
        }
        if (value_count == prefetch_queues_.size()) {
            while (true) {
                if (read_ranges_.empty()) {
                    break;
                }
                const auto& current_read_range = read_ranges_.front();
                if (current_read_range.first < min_range.value().first) {
                    read_ranges_.pop_front();
                } else {
                    break;
                }
            }
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }
    }
}

Status PrefetchFileBatchReaderImpl::SeekToRow(uint64_t row_number) {
    return Status::NotImplemented("not support seek to row for prefetch reader");
}

std::shared_ptr<Metrics> PrefetchFileBatchReaderImpl::GetReaderMetrics() const {
    auto result = std::make_shared<MetricsImpl>();
    if (need_prefetch_) {
        result->Merge(MetricsImpl::CollectReadMetrics(readers_));
    } else if (!readers_.empty()) {
        result->Merge(readers_[0]->GetReaderMetrics());
    }

    auto set_prefetch_counter = [&result](const char* name, const std::atomic<uint64_t>& value) {
        result->SetCounter(name, value.load(kMetricsMemoryOrder));
    };
    set_prefetch_counter(PrefetchMetrics::READ_RANGES_TOTAL, prefetch_metrics_->read_ranges_total);
    set_prefetch_counter(PrefetchMetrics::READ_RANGES_AFTER_BITMAP,
                         prefetch_metrics_->read_ranges_after_bitmap);
    set_prefetch_counter(PrefetchMetrics::SEEK_COUNT, prefetch_metrics_->seek_count);
    set_prefetch_counter(PrefetchMetrics::PRODUCED_BATCHES, prefetch_metrics_->produced_batches);
    set_prefetch_counter(PrefetchMetrics::CONSUMED_BATCHES, prefetch_metrics_->consumed_batches);
    set_prefetch_counter(PrefetchMetrics::DISCARDED_BATCHES, prefetch_metrics_->discarded_batches);
    set_prefetch_counter(PrefetchMetrics::ERRORS, prefetch_metrics_->errors);
    set_prefetch_counter(PrefetchMetrics::ADAPTIVE_DISABLED_COUNT,
                         prefetch_metrics_->adaptive_disabled_count);
    set_prefetch_counter(PrefetchMetrics::QUEUE_FULL_COUNT, prefetch_metrics_->queue_full_count);
    result->SetGauge(PrefetchMetrics::ENABLED,
                     prefetch_metrics_->enabled.load(kMetricsMemoryOrder) ? 1.0 : 0.0);
    result->SetGauge(PrefetchMetrics::PARALLELISM,
                     prefetch_metrics_->enabled.load(kMetricsMemoryOrder)
                         ? static_cast<double>(parallel_num_)
                         : 1.0);
    result->SetGauge(PrefetchMetrics::QUEUE_DEPTH,
                     static_cast<double>(prefetch_metrics_->queue_depth.load(kMetricsMemoryOrder)));
    result->SetGauge(
        PrefetchMetrics::QUEUE_DEPTH_MAX,
        static_cast<double>(prefetch_metrics_->queue_depth_max.load(kMetricsMemoryOrder)));
    result->Merge(prefetch_metrics_->histograms);
    if (cache_) {
        // PR #209 owns the read-ahead cache metrics. Keep collecting its file-level
        // hit/miss counters without defining another C++ metrics surface here.
        std::shared_ptr<Metrics> cache_metrics = std::make_shared<MetricsImpl>();
        cache_->CollectMetrics(&cache_metrics);
        result->Merge(cache_metrics);
    }

    if (!io_metrics_) {
        return result;
    }
    auto set_io_counter = [&result](const char* name, const std::atomic<uint64_t>& value) {
        result->SetCounter(name, value.load(kMetricsMemoryOrder));
    };
    set_io_counter(PrefetchIoMetrics::READ_REQUESTS, io_metrics_->read_requests);
    set_io_counter(PrefetchIoMetrics::READ_REQUESTED_BYTES, io_metrics_->read_requested_bytes);
    set_io_counter(PrefetchIoMetrics::READ_PHYSICAL_BYTES, io_metrics_->read_physical_bytes);
    set_io_counter(PrefetchIoMetrics::READ_FAILED, io_metrics_->read_failed);
    set_io_counter(PrefetchIoMetrics::READ_LATENCY_COUNT, io_metrics_->read_latency_count);
    set_io_counter(PrefetchIoMetrics::READ_LATENCY_SUM_US, io_metrics_->read_latency_sum_us);
    set_io_counter(PrefetchIoMetrics::ASYNC_REQUESTS, io_metrics_->async_requests);
    set_io_counter(PrefetchIoMetrics::ASYNC_REQUESTED_BYTES, io_metrics_->async_requested_bytes);
    set_io_counter(PrefetchIoMetrics::ASYNC_PHYSICAL_BYTES, io_metrics_->async_physical_bytes);
    set_io_counter(PrefetchIoMetrics::ASYNC_COMPLETED, io_metrics_->async_completed);
    set_io_counter(PrefetchIoMetrics::ASYNC_FAILED, io_metrics_->async_failed);
    set_io_counter(PrefetchIoMetrics::ASYNC_LATENCY_COUNT, io_metrics_->async_latency_count);
    set_io_counter(PrefetchIoMetrics::ASYNC_LATENCY_SUM_US, io_metrics_->async_latency_sum_us);
    result->SetGauge(PrefetchIoMetrics::ASYNC_PENDING,
                     static_cast<double>(io_metrics_->async_pending.load(kMetricsMemoryOrder)));
    return result;
}

Result<std::unique_ptr<::ArrowSchema>> PrefetchFileBatchReaderImpl::GetFileSchema() const {
    assert(!readers_.empty());
    return readers_[0]->GetFileSchema();
}

Result<uint64_t> PrefetchFileBatchReaderImpl::GetPreviousBatchFileRowId(
    uint64_t batch_row_id) const {
    if (current_batch_global_row_ids_.empty()) {
        return Status::Invalid(
            "Last batch is not read or last batch is empty, cannot get previous batch global row "
            "id");
    }
    if (batch_row_id >= current_batch_global_row_ids_.size()) {
        return Status::Invalid(
            fmt::format("batch_row_id {} is out of range, last batch row count is {}", batch_row_id,
                        current_batch_global_row_ids_.size()));
    }
    return current_batch_global_row_ids_[batch_row_id];
}

Result<uint64_t> PrefetchFileBatchReaderImpl::GetNumberOfRows() const {
    assert(!readers_.empty());
    return readers_[0]->GetNumberOfRows();
}

Result<uint64_t> PrefetchFileBatchReaderImpl::GetNextRowToRead() const {
    assert(false);
    return -1;
}

void PrefetchFileBatchReaderImpl::SetReadStatus(const Status& status) {
    if (!status.ok()) {
        prefetch_metrics_->errors.fetch_add(1, kMetricsMemoryOrder);
    }
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    read_status_ = status;
}

Status PrefetchFileBatchReaderImpl::GetReadStatus() const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    return read_status_;
}
Result<bool> PrefetchFileBatchReaderImpl::IsEofRange(
    const std::pair<uint64_t, uint64_t>& read_range) const {
    PAIMON_ASSIGN_OR_RAISE(uint64_t num_rows, GetNumberOfRows());
    return read_range.first >= num_rows;
}

Result<std::pair<uint64_t, uint64_t>> PrefetchFileBatchReaderImpl::EofRange() const {
    PAIMON_ASSIGN_OR_RAISE(uint64_t num_rows, GetNumberOfRows());
    return std::make_pair(num_rows, num_rows + 1);
}

void PrefetchFileBatchReaderImpl::Close() {
    // CleanUp() no longer resets the read-ahead cache: ConcatBatchReader closes file readers as
    // soon as they reach EOF, and the cache hit/miss counters must remain readable through
    // GetReaderMetrics() after that. The cache is reset only when the reader is reused via
    // SetReadSchema()/RefreshReadRanges().
    (void)CleanUp();
    if (cache_) {
        // Free the prefetched buffers of this file right away (ConcatBatchReader keeps
        // closed file readers alive until the whole scan finishes), but keep the
        // counters for GetReaderMetrics().
        cache_->ReleaseBuffers();
    }
    for (const auto& reader : readers_) {
        reader->Close();
    }
}

}  // namespace paimon
