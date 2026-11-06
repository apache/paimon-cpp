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
#include "paimon/common/reader/file_read_ahead.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/io/cache_input_stream.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/warmup_file_batch_reader.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/format/reader_builder.h"

namespace paimon {

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

constexpr std::memory_order kMetricsMemoryOrder = std::memory_order_relaxed;

template <typename Func>
auto RunOrSubmit(Executor* executor, Func&& task) -> std::future<decltype(task())> {
    if (executor) {
        return Via(executor, std::forward<Func>(task));
    }
    std::promise<decltype(task())> promise;
    auto future = promise.get_future();
    promise.set_value(task());
    return future;
}

uint64_t ElapsedMicros(const std::chrono::steady_clock::time_point& start) {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                     std::chrono::steady_clock::now() - start)
                                     .count());
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

}  // namespace

std::shared_ptr<FileReadAhead> FileReadAhead::Create(const std::shared_ptr<ReadAheadCache>& cache,
                                                     const std::string& path) {
    return std::shared_ptr<FileReadAhead>(new FileReadAhead(cache, path));
}

Result<FileReadAhead::OpenedReaders> FileReadAhead::Open(
    const std::string& path, int64_t file_size, const ReaderBuilder* builder,
    const std::shared_ptr<FileSystem>& fs, uint32_t reader_count,
    const std::shared_ptr<Executor>& executor, bool cache_enabled, const CacheConfig& cache_config,
    bool enable_io_metrics, const std::shared_ptr<MemoryPool>& pool) {
    if (file_size < 0 || !builder || !fs || reader_count == 0 || (!executor && reader_count > 1)) {
        return Status::Invalid("invalid file read-ahead open arguments");
    }
    std::shared_ptr<PrefetchIoMetricsState> io_metrics;
    if (enable_io_metrics) {
        io_metrics = std::make_shared<PrefetchIoMetricsState>();
    }
    // Synchronous reads need no executor and open/build inline. With an executor, opening a file
    // mostly waits on remote I/O, so the streams are all opened concurrently
    // instead of opening the cache's stream first and paying its round trip before the readers'
    // begin. Both waves run on the read context's `executor`, which is sized to the prefetch
    // parallelism, so no separate pool is spun up and torn down per file and the open/build
    // concurrency follows the configured parallelism instead of a fixed default. That reuse is
    // safe: neither wave's tasks wait on one another or on `executor`, and Create never runs on
    // an `executor` worker, so the CollectAll calls below only ever wait on tasks that progress.
    // The build has to be a second wave rather than folded into the first: each build task wraps
    // its stream in the cache, and a task waiting on the cache would block a worker while the
    // cache's own open is still queued behind it. Wave one only opens, so its tasks never wait on
    // one another.
    const uint32_t open_count = cache_enabled ? reader_count + 1 : reader_count;
    std::vector<std::future<Result<std::unique_ptr<InputStream>>>> open_futures;
    open_futures.reserve(open_count);
    for (uint32_t i = 0; i < open_count; i++) {
        open_futures.push_back(RunOrSubmit(
            executor.get(), [&fs, &path, file_size]() -> Result<std::unique_ptr<InputStream>> {
                return fs->Open(FileStatus(path, file_size));
            }));
    }
    // The tasks only reference locals of this frame, which stay alive because both CollectAll
    // calls below drain every future before returning.
    std::vector<Result<std::unique_ptr<InputStream>>> opened_streams = CollectAll(open_futures);
    std::vector<std::unique_ptr<InputStream>> streams;
    streams.reserve(opened_streams.size());
    for (auto& opened_stream : opened_streams) {
        if (!opened_stream.ok()) {
            return opened_stream.status();
        }
        streams.push_back(std::move(opened_stream).value());
    }

    size_t next_stream = 0;
    std::shared_ptr<ReadAheadCache> cache;
    if (cache_enabled) {
        std::shared_ptr<InputStream> input_stream = std::move(streams[next_stream++]);
        if (io_metrics) {
            input_stream = std::make_shared<MetricsInputStream>(input_stream, io_metrics);
        }
        // The file size lets the cache align its blocks to the end of the file,
        // where the metadata the readers read before any range is registered
        // lives. A zero size means unknown and disables the block cache.
        cache = std::make_shared<ReadAheadCache>(input_stream, cache_config,
                                                 static_cast<uint64_t>(file_size), pool);
    }
    auto state = Create(cache, path);
    state->io_metrics_ = io_metrics;
    // Wave two builds the readers, which reads each file's footer and so still needs the cache
    // above to exist. The builds are concurrent, like the opens were.
    std::vector<std::future<Result<std::unique_ptr<FileBatchReader>>>> futures;
    futures.reserve(reader_count);
    for (uint32_t i = 0; i < reader_count; i++) {
        futures.push_back(RunOrSubmit(
            executor.get(),
            [&builder, &cache, &streams, i, next_stream, io_metrics,
             state]() -> Result<std::unique_ptr<FileBatchReader>> {
                std::unique_ptr<InputStream> input_stream = std::move(streams[next_stream + i]);
                if (io_metrics) {
                    input_stream =
                        std::make_unique<MetricsInputStream>(std::move(input_stream), io_metrics);
                }
                auto cache_input_stream = std::make_shared<CacheInputStream>(
                    std::move(input_stream), cache, CacheInputStream::CacheFactory(), state);
                return builder->Build(cache_input_stream);
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
    return OpenedReaders{std::move(readers), std::move(state)};
}

Result<std::unique_ptr<FileBatchReader>> FileReadAhead::OpenSynchronous(
    const std::string& path, int64_t file_size, const ReaderBuilder* builder,
    const std::shared_ptr<FileSystem>& fs, bool cache_enabled, const CacheConfig& cache_config,
    bool enable_io_metrics, WarmupLevel level, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (file_size < 0 || !builder || !fs) {
        return Status::Invalid("invalid synchronous file read-ahead open arguments");
    }
    std::shared_ptr<PrefetchIoMetricsState> io_metrics;
    if (enable_io_metrics) {
        io_metrics = std::make_shared<PrefetchIoMetricsState>();
    }
    auto state = Create(nullptr, path);
    state->io_metrics_ = io_metrics;
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> input_stream,
                           fs->Open(FileStatus(path, file_size)));
    if (io_metrics) {
        input_stream = std::make_unique<MetricsInputStream>(std::move(input_stream), io_metrics);
    }
    CacheInputStream::CacheFactory cache_factory;
    if (cache_enabled) {
        cache_factory = [state, fs, path, file_size, cache_config,
                         pool]() -> Result<std::shared_ptr<ReadAheadCache>> {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> cache_stream,
                                   fs->Open(FileStatus(path, file_size)));
            if (state->io_metrics_) {
                cache_stream =
                    std::make_shared<MetricsInputStream>(cache_stream, state->io_metrics_);
            }
            state->cache_ = std::make_shared<ReadAheadCache>(
                cache_stream, cache_config, static_cast<uint64_t>(file_size), pool);
            return state->cache_;
        };
    }
    auto stream = std::make_shared<CacheInputStream>(std::move(input_stream), nullptr,
                                                     std::move(cache_factory), state);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> reader, builder->Build(stream));
    // The byte-range interface, rather than the format name, determines cache/warmup support.
    if (!dynamic_cast<PrefetchFileBatchReader*>(reader.get())) {
        stream->ClearCacheFactory();
        return reader;
    }
    PAIMON_RETURN_NOT_OK(stream->EnableCache().status());
    state->SetSynchronous(true);
    std::shared_ptr<PrefetchFileBatchReader> capable_reader =
        checked_pointer_cast<PrefetchFileBatchReader>(std::move(reader));
    return std::make_unique<WarmupFileBatchReader>(std::move(capable_reader), std::move(state),
                                                   level);
}

Status FileReadAhead::Warmup(PrefetchFileBatchReader* reader) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!cache_ || warmed_) {
        return status_;
    }
    warmed_ = true;
    return PrepareRangesLocked(reader->PreBufferRange(), true);
}

Status FileReadAhead::PrepareRanges(PrefetchFileBatchReader* reader) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!cache_ || !warmed_) {
            return Status::OK();
        }
        if (!status_.ok()) {
            return status_;
        }
    }
    // Range discovery may read remote metadata. Keep it outside the context lock so parallel
    // format readers can discover their stripes concurrently. Reset/Close stop reader tasks first.
    Result<ByteRanges> ranges = reader->PreBufferRange();
    std::lock_guard<std::mutex> lock(mutex_);
    if (!cache_ || !warmed_) {
        return Status::OK();
    }
    if (!status_.ok()) {
        return status_;
    }
    return PrepareRangesLocked(std::move(ranges), false);
}

Status FileReadAhead::PrepareRangesLocked(Result<ByteRanges>&& ranges, bool initial) {
    if (!ranges.ok()) {
        status_ = ranges.status();
        ++error_count_;
        return status_;
    }
    std::vector<ByteRange> byte_ranges;
    for (const auto& range : ranges.value()) {
        byte_ranges.emplace_back(range.first, range.second);
    }
    auto added = cache_->AddRanges(std::move(byte_ranges));
    if (!added.ok()) {
        status_ = Status(added.status().code(), fmt::format("read-ahead ranges for file '{}': {}",
                                                            path_, added.status().ToString()));
        ++error_count_;
        return status_;
    }
    if (initial) {
        cache_->Warmup();
    } else if (added.value()) {
        cache_->Warmup(added.value().value());
    }
    return Status::OK();
}

void FileReadAhead::SetSynchronous(bool synchronous) {
    if (cache_) {
        cache_->SetBoundedRetention(synchronous);
    }
}

Status FileReadAhead::GetStatus() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return status_;
}

uint64_t FileReadAhead::GetErrorCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return error_count_;
}

void FileReadAhead::EndGeneration() {
    std::lock_guard<std::mutex> lock(mutex_);
    warmed_ = false;
}

void FileReadAhead::Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    warmed_ = false;
    status_ = Status::OK();
    if (cache_) {
        cache_->Reset();
    }
}

void FileReadAhead::ReleaseBuffers() {
    std::lock_guard<std::mutex> lock(mutex_);
    warmed_ = false;
    if (cache_) {
        cache_->ReleaseBuffers();
    }
}

std::shared_ptr<Metrics> FileReadAhead::GetMetrics() const {
    auto result = std::make_shared<MetricsImpl>();
    if (cache_) {
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

}  // namespace paimon
