/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/core/io/row_to_arrow_array_converter.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/sort_merge_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "tbb/concurrent_queue.h"

namespace paimon {
template <typename T, typename R>
class AsyncKeyValueConsumer;
class Metrics;

enum class AsyncKeyValueBatchType {
    DATA,
    CHANGELOG,
};

struct AsyncKeyValueRowsBatch {
    AsyncKeyValueBatchType type = AsyncKeyValueBatchType::DATA;
    std::vector<KeyValue> rows;
};

class AsyncKeyValueBatchSink {
 public:
    virtual ~AsyncKeyValueBatchSink() = default;

    virtual Status Write(AsyncKeyValueBatchType type, std::vector<KeyValue>&& rows) = 0;

    virtual bool IsCancelled() const = 0;
};

class AsyncKeyValueBatchProducer {
 public:
    virtual ~AsyncKeyValueBatchProducer() = default;

    virtual Status Produce(AsyncKeyValueBatchSink* sink) = 0;

    virtual std::shared_ptr<Metrics> GetReaderMetrics() const;

    virtual void Close() {}

 protected:
    // Limits the number of rows sent to one Arrow projection call.
    static int32_t NormalizeProjectionBatchSize(int32_t batch_size) {
        return std::min(batch_size, MAX_PROJECTION_BATCH_SIZE);
    }

 private:
    static constexpr int32_t MAX_PROJECTION_BATCH_SIZE = 100000;
};

class SortMergeReaderBatchProducer : public AsyncKeyValueBatchProducer {
 public:
    SortMergeReaderBatchProducer(std::unique_ptr<SortMergeReader>&& sort_merge_reader,
                                 int32_t batch_size);

    Status Produce(AsyncKeyValueBatchSink* sink) override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override;

    void Close() override;

 private:
    std::unique_ptr<SortMergeReader> sort_merge_reader_;
    int32_t batch_size_;
    bool closed_ = false;
};

template <typename R>
struct AsyncKeyValueResultBatch {
    AsyncKeyValueBatchType type = AsyncKeyValueBatchType::DATA;
    R result;
};

// Asynchronous iterates AsyncKeyValueBatchProducer(producer) and row-to-array conversion
// (consumer), support multi-threaded conversion, R can be BatchReader::ReadBatch or KeyValueBatch.
template <typename T, typename R>
class AsyncKeyValueProducerAndConsumer {
 public:
    using ConsumerCreator =
        std::function<Result<std::unique_ptr<RowToArrowArrayConverter<T, R>>>()>;

    AsyncKeyValueProducerAndConsumer(std::unique_ptr<AsyncKeyValueBatchProducer>&& producer,
                                     ConsumerCreator create_consumer, int32_t consumer_thread_num);

    ~AsyncKeyValueProducerAndConsumer() {
        CleanUp();
    }

    Result<R> NextBatch();

    Result<AsyncKeyValueResultBatch<R>> NextBatchWithType();

    std::shared_ptr<Metrics> GetReaderMetrics() const {
        return producer_->GetReaderMetrics();
    }

    void Close() {
        CleanUp();
        producer_->Close();
    }

 private:
    static constexpr int32_t RESULT_BATCH_COUNT = 3;

    void CleanUpQueue();
    Status ProduceLoop();
    void CleanUp();
    Status CheckStatus() const;
    Status CheckStatusAndCleanUp();

 private:
    int32_t consumer_thread_num_;
    ConsumerCreator create_consumer_;
    std::unique_ptr<AsyncKeyValueBatchProducer> producer_;

    // produce: merge sort KeyValue and push result KeyValue to kv_queue_, consume: project KeyValue
    // to arrow array and push result array to result_queue_
    std::atomic<bool> consume_finished_ = false;
    std::atomic<bool> next_batch_finished_ = false;
    std::shared_future<Status> producer_future_;
    std::vector<std::unique_ptr<AsyncKeyValueConsumer<T, R>>> consumers_;
    std::atomic<int32_t> consumer_finished_count_ = 0;
    tbb::concurrent_bounded_queue<AsyncKeyValueRowsBatch> kv_queue_;
    tbb::concurrent_bounded_queue<AsyncKeyValueResultBatch<R>> result_queue_;
};

template <typename T, typename R>
class AsyncKeyValueConsumer {
 public:
    AsyncKeyValueConsumer(std::unique_ptr<RowToArrowArrayConverter<T, R>>&& consumer,
                          std::atomic<bool>& consume_finished,
                          std::atomic<int32_t>& consumer_finished_count,
                          tbb::concurrent_bounded_queue<AsyncKeyValueRowsBatch>& kv_queue,
                          tbb::concurrent_bounded_queue<AsyncKeyValueResultBatch<R>>& result_queue);

    ~AsyncKeyValueConsumer() {
        CleanUp();
    }

    Status GetStatus() const;
    void CleanUp();

 private:
    Status ConsumeLoop();

 private:
    std::unique_ptr<RowToArrowArrayConverter<T, R>> consumer_;
    std::shared_future<Status> consumer_future_;
    std::atomic<bool>& consume_finished_;
    std::atomic<int32_t>& consumer_finished_count_;
    tbb::concurrent_bounded_queue<AsyncKeyValueRowsBatch>& kv_queue_;
    tbb::concurrent_bounded_queue<AsyncKeyValueResultBatch<R>>& result_queue_;
};

}  // namespace paimon
