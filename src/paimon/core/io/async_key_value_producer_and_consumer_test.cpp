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

#include "paimon/core/io/async_key_value_producer_and_consumer.h"

#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class ConsumerBarrier {
 public:
    explicit ConsumerBarrier(size_t consumer_count) : consumer_count_(consumer_count) {}

    void Wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        ++arrived_count_;
        if (arrived_count_ == consumer_count_) {
            condition_.notify_all();
        } else {
            condition_.wait(lock, [this]() { return arrived_count_ >= consumer_count_; });
        }
    }

 private:
    const size_t consumer_count_;
    size_t arrived_count_ = 0;
    std::mutex mutex_;
    std::condition_variable condition_;
};

class TestKeyValueConsumer : public RowToArrowArrayConverter<KeyValue, KeyValueBatch> {
 public:
    explicit TestKeyValueConsumer(std::shared_ptr<ConsumerBarrier> barrier)
        : RowToArrowArrayConverter(/*reserve_count=*/0, std::vector<AppendValueFunc>(), nullptr,
                                   nullptr),
          barrier_(std::move(barrier)) {}

    Result<KeyValueBatch> NextBatch(const std::vector<KeyValue>&) override {
        barrier_->Wait();
        return KeyValueBatch();
    }

 private:
    std::shared_ptr<ConsumerBarrier> barrier_;
};

class TestSortMergeReader : public SortMergeReader {
 public:
    explicit TestSortMergeReader(size_t row_count) : rows_(row_count) {}

    class Iterator : public SortMergeReader::Iterator {
     public:
        explicit Iterator(TestSortMergeReader* reader) : reader_(reader) {}

        Result<bool> HasNext() override {
            return reader_->next_row_ < reader_->rows_.size();
        }

        KeyValue&& Next() override {
            return std::move(reader_->rows_[reader_->next_row_++]);
        }

     private:
        TestSortMergeReader* reader_;
    };

    Result<std::unique_ptr<SortMergeReader::Iterator>> NextBatch() override {
        if (iterator_created_) {
            return std::unique_ptr<SortMergeReader::Iterator>();
        }
        iterator_created_ = true;
        return std::make_unique<Iterator>(this);
    }

    void Close() override {}

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return nullptr;
    }

 private:
    std::vector<KeyValue> rows_;
    size_t next_row_ = 0;
    bool iterator_created_ = false;
};

}  // namespace

TEST(AsyncKeyValueProducerAndConsumerTest, TestEarlyCloseWithBlockedConsumers) {
    constexpr int32_t kConsumerCount = 32;
    std::shared_ptr<ConsumerBarrier> barrier = std::make_shared<ConsumerBarrier>(kConsumerCount);
    auto create_consumer =
        [barrier]() -> Result<std::unique_ptr<RowToArrowArrayConverter<KeyValue, KeyValueBatch>>> {
        std::unique_ptr<RowToArrowArrayConverter<KeyValue, KeyValueBatch>> consumer =
            std::make_unique<TestKeyValueConsumer>(barrier);
        return consumer;
    };

    AsyncKeyValueProducerAndConsumer<KeyValue, KeyValueBatch> producer_and_consumer(
        std::make_unique<TestSortMergeReader>(kConsumerCount * 4), create_consumer,
        /*batch_size=*/1, kConsumerCount, /*pool=*/nullptr);

    ASSERT_OK_AND_ASSIGN(KeyValueBatch first_batch, producer_and_consumer.NextBatch());
    ASSERT_FALSE(first_batch.batch);
    producer_and_consumer.Close();
}

}  // namespace paimon::test
