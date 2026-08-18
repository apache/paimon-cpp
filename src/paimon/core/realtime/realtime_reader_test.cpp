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

#include "paimon/core/realtime/realtime_reader.h"

#include <memory>
#include <optional>

#include "paimon/arrow/abi.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class TestingReadView : public RealtimeReadView {
 public:
    std::optional<OffsetRange> GetOffsetRange() const override {
        return std::nullopt;
    }
};

class TestingBatchReader : public BatchReader {
 public:
    Result<ReadBatch> NextBatch() override {
        return MakeEofBatch();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return nullptr;
    }

    void Close() override {}
};

TEST(RealtimeReaderTest, TestRejectsIncompleteReader) {
    ASSERT_NOK_WITH_MSG(
        RealtimeReader::Create(/*read_view=*/nullptr, std::make_unique<TestingBatchReader>()),
        "view is null");
    ASSERT_NOK_WITH_MSG(RealtimeReader::Create(std::make_shared<TestingReadView>(),
                                               /*reader=*/nullptr),
                        "inner reader is null");
}

}  // namespace
}  // namespace paimon::test
