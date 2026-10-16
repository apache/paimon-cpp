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

#include "paimon/core/utils/manifest_meta_reader.h"

#include <string>
#include <type_traits>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/scan_cancellation.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {
struct MetadataRootState {
    int32_t arrays = 0;
    int32_t schemas = 0;
    int32_t next = 0;
    int32_t close = 0;
    int32_t destroyed = 0;
};

template <typename Root>
struct CountedMetadataRoot {
    static void Wrap(Root* root, const std::shared_ptr<MetadataRootState>& state) {
        auto* owner = new CountedMetadataRoot{root->release, root->private_data, state};
        root->private_data = owner;
        root->release = [](Root* root) {
            std::unique_ptr<CountedMetadataRoot> owner(
                static_cast<CountedMetadataRoot*>(root->private_data));
            root->private_data = owner->data;
            root->release = owner->release;
            if constexpr (std::is_same_v<Root, ArrowArray>) {
                ++owner->state->arrays;
            } else {
                ++owner->state->schemas;
            }
            owner->release(root);
        };
    }

    void (*release)(Root*);
    void* data;
    std::shared_ptr<MetadataRootState> state;
};

class CancellationMetadataReader : public BatchReader {
 public:
    CancellationMetadataReader(std::shared_ptr<arrow::Array> data,
                               std::shared_ptr<MetadataRootState> state,
                               ScanCancellationToken* token, bool cancel)
        : data_(std::move(data)), state_(std::move(state)), token_(token), cancel_(cancel) {}

    ~CancellationMetadataReader() override {
        ++state_->destroyed;
    }

    Result<ReadBatch> NextBatch() override {
        ++state_->next;
        if (!failure.ok()) {
            token_->Cancel();
            return failure;
        }
        auto schema = std::make_unique<ArrowSchema>();
        std::unique_ptr<ArrowArray> array;
        if (data_) {
            array = std::make_unique<ArrowArray>();
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*data_, array.get(), schema.get()));
            CountedMetadataRoot<ArrowArray>::Wrap(array.get(), state_);
        } else {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportType(*arrow::int32(), schema.get()));
        }
        CountedMetadataRoot<ArrowSchema>::Wrap(schema.get(), state_);
        if (cancel_) {
            token_->Cancel();
        }
        return std::make_pair(std::move(array), std::move(schema));
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return nullptr;
    }
    void Close() override {
        ++state_->close;
    }
    Status failure;

 private:
    std::shared_ptr<arrow::Array> data_;
    std::shared_ptr<MetadataRootState> state_;
    ScanCancellationToken* token_;
    bool cancel_;
};

class CancellingAlignmentPool : public MemoryPool {
 public:
    explicit CancellingAlignmentPool(ScanCancellationToken* token) : token_(token) {}

    void* Malloc(uint64_t size, uint64_t alignment = 0) override {
        void* result = delegate_->Malloc(size, alignment);
        Observe();
        return result;
    }
    void* Realloc(void* pointer, size_t old_size, size_t new_size,
                  uint64_t alignment = 0) override {
        void* result = delegate_->Realloc(pointer, old_size, new_size, alignment);
        Observe();
        return result;
    }
    void Free(void* pointer, uint64_t size) override {
        delegate_->Free(pointer, size);
    }
    void Free(void* pointer, uint64_t size, uint64_t alignment) override {
        delegate_->Free(pointer, size, alignment);
    }
    uint64_t CurrentUsage() const override {
        return delegate_->CurrentUsage();
    }
    uint64_t MaxMemoryUsage() const override {
        return delegate_->MaxMemoryUsage();
    }
    int32_t allocations = 0;
    bool cancel = false;

 private:
    void Observe() {
        ++allocations;
        if (cancel) {
            token_->Cancel();
        }
    }
    ScanCancellationToken* token_;
    std::unique_ptr<MemoryPool> delegate_ = GetMemoryPool();
};
}  // namespace

TEST(ManifestMetaReaderTest, CancellationDuringAlignmentStopsBeforeTheNextField) {
    auto type = arrow::struct_({arrow::field("id", arrow::int32())});
    auto data = arrow::ipc::internal::json::ArrayFromJSON(type, "[[1], [2]]").ValueOrDie();
    auto one_extra =
        arrow::struct_({arrow::field("id", arrow::int32()), arrow::field("first", arrow::int64())});
    auto two_extra =
        arrow::struct_({arrow::field("id", arrow::int32()), arrow::field("first", arrow::int64()),
                        arrow::field("second", arrow::int64())});
    ScanCancellationToken live;
    auto baseline = std::make_shared<CancellingAlignmentPool>(&live);
    auto baseline_arrow = GetArrowPool(baseline);
    ASSERT_OK_AND_ASSIGN(auto aligned, ManifestMetaReader::AlignArrayWithSchema(
                                           data, one_extra, baseline_arrow.get(), &live));
    ASSERT_GT(baseline->allocations, 0);
    const int32_t one_field_allocations = baseline->allocations;
    aligned.reset();
    ASSERT_EQ(baseline->CurrentUsage(), 0);

    ScanCancellationToken token;
    auto pool = std::make_shared<CancellingAlignmentPool>(&token);
    auto arrow_pool = GetArrowPool(pool);
    pool->cancel = true;
    auto result =
        ManifestMetaReader::AlignArrayWithSchema(data, two_extra, arrow_pool.get(), &token);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsCancelled());
    ASSERT_TRUE(token.IsCancelled());
    ASSERT_EQ(pool->allocations, one_field_allocations);
    ASSERT_EQ(pool->CurrentUsage(), 0);
}

TEST(ManifestMetaReaderTest, CancellationAfterBatchAcquisitionReleasesBothRoots) {
    auto type = arrow::struct_({arrow::field("id", arrow::int32())});
    for (bool eof : {false, true}) {
        SCOPED_TRACE(eof);
        auto state = std::make_shared<MetadataRootState>();
        ScanCancellationToken token;
        std::shared_ptr<arrow::Array> data;
        if (!eof) {
            data = arrow::ipc::internal::json::ArrayFromJSON(type, "[[1], [2]]").ValueOrDie();
        }
        auto input = std::make_unique<CancellationMetadataReader>(data, state, &token, true);
        {
            ManifestMetaReader reader(std::move(input), type, GetArrowPool(GetDefaultPool()),
                                      &token);
            ASSERT_TRUE(reader.NextBatch().status().IsCancelled());
            ASSERT_EQ(state->next, 1);
            ASSERT_EQ(state->arrays, eof ? 0 : 1);
            ASSERT_EQ(state->schemas, 1);
        }
        ASSERT_EQ(state->close, 1);
        ASSERT_EQ(state->destroyed, 1);
    }
}

TEST(ManifestMetaReaderTest, CancelledEntryDoesNotReadAndNativeFailureKeepsItsIdentity) {
    auto type = arrow::struct_({arrow::field("id", arrow::int32())});
    for (bool before_read : {false, true}) {
        SCOPED_TRACE(before_read);
        auto state = std::make_shared<MetadataRootState>();
        ScanCancellationToken token;
        auto input = std::make_unique<CancellationMetadataReader>(nullptr, state, &token, false);
        input->failure = Status::IOError("metadata decoder failed");
        if (before_read) {
            token.Cancel();
        }
        {
            ManifestMetaReader reader(std::move(input), type, GetArrowPool(GetDefaultPool()),
                                      &token);
            auto result = reader.NextBatch();
            ASSERT_FALSE(result.ok());
            ASSERT_EQ(result.status().IsCancelled(), before_read);
            ASSERT_EQ(result.status().IsIOError(), !before_read);
            ASSERT_EQ(state->next, before_read ? 0 : 1);
        }
        ASSERT_EQ(state->close, 1);
        ASSERT_EQ(state->destroyed, 1);
        ASSERT_EQ(state->arrays, 0);
        ASSERT_EQ(state->schemas, 0);
    }
}

TEST(ManifestMetaReaderTest, ControlledExportRetainsAllocatorAfterReaderDestruction) {
    auto type = arrow::struct_({arrow::field("id", arrow::int32())});
    auto data = arrow::ipc::internal::json::ArrayFromJSON(type, "[[1], [2]]").ValueOrDie();
    auto state = std::make_shared<MetadataRootState>();
    ScanCancellationToken token;
    auto native_pool = std::shared_ptr<MemoryPool>(GetMemoryPool());
    std::weak_ptr<MemoryPool> retained = native_pool;
    auto pool = GetArrowPool(native_pool);
    auto input =
        std::make_unique<CancellationMetadataReader>(std::move(data), state, &token, false);
    auto target = arrow::struct_(
        {arrow::field("id", arrow::int32()), arrow::field("missing", arrow::int64())});
    auto reader = std::make_unique<ManifestMetaReader>(std::move(input), target, pool, &token);
    ASSERT_OK_AND_ASSIGN(auto batch, reader->NextBatch());
    ASSERT_TRUE(batch.first);
    ASSERT_TRUE(batch.second);
    ASSERT_GT(native_pool->CurrentUsage(), 0);
    reader.reset();
    pool.reset();
    native_pool.reset();
    ASSERT_EQ(state->close, 1);
    ASSERT_EQ(state->destroyed, 1);
    ASSERT_FALSE(retained.expired());
    ArrowArrayRelease(batch.first.get());
    ArrowSchemaRelease(batch.second.get());
    ASSERT_EQ(state->arrays, 1);
    ASSERT_EQ(state->schemas, 1);
    ASSERT_TRUE(retained.expired());
}

TEST(ManifestMetaReaderTest, TestNoNeedCompleteNonExistField) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::list(arrow::int32())),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::int64())),
        arrow::field("f2",
                     arrow::struct_({field("sub1", arrow::int64()), field("sub2", arrow::float64()),
                                     field("sub3", arrow::boolean())})),
    };
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [[1, 2, 3],    [["apple", 3], ["banana", 4]],          [10, 10.1, false]],
        [[4, 5],       [["cat", 5], ["dog", 6], ["mouse", 7]], [20, 20.1, true]],
        [[6],          [["elephant", 7], ["fox", 8]],          [null, 30.1, true]],
        [[7],          [["giraffe", 9]],                       [null, 40.1, true]],
        [null,         [["horse", 10], ["Panda", 11]],         [50, 50.1, null]],
        [[9],          null,                                   [60, 60.1, false]],
        [[10, 11, 12], [["rabbit", null], ["tiger", 13]],      null]
    ])")
                         .ValueOrDie();
    arrow::FieldVector target_fields = fields;
    auto target_arrow_type = arrow::struct_(target_fields);
    auto arrow_pool = GetArrowPool(GetDefaultPool());
    ASSERT_OK_AND_ASSIGN(auto ret, ManifestMetaReader::AlignArrayWithSchema(
                                       src_array, target_arrow_type, arrow_pool.get()));
    ASSERT_TRUE(src_array->Equals(ret)) << ret->ToString();
}
TEST(ManifestMetaReaderTest, TestCompleteNonExistFieldSimple) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::list(arrow::int32())),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::int64())),
        arrow::field("f2",
                     arrow::struct_({field("sub1", arrow::int64()), field("sub2", arrow::float64()),
                                     field("sub3", arrow::boolean())})),
    };
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [[1, 2, 3],    [["apple", 3], ["banana", 4]],          [10, 10.1, false]],
        [[4, 5],       [["cat", 5], ["dog", 6], ["mouse", 7]], [20, 20.1, true]],
        [[6],          [["elephant", 7], ["fox", 8]],          [null, 30.1, true]],
        [[7],          [["giraffe", 9]],                       [null, 40.1, true]],
        [null,         [["horse", 10], ["Panda", 11]],         [50, 50.1, null]],
        [[9],          null,                                   [60, 60.1, false]],
        [[10, 11, 12], [["rabbit", null], ["tiger", 13]],      null]
    ])")
                         .ValueOrDie();
    arrow::FieldVector target_fields = {
        arrow::field("f0", arrow::list(arrow::int32())),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::int64())),
        arrow::field("f3", arrow::list(arrow::utf8())),
        arrow::field(
            "f2", arrow::struct_({field("sub1", arrow::int64()), field("sub2", arrow::float64()),
                                  field("sub3", arrow::boolean()), field("sub4", arrow::int8())})),
    };
    auto target_arrow_type = arrow::struct_(target_fields);
    auto arrow_pool = GetArrowPool(GetDefaultPool());
    ASSERT_OK_AND_ASSIGN(auto ret, ManifestMetaReader::AlignArrayWithSchema(
                                       src_array, target_arrow_type, arrow_pool.get()));

    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(target_fields), R"([
        [[1, 2, 3],    [["apple", 3], ["banana", 4]],          null, [10, 10.1, false, null]],
        [[4, 5],       [["cat", 5], ["dog", 6], ["mouse", 7]], null, [20, 20.1, true, null]],
        [[6],          [["elephant", 7], ["fox", 8]],          null, [null, 30.1, true, null]],
        [[7],          [["giraffe", 9]],                       null, [null, 40.1, true, null]],
        [null,         [["horse", 10], ["Panda", 11]],         null, [50, 50.1, null, null]],
        [[9],          null,                                   null, [60, 60.1, false, null]],
        [[10, 11, 12], [["rabbit", null], ["tiger", 13]],      null, null]
    ])")
            .ValueOrDie();
    ASSERT_TRUE(expected_array->Equals(ret)) << ret->ToString();
}

TEST(ManifestMetaReaderTest, TestCompleteNonExistFieldNested) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::list(arrow::struct_(
                               {field("a", arrow::int64()), field("b", arrow::boolean())}))),
        arrow::field("f1", arrow::map(arrow::struct_({field("a", arrow::int64()),
                                                      field("b", arrow::boolean())}),
                                      arrow::boolean()))};
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [[null, [1, true], null], [[[1, true], true]]],
        [[[2, false], null], null],
        [[[2, false], [3, true], [4, null]], [[[1, true], true], [[5, false], null]]],
        [null, null]
    ])")
                         .ValueOrDie();
    arrow::FieldVector target_fields = {
        arrow::field("f0", arrow::list(arrow::struct_({field("a", arrow::int64()),
                                                       field("b", arrow::boolean()),
                                                       field("c", arrow::int64())}))),
        arrow::field("f1", arrow::map(arrow::struct_({field("a", arrow::int64()),
                                                      field("b", arrow::boolean()),
                                                      field("c", arrow::int64())}),
                                      arrow::boolean()))};
    auto target_arrow_type = arrow::struct_(target_fields);
    auto arrow_pool = GetArrowPool(GetDefaultPool());
    ASSERT_OK_AND_ASSIGN(auto ret, ManifestMetaReader::AlignArrayWithSchema(
                                       src_array, target_arrow_type, arrow_pool.get()));

    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(target_fields), R"([
        [[null, [1, true, null], null], [[[1, true, null], true]]],
        [[[2, false, null], null], null],
        [[[2, false, null], [3, true, null], [4, null, null]], [[[1, true, null], true], [[5, false, null], null]]],
        [null, null]
    ])")
            .ValueOrDie();
    ASSERT_TRUE(expected_array->Equals(ret)) << ret->ToString();
}

TEST(ManifestMetaReaderTest, TestCastInt32Type) {
    arrow::FieldVector src_fields = {
        arrow::field("f0", arrow::list(arrow::int32())),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::int32())),
        arrow::field("f2",
                     arrow::struct_({field("sub1", arrow::int32()), field("sub2", arrow::float64()),
                                     field("sub3", arrow::boolean())})),
        arrow::field("f3", arrow::int32()),
    };
    std::string array_json = R"([
        [[1, 2, 3],    [["apple", 3], ["banana", 4]],          [10, 10.1, false],  10],
        [[4, 5],       [["cat", 5], ["dog", 6], ["mouse", 7]], [20, 20.1, true],   20],
        [[6],          [["elephant", 7], ["fox", 8]],          [null, 30.1, true], 30],
        [[7],          [["giraffe", 9]],                       [null, 40.1, true], 40],
        [null,         [["horse", 10], ["Panda", 11]],         [50, 50.1, null],   50],
        [[9],          null,                                   [60, 60.1, false],  60],
        [[10, 11, 12], [["rabbit", null], ["tiger", 13]],      null,               70]
    ])";
    auto src_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(src_fields), array_json)
            .ValueOrDie();

    arrow::FieldVector target_fields = {
        arrow::field("f0", arrow::list(arrow::int8())),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::int16())),
        arrow::field("f2",
                     arrow::struct_({field("sub1", arrow::int8()), field("sub2", arrow::float64()),
                                     field("sub3", arrow::boolean())})),
        arrow::field("f3", arrow::int16()),
    };
    auto target_arrow_type = arrow::struct_(target_fields);
    auto target_array =
        arrow::ipc::internal::json::ArrayFromJSON(target_arrow_type, array_json).ValueOrDie();
    auto arrow_pool = GetArrowPool(GetDefaultPool());
    ASSERT_OK_AND_ASSIGN(auto result_array, ManifestMetaReader::AlignArrayWithSchema(
                                                src_array, target_arrow_type, arrow_pool.get()));
    ASSERT_TRUE(target_array->Equals(result_array)) << result_array->ToString();
}

}  // namespace paimon::test
