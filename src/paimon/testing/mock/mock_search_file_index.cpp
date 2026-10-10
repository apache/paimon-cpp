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

#include "paimon/testing/mock/mock_search_file_index.h"

#include <map>

#include "arrow/c/bridge.h"
#include "paimon/common/io/byte_array_output_stream.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/io/offset_input_stream.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/factories/factory.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/file_index/scored_file_index_result.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/vector_search.h"

namespace paimon::test {
MockSearchFileIndexReader::MockSearchFileIndexReader(std::string field_name,
                                                     SearchIndexType index_type,
                                                     MockSearchFileIndex::ScoredRows rows)
    : field_name_(std::move(field_name)), index_type_(index_type), rows_(std::move(rows)) {}

Result<std::shared_ptr<ScoredFileIndexResult>> MockSearchFileIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& search) {
    if (index_type_ != SearchIndexType::VECTOR) {
        return FileIndexReader::VisitVectorSearch(search);
    }
    if (search->field_name != field_name_) {
        return Status::Invalid("Mock vector search field does not match the indexed field");
    }
    std::map<int32_t, float> selected;
    for (const auto& [row_id, score] : rows_) {
        if (selected.size() >= static_cast<size_t>(search->limit)) {
            break;
        }
        if (!search->pre_filter || search->pre_filter(row_id)) {
            selected.emplace(row_id, score);
        }
    }
    RoaringBitmap32 positions;
    std::vector<float> scores;
    for (const auto& [row_id, score] : selected) {
        positions.Add(row_id);
        scores.push_back(score);
    }
    return ScoredFileIndexResult::Create(std::move(positions), std::move(scores));
}

Result<std::shared_ptr<FileIndexResult>> MockSearchFileIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& search) {
    if (index_type_ != SearchIndexType::FULL_TEXT) {
        return FileIndexReader::VisitFullTextSearch(search);
    }
    if (search->field_name != field_name_) {
        return Status::Invalid("Mock full-text search field does not match the indexed field");
    }
    RoaringBitmap32 positions;
    for (const auto& [row_id, score] : rows_) {
        if (search->limit && positions.Cardinality() >= search->limit.value()) {
            break;
        }
        if (!search->pre_filter || search->pre_filter->Contains(row_id)) {
            positions.Add(row_id);
        }
    }
    return std::make_shared<BitmapIndexResult>(
        [bitmap = std::move(positions)]() -> Result<RoaringBitmap32> { return bitmap; });
}

MockSearchFileIndexer::MockSearchFileIndexer(SearchIndexType index_type)
    : index_type_(index_type) {}

Result<std::shared_ptr<FileIndexReader>> MockSearchFileIndexer::CreateReader(
    ::ArrowSchema* c_schema, int32_t start, int32_t length,
    const std::shared_ptr<InputStream>& input, const std::shared_ptr<MemoryPool>&) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                      arrow::ImportSchema(c_schema));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OffsetInputStream> payload,
                           OffsetInputStream::Create(input, length, start));
    DataInputStream data_input(payload);
    PAIMON_ASSIGN_OR_RAISE(int32_t count, data_input.ReadValue<int32_t>());
    MockSearchFileIndex::ScoredRows rows;
    for (int32_t i = 0; i < count; ++i) {
        PAIMON_ASSIGN_OR_RAISE(int32_t row_id, data_input.ReadValue<int32_t>());
        PAIMON_ASSIGN_OR_RAISE(float score, data_input.ReadValue<float>());
        rows.emplace_back(row_id, score);
    }
    return std::make_shared<MockSearchFileIndexReader>(schema->field(0)->name(), index_type_,
                                                       std::move(rows));
}

Result<std::shared_ptr<FileIndexWriter>> MockSearchFileIndexer::CreateWriter(
    ::ArrowSchema*, const std::shared_ptr<MemoryPool>&) const {
    return Status::NotImplemented("Mock search file index is read-only");
}

const char* MockVectorSearchFileIndexFactory::Identifier() const {
    return MockSearchFileIndex::VECTOR_IDENTIFIER;
}

Result<std::unique_ptr<FileIndexer>> MockVectorSearchFileIndexFactory::Create(
    const std::map<std::string, std::string>&) const {
    return std::make_unique<MockSearchFileIndexer>(SearchIndexType::VECTOR);
}

const char* MockFullTextSearchFileIndexFactory::Identifier() const {
    return MockSearchFileIndex::FULL_TEXT_IDENTIFIER;
}

Result<std::unique_ptr<FileIndexer>> MockFullTextSearchFileIndexFactory::Create(
    const std::map<std::string, std::string>&) const {
    return std::make_unique<MockSearchFileIndexer>(SearchIndexType::FULL_TEXT);
}

REGISTER_PAIMON_FACTORY(MockVectorSearchFileIndexFactory);
REGISTER_PAIMON_FACTORY(MockFullTextSearchFileIndexFactory);

namespace {

std::shared_ptr<ByteArrayOutputStream> MakeOutput(const std::shared_ptr<MemoryPool>& pool) {
    auto segments = std::make_unique<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    return std::make_shared<ByteArrayOutputStream>(std::move(segments));
}

}  // namespace

const char MockSearchFileIndex::VECTOR_IDENTIFIER[] = "mock-vector-search";
const char MockSearchFileIndex::FULL_TEXT_IDENTIFIER[] = "mock-full-text-search";

Result<std::shared_ptr<Bytes>> MockSearchFileIndex::MakePayload(
    const ScoredRows& rows, const std::shared_ptr<MemoryPool>& pool) {
    auto output = MakeOutput(pool);
    DataOutputStream data_output(output);
    PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(static_cast<int32_t>(rows.size())));
    for (const auto& [row_id, score] : rows) {
        PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(row_id));
        PAIMON_RETURN_NOT_OK(data_output.WriteValue<float>(score));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> payload, output->Finish(pool.get()));
    return payload;
}

Result<std::shared_ptr<Bytes>> MockSearchFileIndex::Serialize(
    const FileIndexFormat::ColumnIndexes& indexes, const std::shared_ptr<MemoryPool>& pool) {
    auto output = MakeOutput(pool);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexFormat::Writer> writer,
                           FileIndexFormat::CreateWriter(output, pool));
    PAIMON_RETURN_NOT_OK(writer->WriteColumnIndexes(indexes));
    PAIMON_RETURN_NOT_OK(writer->Close());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, output->Finish(pool.get()));
    return bytes;
}

Result<std::shared_ptr<Bytes>> MockSearchFileIndex::MakeIndex(
    const std::string& field_name, const std::string& index_type, const ScoredRows& rows,
    const std::shared_ptr<MemoryPool>& pool) {
    FileIndexFormat::ColumnIndexes indexes;
    PAIMON_ASSIGN_OR_RAISE(indexes[field_name][index_type], MakePayload(rows, pool));
    return Serialize(indexes, pool);
}

}  // namespace paimon::test
