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

#include "paimon/common/file_index/bitmap/bitmap_file_index_meta.h"

#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/defs.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"

namespace paimon {
class MemoryPool;

BitmapFileIndexMeta::Entry::Entry(const Literal& key, int32_t offset, int32_t length)
    : key(key), offset(offset), length(length) {}

BitmapFileIndexMeta::BitmapFileIndexMeta(const FieldType& type, int32_t total_length,
                                         const std::shared_ptr<MemoryPool>& pool)
    : data_type_(type),
      total_length_(total_length),
      null_value_entry_(Literal(type), -1, -1),
      pool_(pool) {}

BitmapFileIndexMeta::BitmapFileIndexMeta(const FieldType& type, int32_t row_count,
                                         bool has_null_value, const Entry& null_value_entry,
                                         std::vector<Entry>&& write_entries,
                                         const std::shared_ptr<MemoryPool>& pool)
    : data_type_(type),
      row_count_(row_count),
      has_null_value_(has_null_value),
      null_value_entry_(null_value_entry),
      write_entries_(std::move(write_entries)),
      pool_(pool) {}

Result<std::function<void(const Literal&)>> BitmapFileIndexMeta::GetValueWriter(
    const std::shared_ptr<MemorySegmentOutputStream>& output_stream) const {
    switch (data_type_) {
        case FieldType::BOOLEAN:
            return std::function<void(const Literal&)>(
                [output_stream](const Literal& literal) -> void {
                    output_stream->WriteValue<bool>(literal.GetValue<bool>());
                });
        case FieldType::TINYINT:
            return std::function<void(const Literal&)>(
                [output_stream](const Literal& literal) -> void {
                    output_stream->WriteValue<int8_t>(literal.GetValue<int8_t>());
                });
        case FieldType::SMALLINT:
            return std::function<void(const Literal&)>(
                [output_stream](const Literal& literal) -> void {
                    output_stream->WriteValue<int16_t>(literal.GetValue<int16_t>());
                });
        case FieldType::DATE:
        case FieldType::INT:
            return std::function<void(const Literal&)>(
                [output_stream](const Literal& literal) -> void {
                    output_stream->WriteValue<int32_t>(literal.GetValue<int32_t>());
                });
        case FieldType::BIGINT:
            return std::function<void(const Literal&)>(
                [output_stream](const Literal& literal) -> void {
                    output_stream->WriteValue<int64_t>(literal.GetValue<int64_t>());
                });
        case FieldType::STRING:
            return std::function<void(const Literal&)>(
                [output_stream](const Literal& literal) -> void {
                    auto value = literal.GetValue<std::string>();
                    output_stream->WriteValue<uint32_t>(value.size());
                    output_stream->Write(value.data(), value.size());
                });
        default:
            return Status::Invalid(fmt::format("invalid index field type {}",
                                               FieldTypeUtils::FieldTypeToString(data_type_)));
    }
}

Result<std::function<Result<Literal>()>> BitmapFileIndexMeta::GetValueReader(
    const std::shared_ptr<DataInputStream>& in, bool move_body_start) {
    const auto& field_type = data_type_;
    switch (field_type) {
        case FieldType::STRING: {
            std::function<Result<Literal>()> func = [&in, move_body_start, field_type,
                                                     this]() -> Result<Literal> {
                PAIMON_ASSIGN_OR_RAISE(uint32_t length,
                                       ReadAndMoveBodyStart<int32_t>(in, move_body_start));
                if (move_body_start) {
                    body_start_ += length;
                }
                auto bytes = std::make_unique<Bytes>(length, pool_.get());
                PAIMON_RETURN_NOT_OK(in->ReadBytes(bytes.get()));
                return Literal(field_type, bytes->data(), bytes->size());
            };
            return func;
        }
        case FieldType::BOOLEAN: {
            std::function<Result<Literal>()> func = [&in, move_body_start,
                                                     this]() -> Result<Literal> {
                PAIMON_ASSIGN_OR_RAISE(bool value, ReadAndMoveBodyStart<bool>(in, move_body_start));
                return Literal(value);
            };
            return func;
        }
        case FieldType::TINYINT: {
            std::function<Result<Literal>()> func = [&in, move_body_start,
                                                     this]() -> Result<Literal> {
                PAIMON_ASSIGN_OR_RAISE(int8_t value,
                                       ReadAndMoveBodyStart<int8_t>(in, move_body_start));
                return Literal(value);
            };
            return func;
        }
        case FieldType::SMALLINT: {
            std::function<Result<Literal>()> func = [&in, move_body_start,
                                                     this]() -> Result<Literal> {
                PAIMON_ASSIGN_OR_RAISE(int16_t value,
                                       ReadAndMoveBodyStart<int16_t>(in, move_body_start));
                return Literal(value);
            };
            return func;
        }
        case FieldType::INT: {
            std::function<Result<Literal>()> func = [&in, move_body_start,
                                                     this]() -> Result<Literal> {
                PAIMON_ASSIGN_OR_RAISE(int32_t value,
                                       ReadAndMoveBodyStart<int32_t>(in, move_body_start));
                return Literal(value);
            };
            return func;
        }
        case FieldType::BIGINT: {
            std::function<Result<Literal>()> func = [&in, move_body_start,
                                                     this]() -> Result<Literal> {
                PAIMON_ASSIGN_OR_RAISE(int64_t value,
                                       ReadAndMoveBodyStart<int64_t>(in, move_body_start));
                return Literal(value);
            };
            return func;
        }
        case FieldType::DATE: {
            std::function<Result<Literal>()> func = [&in, move_body_start,
                                                     this]() -> Result<Literal> {
                PAIMON_ASSIGN_OR_RAISE(int32_t value,
                                       ReadAndMoveBodyStart<int32_t>(in, move_body_start));
                return Literal(FieldType::DATE, value);
            };
            return func;
        }
        case FieldType::TIMESTAMP: {
            std::function<Result<Literal>()> func = [&in, move_body_start,
                                                     this]() -> Result<Literal> {
                PAIMON_ASSIGN_OR_RAISE(int64_t value,
                                       ReadAndMoveBodyStart<int64_t>(in, move_body_start));
                // convert timestamp to bigint
                return Literal(value);
            };
            return func;
        }
        default:
            return Status::Invalid(fmt::format("not support field type {} in BitmapIndex",
                                               FieldTypeUtils::FieldTypeToString(field_type)));
    }
}

}  // namespace paimon
