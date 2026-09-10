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

#include "paimon/format/parquet/parquet_read_type_adapter.h"

#include <memory>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/casting/timestamp_to_timestamp_cast_executor.h"

namespace paimon::parquet {

bool ParquetReadTypeAdapter::IsCompatibleBlob(const std::shared_ptr<arrow::Field>& src_field,
                                              const std::shared_ptr<arrow::Field>& target_field) {
    // ARROW:schema may restore an inline BLOB's physical binary column to its logical
    // large_binary representation before this compatibility check runs.
    return src_field->type()->id() == arrow::Type::LARGE_BINARY &&
           target_field->type()->id() == arrow::Type::BINARY &&
           BlobUtils::IsBlobMetadata(target_field->metadata());
}

Result<std::shared_ptr<arrow::DataType>> ParquetReadTypeAdapter::NormalizeFileType(
    const std::shared_ptr<arrow::DataType>& src_data_type) {
    arrow::Type::type type = src_data_type->id();
    switch (type) {
        case arrow::Type::type::STRUCT: {
            auto* src_struct_type = checked_cast<arrow::StructType*>(src_data_type.get());
            arrow::FieldVector new_fields;
            new_fields.reserve(src_struct_type->num_fields());
            for (int32_t i = 0; i < src_struct_type->num_fields(); ++i) {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> sub_type,
                                       NormalizeFileType(src_struct_type->field(i)->type()));
                new_fields.push_back(src_struct_type->field(i)->WithType(sub_type));
            }
            return arrow::struct_(new_fields);
        }
        case arrow::Type::type::MAP: {
            auto* src_map_type = checked_cast<arrow::MapType*>(src_data_type.get());
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> key_type,
                                   NormalizeFileType(src_map_type->key_type()));
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> item_type,
                                   NormalizeFileType(src_map_type->item_type()));
            return std::make_shared<arrow::MapType>(
                src_map_type->key_field()->WithType(key_type),
                src_map_type->item_field()->WithType(item_type));
        }
        case arrow::Type::type::LIST: {
            auto* src_list_type = checked_cast<arrow::ListType*>(src_data_type.get());
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> value_type,
                                   NormalizeFileType(src_list_type->value_type()));
            return arrow::list(src_list_type->value_field()->WithType(value_type));
        }
        case arrow::Type::type::TIMESTAMP: {
            auto* src_ts_type = checked_cast<arrow::TimestampType*>(src_data_type.get());
            if (!src_ts_type->timezone().empty()) {
                return arrow::timestamp(src_ts_type->unit(), DateTimeUtils::GetLocalTimezoneName());
            }
        }
        default:
            return src_data_type;
    }
}

Result<bool> ParquetReadTypeAdapter::NeedsArrayConversion(
    const std::shared_ptr<arrow::DataType>& src_data_type,
    const std::shared_ptr<arrow::DataType>& target_data_type) {
    return NeedsArrayConversionImpl(arrow::field("", src_data_type),
                                    arrow::field("", target_data_type));
}

Result<bool> ParquetReadTypeAdapter::NeedsArrayConversionImpl(
    const std::shared_ptr<arrow::Field>& src_field,
    const std::shared_ptr<arrow::Field>& target_field) {
    if (IsCompatibleBlob(src_field, target_field)) {
        return false;
    }

    const std::shared_ptr<arrow::DataType>& src_data_type = src_field->type();
    const std::shared_ptr<arrow::DataType>& target_data_type = target_field->type();
    arrow::Type::type type = src_data_type->id();
    if (type != target_data_type->id()) {
        return Status::Invalid(fmt::format("src type {} and target type {} mismatch",
                                           src_data_type->ToString(),
                                           target_data_type->ToString()));
    }
    switch (type) {
        case arrow::Type::type::STRUCT: {
            auto* src_struct_type = checked_cast<arrow::StructType*>(src_data_type.get());
            auto* target_struct_type = checked_cast<arrow::StructType*>(target_data_type.get());
            if (src_struct_type->num_fields() != target_struct_type->num_fields()) {
                return Status::Invalid(
                    fmt::format("source type {} and target type {} number of fields mismatch",
                                src_data_type->ToString(), target_data_type->ToString()));
            }
            for (int32_t i = 0; i < src_struct_type->num_fields(); ++i) {
                PAIMON_ASSIGN_OR_RAISE(bool need_cast,
                                       NeedsArrayConversionImpl(src_struct_type->field(i),
                                                                target_struct_type->field(i)));
                if (need_cast) {
                    return true;
                }
            }
            return false;
        }
        case arrow::Type::type::MAP: {
            auto* src_map_type = checked_cast<arrow::MapType*>(src_data_type.get());
            auto* target_map_type = checked_cast<arrow::MapType*>(target_data_type.get());
            PAIMON_ASSIGN_OR_RAISE(
                bool need_cast,
                NeedsArrayConversionImpl(src_map_type->key_field(), target_map_type->key_field()));
            if (need_cast) {
                return true;
            }
            PAIMON_ASSIGN_OR_RAISE(need_cast,
                                   NeedsArrayConversionImpl(src_map_type->item_field(),
                                                            target_map_type->item_field()));
            return need_cast;
        }
        case arrow::Type::type::LIST: {
            auto* src_list_type = checked_cast<arrow::ListType*>(src_data_type.get());
            auto* target_list_type = checked_cast<arrow::ListType*>(target_data_type.get());
            return NeedsArrayConversionImpl(src_list_type->value_field(),
                                            target_list_type->value_field());
        }
        case arrow::Type::type::TIMESTAMP: {
            auto* src_ts_type = checked_cast<arrow::TimestampType*>(src_data_type.get());
            auto* target_ts_type = checked_cast<arrow::TimestampType*>(target_data_type.get());
            return src_ts_type->unit() != target_ts_type->unit() ||
                   src_ts_type->timezone() != target_ts_type->timezone();
        }
        default:
            return false;
    }
}

Result<std::shared_ptr<arrow::Array>> ParquetReadTypeAdapter::AdaptArray(
    const std::shared_ptr<arrow::Array>& array,
    const std::shared_ptr<arrow::DataType>& target_data_type,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    PAIMON_ASSIGN_OR_RAISE(bool need_cast, NeedsArrayConversion(array->type(), target_data_type));
    if (!need_cast) {
        return array;
    }

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> result,
                           AdaptArrayImpl(array, target_data_type, arrow_pool));
    PAIMON_ASSIGN_OR_RAISE(need_cast, NeedsArrayConversion(result->type(), target_data_type));
    if (need_cast) {
        return Status::Invalid(fmt::format(
            "unexpected: in parquet, after AdaptArray, output type {} does not match target "
            "type {}",
            result->type()->ToString(), target_data_type->ToString()));
    }
    return result;
}

Result<std::shared_ptr<arrow::Array>> ParquetReadTypeAdapter::AdaptArrayImpl(
    const std::shared_ptr<arrow::Array>& array,
    const std::shared_ptr<arrow::DataType>& target_data_type,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    arrow::Type::type type = array->type()->id();
    switch (type) {
        case arrow::Type::type::STRUCT: {
            auto* struct_array = checked_cast<arrow::StructArray*>(array.get());
            arrow::ArrayVector target_sub_arrays;
            std::vector<std::string> target_names;
            target_sub_arrays.reserve(struct_array->num_fields());
            target_names.reserve(struct_array->num_fields());
            for (int32_t i = 0; i < struct_array->num_fields(); i++) {
                const auto& field = struct_array->field(i);
                PAIMON_ASSIGN_OR_RAISE(
                    std::shared_ptr<arrow::Array> sub_array,
                    AdaptArrayImpl(field, target_data_type->field(i)->type(), arrow_pool));
                target_sub_arrays.push_back(sub_array);
                target_names.push_back(target_data_type->field(i)->name());
            }
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> new_array,
                arrow::StructArray::Make(target_sub_arrays, target_names,
                                         struct_array->null_bitmap(), struct_array->null_count(),
                                         struct_array->offset()));
            return new_array;
        }
        case arrow::Type::type::MAP: {
            auto* map_array = checked_cast<arrow::MapArray*>(array.get());
            auto* map_type = checked_cast<arrow::MapType*>(target_data_type.get());
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::Array> key_array,
                AdaptArrayImpl(map_array->keys(), map_type->key_type(), arrow_pool));
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::Array> item_array,
                AdaptArrayImpl(map_array->items(), map_type->item_type(), arrow_pool));
            return std::make_shared<arrow::MapArray>(
                arrow::map(key_array->type(), item_array->type()), map_array->length(),
                map_array->value_offsets(), key_array, item_array, map_array->null_bitmap(),
                map_array->null_count(), map_array->offset());
        }
        case arrow::Type::type::LIST: {
            auto* list_array = checked_cast<arrow::ListArray*>(array.get());
            auto* list_type = checked_cast<arrow::ListType*>(target_data_type.get());
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::Array> value_array,
                AdaptArrayImpl(list_array->values(), list_type->value_type(), arrow_pool));
            return std::make_shared<arrow::ListArray>(
                arrow::list(value_array->type()), list_array->length(), list_array->value_offsets(),
                value_array, list_array->null_bitmap(), list_array->null_count(),
                list_array->offset());
        }
        case arrow::Type::type::TIMESTAMP: {
            auto* ts_array = checked_cast<arrow::TimestampArray*>(array.get());
            auto* src_type = checked_cast<arrow::TimestampType*>(ts_array->type().get());
            auto* ts_target_type = checked_cast<arrow::TimestampType*>(target_data_type.get());
            if (src_type->unit() == arrow::TimeUnit::type::MILLI &&
                ts_target_type->unit() == arrow::TimeUnit::type::SECOND) {
                // Parquet does not support second-precision timestamps, so the writer stores
                // them as milliseconds. Convert them back to seconds for the Paimon read type.
                auto cast_executor = std::make_shared<TimestampToTimestampCastExecutor>();
                PAIMON_ASSIGN_OR_RAISE(
                    std::shared_ptr<arrow::Array> target_array,
                    cast_executor->Cast(array, target_data_type, arrow_pool.get()));
                return target_array;
            }
            if (src_type->timezone() != ts_target_type->timezone()) {
                // INT96 carries no timezone, while other Parquet timestamps may be exposed as
                // UTC. Attach the timezone required by the Paimon read type without changing
                // the underlying values.
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> target_array,
                                                  ts_array->View(target_data_type));
                return target_array;
            }
        }
        default:
            return array;
    }
}

}  // namespace paimon::parquet
