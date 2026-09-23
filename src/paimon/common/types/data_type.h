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

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "paimon/common/utils/jsonizable.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace arrow {
class DataType;
class Field;
class TimestampType;
class KeyValueMetadata;
}  // namespace arrow

namespace paimon {

class DataType : public Jsonizable<DataType> {
 public:
    static constexpr char kTimePrecision[] = "paimon.time.precision";

    // Arrow carries milliseconds, while metadata preserves the declared TIME precision.
    // Arrow-only schemas use Paimon's default precision of zero.
    static Result<int32_t> GetTimePrecision(const arrow::Field& field);

    static std::unique_ptr<DataType> Create(
        const std::shared_ptr<arrow::DataType>& type, bool nullable,
        const std::shared_ptr<const arrow::KeyValueMetadata>& metadata);

    ~DataType() override = default;

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;
    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

    std::string WithNullable(const std::string& type) const;

 protected:
    DataType(const std::shared_ptr<arrow::DataType>& type, bool nullable,
             const std::shared_ptr<const arrow::KeyValueMetadata>& metadata);

    std::shared_ptr<arrow::DataType> type_;
    bool nullable_;
    std::shared_ptr<const arrow::KeyValueMetadata> metadata_;

 private:
    static Result<int32_t> GetTimePrecision(
        const std::shared_ptr<arrow::DataType>& type,
        const std::shared_ptr<const arrow::KeyValueMetadata>& metadata);

    std::string TimestampToString(const std::shared_ptr<arrow::TimestampType>& type) const;
    std::string DataTypeToString(const std::shared_ptr<arrow::DataType>& type) const;
};

}  // namespace paimon
