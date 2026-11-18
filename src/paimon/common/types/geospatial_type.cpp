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

#include "paimon/common/types/geospatial_type.h"

#include <algorithm>
#include <cstdint>
#include <unordered_map>

#include "arrow/util/key_value_metadata.h"
#include "fmt/format.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/map_type.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/string_utils.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "re2/re2.h"

namespace paimon {
namespace {

bool CrsEqualsIgnoreCase(const std::string& left, const std::string& right) {
    if (StringUtils::EqualsIgnoreCase(left, right)) {
        return true;
    }
    auto ascii = [](uint8_t c) { return c < 128; };
    if (std::all_of(left.begin(), left.end(), ascii) &&
        std::all_of(right.begin(), right.end(), ascii)) {
        return false;
    }
    // Java's equalsIgnoreCase groups dotted and dotless I with ASCII I. RE2's
    // Unicode simple case folding handles the remaining characters.
    auto normalize_i = [](const std::string& crs) {
        return StringUtils::Replace(StringUtils::Replace(crs, u8"İ", "i"), u8"ı", "i");
    };
    re2::RE2::Options options;
    options.set_literal(true);
    options.set_case_sensitive(false);
    return re2::RE2::FullMatch(normalize_i(right), re2::RE2(normalize_i(left), options));
}

const std::string* FieldId(const arrow::Field& field) {
    const auto& metadata = field.metadata();
    int32_t index = metadata ? metadata->FindKey(DataField::FIELD_ID) : -1;
    return index < 0 ? nullptr : &metadata->value(index);
}

Result<std::string> LogicalTypeName(const arrow::Field& field) {
    if (GeospatialType::IsGeospatial(field)) {
        PAIMON_ASSIGN_OR_RAISE(GeospatialType type, GeospatialType::FromField(field));
        return type.ToString();
    }
    return field.type()->id() == arrow::Type::BINARY ? "BYTES" : field.type()->ToString();
}

}  // namespace

bool GeospatialType::IsGeospatial(const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) {
    int32_t index = metadata ? metadata->FindKey(kExtensionNameKey) : -1;
    return index >= 0 && metadata->value(index) == kExtensionName;
}

bool GeospatialType::IsGeospatial(const arrow::Field& field) {
    return IsGeospatial(field.metadata());
}

bool GeospatialType::ContainsGeospatial(const arrow::Field& field) {
    return IsGeospatial(field) || ContainsGeospatial(field.type()->fields());
}

bool GeospatialType::ContainsGeospatial(const arrow::FieldVector& fields) {
    return std::any_of(fields.begin(), fields.end(),
                       [](const auto& field) { return ContainsGeospatial(*field); });
}

Result<GeospatialType> GeospatialType::Create(const std::string& crs, const std::string& edges) {
    if (crs.empty()) {
        return Status::Invalid("Invalid CRS: empty string");
    }
    std::string normalized = StringUtils::ToLowerCase(edges);
    if (!normalized.empty() && normalized != "spherical" && normalized != "vincenty" &&
        normalized != "thomas" && normalized != "andoyer" && normalized != "karney") {
        return Status::Invalid("Invalid edge interpolation algorithm: ", edges);
    }
    return GeospatialType(crs, normalized);
}

Status GeospatialType::ValidateFileFormat(const arrow::Schema& schema, const std::string& format) {
    if (!StringUtils::EqualsIgnoreCase(format, "parquet") && ContainsGeospatial(schema.fields())) {
        return Status::Invalid("GEOMETRY/GEOGRAPHY only supports parquet data files: ", format);
    }
    return Status::OK();
}

Result<GeospatialType> GeospatialType::FromField(const arrow::Field& field) {
    if (!IsGeospatial(field) || field.type()->id() != arrow::Type::BINARY) {
        return Status::Invalid("geoarrow.wkb requires a Binary field: ", field.name());
    }
    std::string crs = kDefaultCrs;
    std::string edges;
    if (field.metadata()->Contains(kExtensionMetadataKey)) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::string json,
                                          field.metadata()->Get(kExtensionMetadataKey));
        rapidjson::Document doc;
        doc.Parse(json.data(), json.size());
        if (doc.HasParseError() || !doc.IsObject()) {
            return Status::Invalid("Invalid geoarrow.wkb metadata for field ", field.name());
        }
        if (doc.HasMember("crs")) {
            if (!doc["crs"].IsString()) {
                return Status::Invalid("geoarrow.wkb CRS must be a string");
            }
            crs.assign(doc["crs"].GetString(), doc["crs"].GetStringLength());
        }
        if (doc.HasMember("edges")) {
            if (!doc["edges"].IsString() || doc["edges"].GetStringLength() == 0) {
                return Status::Invalid("Invalid edge interpolation algorithm in geoarrow.wkb");
            }
            edges.assign(doc["edges"].GetString(), doc["edges"].GetStringLength());
            if (edges == "planar") {
                edges.clear();
            }
        }
    }
    return Create(crs, edges);
}

Result<std::shared_ptr<arrow::Field>> GeospatialType::GeometryField(const std::string& name,
                                                                    const std::string& crs,
                                                                    bool nullable) {
    return MakeField(name, crs, "", nullable);
}

Result<std::shared_ptr<arrow::Field>> GeospatialType::GeographyField(const std::string& name,
                                                                     const std::string& crs,
                                                                     const std::string& edges,
                                                                     bool nullable) {
    if (edges.empty()) {
        return Status::Invalid("Invalid edge interpolation algorithm: empty string");
    }
    return MakeField(name, crs, edges, nullable);
}

Result<std::shared_ptr<arrow::Field>> GeospatialType::MakeField(const std::string& name,
                                                                const std::string& crs,
                                                                const std::string& edges,
                                                                bool nullable) {
    PAIMON_ASSIGN_OR_RAISE(GeospatialType type, Create(crs, edges));
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    if (!type.edges_.empty()) {
        writer.Key("edges");
        writer.String(type.edges_.data(), static_cast<rapidjson::SizeType>(type.edges_.size()));
    }
    writer.Key("crs");
    writer.String(type.crs_.data(), static_cast<rapidjson::SizeType>(type.crs_.size()));
    writer.EndObject();
    return arrow::field(name, arrow::binary(), nullable,
                        arrow::KeyValueMetadata::Make({kExtensionNameKey, kExtensionMetadataKey},
                                                      {kExtensionName, buffer.GetString()}));
}

bool GeospatialType::Equals(const arrow::Field& left, const arrow::Field& right) {
    bool left_geo = IsGeospatial(left);
    bool right_geo = IsGeospatial(right);
    if (left_geo || right_geo) {
        if (!left_geo || !right_geo) {
            return false;
        }
        auto lhs = FromField(left);
        auto rhs = FromField(right);
        return lhs.ok() && rhs.ok() && lhs.value().Edges() == rhs.value().Edges() &&
               CrsEqualsIgnoreCase(lhs.value().Crs(), rhs.value().Crs());
    }
    if (left.type()->id() == arrow::Type::MAP && right.type()->id() == arrow::Type::MAP &&
        MapType::HasNullableKey(left.metadata()) != MapType::HasNullableKey(right.metadata())) {
        return false;
    }
    if (left.type()->num_fields() != right.type()->num_fields()) {
        return !ContainsGeospatial(left) && !ContainsGeospatial(right);
    }
    for (int32_t i = 0; i < left.type()->num_fields(); ++i) {
        if (!Equals(*left.type()->field(i), *right.type()->field(i))) {
            return false;
        }
    }
    return true;
}

Status GeospatialType::ValidateEvolution(const arrow::Field& source, const arrow::Field& target) {
    if (IsGeospatial(source) || IsGeospatial(target)) {
        if (Equals(source, target)) {
            return Status::OK();
        }
        PAIMON_ASSIGN_OR_RAISE(std::string source_type, LogicalTypeName(source));
        PAIMON_ASSIGN_OR_RAISE(std::string target_type, LogicalTypeName(target));
        return Status::Invalid("Incompatible geospatial type change for field ", target.name(),
                               ": ", source_type, " -> ", target_type);
    }
    if (!ContainsGeospatial(source) && !ContainsGeospatial(target)) {
        return Status::OK();
    }
    if (source.type()->id() != target.type()->id()) {
        return Status::Invalid("Incompatible geospatial type change for field ", target.name(),
                               ": ", source.type()->ToString(), " -> ", target.type()->ToString());
    }
    if (source.type()->id() == arrow::Type::STRUCT) {
        const auto& row = checked_cast<const arrow::StructType&>(*source.type());
        std::unordered_map<std::string, const arrow::Field*> fields_by_id;
        for (const auto& field : row.fields()) {
            if (const auto* id = FieldId(*field)) {
                fields_by_id.emplace(*id, field.get());
            }
        }
        for (const auto& target_child : target.type()->fields()) {
            const arrow::Field* source_child = row.GetFieldByName(target_child->name()).get();
            if (const auto* id = FieldId(*target_child)) {
                auto it = fields_by_id.find(*id);
                if (it != fields_by_id.end()) {
                    source_child = it->second;
                } else if (source_child && FieldId(*source_child)) {
                    // A new field reuses the old name with a different ID.
                    continue;
                }
            }
            if (source_child) {
                PAIMON_RETURN_NOT_OK(ValidateEvolution(*source_child, *target_child));
            }
        }
    } else if (source.type()->id() == arrow::Type::MAP) {
        const auto& source_map = checked_cast<const arrow::MapType&>(*source.type());
        const auto& target_map = checked_cast<const arrow::MapType&>(*target.type());
        PAIMON_RETURN_NOT_OK(ValidateEvolution(*source_map.key_field(), *target_map.key_field()));
        return ValidateEvolution(*source_map.item_field(), *target_map.item_field());
    } else {
        for (int32_t i = 0; i < source.type()->num_fields(); ++i) {
            PAIMON_RETURN_NOT_OK(
                ValidateEvolution(*source.type()->field(i), *target.type()->field(i)));
        }
    }
    return Status::OK();
}

std::string GeospatialType::ToString() const {
    // Match GeometryType.formatCrs, including Java's non-breaking space exceptions.
    static const re2::RE2 delimiters(R"([<>(),.\x27\x60\x09-\x0d\x1c-\x20\x{1680}\x{2000}-\x{2006})"
                                     R"(\x{2008}-\x{200a}\x{2028}\x{2029}\x{205f}\x{3000}])");
    static const re2::RE2 leading_digit(R"(^\p{Nd})");
    // Java checks the first UTF-16 code unit, which excludes supplementary digits.
    bool quote =
        re2::RE2::PartialMatch(crs_, delimiters) ||
        (static_cast<uint8_t>(crs_.front()) < 0xf0 && re2::RE2::PartialMatch(crs_, leading_digit));
    std::string crs = quote ? fmt::format("'{}'", StringUtils::Replace(crs_, "'", "''")) : crs_;
    return edges_.empty() ? fmt::format("GEOMETRY({})", crs)
                          : fmt::format("GEOGRAPHY({}, {})", crs, edges_);
}

}  // namespace paimon
