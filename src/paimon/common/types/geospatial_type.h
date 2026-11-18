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

#include <memory>
#include <string>

#include "arrow/type.h"
#include "paimon/defs.h"
#include "paimon/result.h"

namespace paimon {

/// Paimon GEOMETRY or GEOGRAPHY carried by an Arrow Binary field with GeoArrow WKB metadata.
class PAIMON_EXPORT GeospatialType {
 public:
    static constexpr char kExtensionNameKey[] = "ARROW:extension:name";
    static constexpr char kExtensionMetadataKey[] = "ARROW:extension:metadata";
    static constexpr char kExtensionName[] = "geoarrow.wkb";
    static constexpr char kDefaultCrs[] = "OGC:CRS84";
    static constexpr char kDefaultEdges[] = "spherical";

    static bool IsGeospatial(const std::shared_ptr<const arrow::KeyValueMetadata>& metadata);
    static bool IsGeospatial(const arrow::Field& field);
    static bool ContainsGeospatial(const arrow::Field& field);
    static bool ContainsGeospatial(const arrow::FieldVector& fields);
    static Status ValidateFileFormat(const arrow::Schema& schema, const std::string& format);
    static Result<GeospatialType> FromField(const arrow::Field& field);
    static Result<std::shared_ptr<arrow::Field>> GeometryField(const std::string& name,
                                                               const std::string& crs = kDefaultCrs,
                                                               bool nullable = true);
    static Result<std::shared_ptr<arrow::Field>> GeographyField(
        const std::string& name, const std::string& crs = kDefaultCrs,
        const std::string& edges = kDefaultEdges, bool nullable = true);

    /// Compares geospatial attributes of corresponding fields, including logical MAP key
    /// nullability.
    static bool Equals(const arrow::Field& left, const arrow::Field& right);
    /// Matches projected ROW fields by ID, falling back to names when IDs are unavailable.
    static Status ValidateEvolution(const arrow::Field& source, const arrow::Field& target);

    FieldType Type() const {
        return edges_.empty() ? FieldType::GEOMETRY : FieldType::GEOGRAPHY;
    }
    const std::string& Crs() const {
        return crs_;
    }
    const std::string& Edges() const {
        return edges_;
    }
    std::string ToString() const;

 private:
    GeospatialType(const std::string& crs, const std::string& edges) : crs_(crs), edges_(edges) {}
    static Result<GeospatialType> Create(const std::string& crs, const std::string& edges);
    static Result<std::shared_ptr<arrow::Field>> MakeField(const std::string& name,
                                                           const std::string& crs,
                                                           const std::string& edges, bool nullable);

    std::string crs_;
    std::string edges_;
};

}  // namespace paimon
