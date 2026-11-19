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

#include <string>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/util/key_value_metadata.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/data_type.h"
#include "paimon/common/types/data_type_json_parser.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(GeospatialTypeTest, SchemaRoundTrip) {
    const std::pair<const char*, const char*> cases[] = {
        {"GEOMETRY", "GEOMETRY(OGC:CRS84)"},
        {"GEOMETRY(EPSG:3857) NOT NULL", "GEOMETRY(EPSG:3857) NOT NULL"},
        {"geography", "GEOGRAPHY(OGC:CRS84, spherical)"},
        {"GEOGRAPHY(EPSG:4326)", "GEOGRAPHY(EPSG:4326, spherical)"},
        {"GEOGRAPHY('a custom, ''quoted'' CRS', KARNEY) NOT NULL",
         "GEOGRAPHY('a custom, ''quoted'' CRS', karney) NOT NULL"},
        {"GEOGRAPHY('123', THOMAS)", "GEOGRAPHY('123', thomas)"},
        {"GEOGRAPHY(EPSG:4326, ANDOYER)", "GEOGRAPHY(EPSG:4326, andoyer)"},
        {"GEOGRAPHY(EPSG:4326, VINCENTY)", "GEOGRAPHY(EPSG:4326, vincenty)"},
        {"GEOMETRY('int')", "GEOMETRY(int)"},
        {u8"GEOMETRY('Référence')", u8"GEOMETRY(Référence)"},
        {u8"GEOMETRY('地方\u3000坐标')", u8"GEOMETRY('地方\u3000坐标')"},
        {u8"GEOMETRY('A\u00a0B')", u8"GEOMETRY(A\u00a0B)"},
        {u8"GEOMETRY('\u0661CRS')", u8"GEOMETRY('\u0661CRS')"}};
    for (const auto& [sql, canonical] : cases) {
        SCOPED_TRACE(sql);
        rapidjson::Document input;
        input.SetString(sql, input.GetAllocator());
        ASSERT_OK_AND_ASSIGN(auto field, DataTypeJsonParser::ParseType("geo", input));
        ASSERT_EQ(field->type()->id(), arrow::Type::BINARY);
        auto type = DataType::Create(field->type(), field->nullable(), field->metadata());
        ASSERT_OK_AND_ASSIGN(auto json, type->ToJsonString());
        rapidjson::Document output;
        output.Parse(json.c_str());
        ASSERT_TRUE(output.IsString());
        ASSERT_STREQ(output.GetString(), canonical);
        ASSERT_OK_AND_ASSIGN(auto restored, DataTypeJsonParser::ParseType("geo", output));
        ASSERT_TRUE(field->Equals(restored, true));
    }
}

TEST(GeospatialTypeTest, RejectsInvalidSql) {
    for (const auto& sql :
         {"GEOMETRY()", "GEOMETRY('')", "GEOMETRY(123)", "GEOMETRY(a, spherical)",
          "GEOGRAPHY(a, rhumb)", "GEOGRAPHY(a, '')", "GEOGRAPHY(a, spherical, karney)",
          "GEOGRAPHY(a", "GEOGRAPHY('unterminated)"}) {
        SCOPED_TRACE(sql);
        rapidjson::Document input;
        input.SetString(sql, input.GetAllocator());
        ASSERT_NOK(DataTypeJsonParser::ParseType("geo", input));
    }
}

TEST(GeospatialTypeTest, GeoArrowWkbMetadata) {
    auto wkb = arrow::field("g", arrow::binary(), true,
                            arrow::key_value_metadata({"ARROW:extension:name"}, {"geoarrow.wkb"}));
    auto with_json = [&](const char* json) {
        return wkb->WithMergedMetadata(
            arrow::key_value_metadata({"ARROW:extension:metadata"}, {json}));
    };
    for (const auto& field : {wkb, with_json("{}"), with_json(R"({"edges":"planar"})")}) {
        ASSERT_OK_AND_ASSIGN(auto type, GeospatialType::FromField(*field));
        ASSERT_EQ(type.Type(), FieldType::GEOMETRY);
        ASSERT_EQ(type.Crs(), "OGC:CRS84");
    }
    auto java_field = with_json(R"({"edges":"karney","crs":"EPSG:4326"})");
    ASSERT_OK_AND_ASSIGN(auto cpp_field,
                         GeospatialType::GeographyField("g", "EPSG:4326", "karney"));
    ASSERT_TRUE(cpp_field->Equals(java_field, true));
    ASSERT_OK_AND_ASSIGN(auto geography, GeospatialType::FromField(*java_field));
    ASSERT_EQ(geography.Type(), FieldType::GEOGRAPHY);
    ASSERT_EQ(geography.Crs(), "EPSG:4326");
    ASSERT_EQ(geography.Edges(), "karney");
    for (const auto* json : {"[]", "invalid", R"({"crs":null})", R"({"crs":""})", R"({"edges":5})",
                             R"({"edges":""})", R"({"edges":"rhumb"})"}) {
        SCOPED_TRACE(json);
        auto field = with_json(json);
        ASSERT_NOK(GeospatialType::FromField(*field));
        ASSERT_NOK(DataField(0, field).ToJsonString());
    }
    ASSERT_NOK(GeospatialType::FromField(*wkb->WithType(arrow::utf8())));
    ASSERT_NOK(GeospatialType::GeometryField("g", ""));
    ASSERT_NOK(GeospatialType::GeographyField("g", ""));
    ASSERT_NOK(GeospatialType::GeographyField("g", "EPSG:4326", ""));
    ASSERT_NOK(GeospatialType::GeographyField("g", "EPSG:4326", "planar"));
}

TEST(GeospatialTypeTest, NestedMetadataAndIdsSurviveCDataBridge) {
    const std::string json = R"json({
        "id":7,"name":"r","type":{"type":"ROW NOT NULL","fields":[
            {"id":8,"name":"a","type":{"type":"ARRAY","element":"GEOMETRY('a, b') NOT NULL"}},
            {"id":9,"name":"m","type":{"type":"MAP","key":"GEOMETRY(EPSG:4326)",
                                     "value":"GEOGRAPHY(EPSG:4326, karney)"}},
            {"id":10,"name":"required","type":{"type":"MAP NOT NULL",
                "key":"GEOGRAPHY NOT NULL","value":"BYTES"}}
        ]}
    })json";
    ASSERT_OK_AND_ASSIGN(auto data_field, DataField::FromJsonString(json));
    auto field = DataField::ConvertDataFieldToArrowField(data_field);
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportField(*field, &c_schema).ok());
    auto imported = arrow::ImportField(&c_schema);
    ASSERT_TRUE(imported.ok());
    ASSERT_TRUE(field->Equals(*imported, true));
    ASSERT_OK_AND_ASSIGN(auto restored, DataField::ConvertArrowFieldToDataField(*imported));
    ASSERT_EQ(data_field, restored);
    ASSERT_OK_AND_ASSIGN(auto serialized, restored.ToJsonString());
    rapidjson::Document document;
    document.Parse(serialized.c_str());
    ASSERT_EQ(document["id"].GetInt(), 7);
    ASSERT_STREQ(document["type"]["type"].GetString(), "ROW NOT NULL");
    const auto& fields = document["type"]["fields"];
    ASSERT_EQ(fields.Size(), 3u);
    ASSERT_EQ(fields[0]["id"].GetInt(), 8);
    ASSERT_STREQ(fields[0]["type"]["element"].GetString(), "GEOMETRY('a, b') NOT NULL");
    ASSERT_EQ(fields[1]["id"].GetInt(), 9);
    ASSERT_STREQ(fields[1]["type"]["key"].GetString(), "GEOMETRY(EPSG:4326)");
    ASSERT_EQ(fields[2]["id"].GetInt(), 10);
    ASSERT_STREQ(fields[2]["type"]["key"].GetString(), "GEOGRAPHY(OGC:CRS84, spherical) NOT NULL");
    ASSERT_OK_AND_ASSIGN(auto roundtrip, DataField::FromJsonString(serialized));
    ASSERT_EQ(data_field, roundtrip);
}

TEST(GeospatialTypeTest, LogicalEqualityAndEvolution) {
    ASSERT_OK_AND_ASSIGN(auto geom, GeospatialType::GeometryField("g", "EPSG:4326"));
    ASSERT_OK_AND_ASSIGN(auto same, GeospatialType::GeometryField("g", "epsg:4326"));
    ASSERT_EQ(DataField(1, geom), DataField(1, same));
    ASSERT_OK(GeospatialType::ValidateEvolution(*geom, *same));
    ASSERT_OK_AND_ASSIGN(auto upper, GeospatialType::GeometryField("g", u8"RÉF:İ"));
    ASSERT_OK_AND_ASSIGN(auto lower, GeospatialType::GeometryField("g", u8"réf:ı"));
    ASSERT_EQ(DataField(1, upper), DataField(1, lower));
    ASSERT_OK_AND_ASSIGN(auto crs, GeospatialType::GeometryField("g", "EPSG:3857"));
    ASSERT_OK_AND_ASSIGN(auto geog, GeospatialType::GeographyField("g", "EPSG:4326"));
    ASSERT_OK_AND_ASSIGN(auto edges, GeospatialType::GeographyField("g", "EPSG:4326", "karney"));
    for (const auto& other : {arrow::field("g", arrow::binary()), crs, geog, edges}) {
        ASSERT_FALSE(DataField(1, geom) == DataField(1, other));
        ASSERT_NOK(GeospatialType::ValidateEvolution(*geom, *other));
        auto lhs = arrow::field("r", arrow::struct_({geom}));
        auto rhs = arrow::field("r", arrow::struct_({other}));
        ASSERT_FALSE(DataField(2, lhs) == DataField(2, rhs));
        ASSERT_NOK(GeospatialType::ValidateEvolution(*lhs, *rhs));
    }
    ASSERT_NOK_WITH_MSG(GeospatialType::ValidateEvolution(*geog, *edges),
                        "GEOGRAPHY(EPSG:4326, spherical) -> GEOGRAPHY(EPSG:4326, karney)");
    auto plain = arrow::field("bytes", arrow::binary());
    ASSERT_OK_AND_ASSIGN(auto json, DataType::Create(plain->type(), true, nullptr)->ToJsonString());
    ASSERT_EQ(json, "\"BYTES\"");
}

TEST(GeospatialTypeTest, NestedEvolutionMatchesFieldIds) {
    ASSERT_OK_AND_ASSIGN(auto geom, GeospatialType::GeometryField("shape"));
    ASSERT_OK_AND_ASSIGN(auto geog, GeospatialType::GeographyField("position"));
    auto shape = DataField::ConvertDataFieldToArrowField(DataField(1, geom));
    auto position = DataField::ConvertDataFieldToArrowField(DataField(2, geog));
    auto source = arrow::field("r", arrow::struct_({shape, position}));
    auto renamed = shape->WithName("renamed");
    auto replacement = DataField::ConvertDataFieldToArrowField(
        DataField(3, arrow::field("shape", arrow::binary())));
    for (const auto& fields :
         {arrow::FieldVector{position, renamed}, arrow::FieldVector{renamed},
          arrow::FieldVector{replacement, position}, arrow::FieldVector{geom, geog}}) {
        auto target = arrow::field("r", arrow::struct_(fields));
        ASSERT_OK(GeospatialType::ValidateEvolution(*source, *target));
    }
    ASSERT_OK(GeospatialType::ValidateEvolution(*arrow::field("r", arrow::struct_({geom, geog})),
                                                *source));
    auto incompatible = renamed->WithType(arrow::utf8());
    for (const auto& type : {arrow::struct_({position, incompatible}),
                             arrow::struct_({shape, position->RemoveMetadata()})}) {
        ASSERT_NOK(GeospatialType::ValidateEvolution(*source, *source->WithType(type)));
    }
    auto bytes = arrow::field("value", arrow::binary());
    const std::pair<std::shared_ptr<arrow::DataType>, std::shared_ptr<arrow::DataType>> types[] = {
        {arrow::list(geom), arrow::list(bytes)},
        {std::make_shared<arrow::MapType>(geom->WithNullable(false), bytes),
         arrow::map(arrow::binary(), bytes)},
        {arrow::map(arrow::int32(), geog), arrow::map(arrow::int32(), bytes)}};
    for (const auto& [source_type, target_type] : types) {
        auto left = arrow::field("container", source_type);
        auto right = arrow::field("container", target_type);
        ASSERT_NOK(GeospatialType::ValidateEvolution(*left, *right));
        ASSERT_NOK(GeospatialType::ValidateEvolution(*right, *left));
    }
}

}  // namespace paimon::test
