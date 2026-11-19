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

#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/io/memory.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/geospatial_type.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/format/column_stats.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/parquet/parquet_file_format.h"
#include "paimon/format/parquet/parquet_stats_extractor.h"
#include "paimon/format/reader_builder.h"
#include "paimon/format/writer_builder.h"
#include "paimon/format/written_file_stats_provider.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"
#include "parquet/statistics.h"
#include "rapidjson/document.h"

namespace paimon::parquet::test {
namespace {

constexpr std::string_view kEmptyCollection("\x01\x07\x00\x00\x00\x00\x00\x00\x00", 9);
constexpr std::string_view kEmptyPoint(
    "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf8\x7f"
    "\x00\x00\x00\x00\x00\x00\xf8\x7f",
    21);
constexpr std::string_view kPoint(
    "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f"
    "\x00\x00\x00\x00\x00\x00\x00\x40",
    21);

Result<std::shared_ptr<arrow::StructArray>> MakeData() {
    arrow::BinaryBuilder builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(kEmptyCollection));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendNull());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(kPoint));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> values, builder.Finish());
    arrow::FieldVector fields;
    arrow::ArrayVector arrays;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> geom,
                           GeospatialType::GeometryField("geom", "EPSG:3857"));
    fields.push_back(geom);
    arrays.push_back(values);
    for (const char* edges : {"spherical", "vincenty", "thomas", "andoyer", "karney"}) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> geog,
                               GeospatialType::GeographyField(edges, "EPSG:4326", edges));
        fields.push_back(geog);
        arrays.push_back(values);
    }
    auto child = geom->WithName("g")->WithMergedMetadata(
        arrow::key_value_metadata({DataField::FIELD_ID}, {"20"}));
    auto other_child = fields[5]->WithName("h")->WithMergedMetadata(
        arrow::key_value_metadata({DataField::FIELD_ID}, {"21"}));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> nested,
        arrow::StructArray::Make({values, values}, {child, other_child}));
    fields.push_back(arrow::field("nested", nested->type()));
    arrays.push_back(nested);
    arrow::Int32Builder offsets_builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(offsets_builder.AppendValues({0, 1, 2, 3}));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> offsets,
                                      offsets_builder.Finish());
    auto list_type = arrow::list(geom->WithName("item"));
    auto list =
        std::make_shared<arrow::ListArray>(list_type, 3, offsets->data()->buffers[1], values);
    fields.push_back(arrow::field("arr", list_type));
    arrays.push_back(list);
    arrow::BinaryBuilder key_builder;
    for (int32_t i = 0; i < 3; ++i) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(key_builder.Append(kEmptyPoint));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> keys, key_builder.Finish());
    auto map_type = std::make_shared<arrow::MapType>(geom->WithName("key")->WithNullable(false),
                                                     fields[5]->WithName("value"));
    auto map =
        std::make_shared<arrow::MapArray>(map_type, 3, offsets->data()->buffers[1], keys, values);
    fields.push_back(arrow::field("mapping", map_type));
    arrays.push_back(map);
    fields.push_back(arrow::field("bytes", arrow::binary()));
    arrays.push_back(values);
    for (int32_t i = 0; i < static_cast<int32_t>(fields.size()); ++i) {
        fields[i] = fields[i]->WithMergedMetadata(
            arrow::key_value_metadata({DataField::FIELD_ID}, {std::to_string(i)}));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> result,
                                      arrow::StructArray::Make(arrays, fields));
    return result;
}

}  // namespace

class GeospatialParquetTest : public ::testing::Test {
 public:
    void SetUp() override {
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        fs_ = dir_->GetFileSystem();
        ASSERT_OK_AND_ASSIGN(data_, MakeData());
    }

    void Write(const std::string& path) {
        ParquetFileFormat format({});
        ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(*arrow::schema(data_->type()->fields()), &schema).ok());
        ASSERT_OK_AND_ASSIGN(auto builder, format.CreateWriterBuilder(&schema, 1024));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> output, fs_->Create(path, true));
        ASSERT_OK_AND_ASSIGN(auto writer, builder->Build(output, "uncompressed"));
        ArrowArray array;
        ASSERT_TRUE(arrow::ExportArray(*data_, &array).ok());
        ASSERT_OK(writer->AddBatch(&array));
        ASSERT_OK(writer->Finish());
        ASSERT_OK(output->Close());
        auto stats_provider = dynamic_cast<WrittenFileStatsProvider*>(writer.get());
        ASSERT_TRUE(stats_provider);
        ASSERT_OK_AND_ASSIGN(auto stats, stats_provider->ExtractWrittenFileStats(GetDefaultPool()));
        ASSERT_EQ(stats.size(), data_->num_fields());
        for (int32_t i = 0; i < 6; ++i) {
            ASSERT_EQ(stats[i]->NullCount(), 1);
            auto string_stats = dynamic_cast<StringColumnStats*>(stats[i].get());
            ASSERT_TRUE(string_stats);
            ASSERT_FALSE(string_stats->Min());
            ASSERT_FALSE(string_stats->Max());
        }
    }

    void Check(const std::string& path) {
        auto native = ::parquet::ParquetFileReader::OpenFile(path, false);
        auto metadata = native->metadata();
        ASSERT_EQ(metadata->num_rows(), 3);
        ASSERT_EQ(metadata->schema()->num_columns(), 12);
        const std::vector<int32_t> field_ids = {0,
                                                1,
                                                2,
                                                3,
                                                4,
                                                5,
                                                20,
                                                21,
                                                SpecialFields::GetArrayElementFieldId(7, 1),
                                                SpecialFields::GetMapKeyFieldId(8, 1),
                                                SpecialFields::GetArrayElementFieldId(8, 1),
                                                9};
        for (int32_t i = 0; i < metadata->schema()->num_columns(); ++i) {
            ASSERT_EQ(metadata->schema()->Column(i)->schema_node()->field_id(), field_ids[i]);
        }
        for (int32_t i = 0; i < 11; ++i) {
            const auto* column = metadata->schema()->Column(i);
            const auto& logical = column->logical_type();
            ASSERT_EQ(column->physical_type(), ::parquet::Type::BYTE_ARRAY);
            ASSERT_EQ(column->sort_order(), ::parquet::SortOrder::UNKNOWN);
            if ((i >= 1 && i <= 5) || i == 7 || i == 10) {
                ASSERT_TRUE(logical->is_geography());
                const auto& geography =
                    checked_cast<const ::parquet::GeographyLogicalType&>(*logical);
                ASSERT_EQ(geography.crs(), "EPSG:4326");
                ASSERT_EQ(static_cast<int32_t>(geography.algorithm()), i <= 5 ? i : 5);
            } else {
                ASSERT_TRUE(logical->is_geometry());
                const auto& geometry =
                    checked_cast<const ::parquet::GeometryLogicalType&>(*logical);
                ASSERT_EQ(geometry.crs(), "EPSG:3857");
            }
            auto stats = metadata->RowGroup(0)->ColumnChunk(i)->statistics();
            ASSERT_TRUE(stats);
            ASSERT_TRUE(stats->HasNullCount());
            ASSERT_EQ(stats->null_count(), i == 9 ? 0 : 1);
            ASSERT_FALSE(stats->HasMinMax());
        }
        ASSERT_TRUE(metadata->schema()->Column(11)->logical_type()->is_none());
        auto bytes_stats = metadata->RowGroup(0)->ColumnChunk(11)->statistics();
        ASSERT_TRUE(bytes_stats);
        ASSERT_TRUE(bytes_stats->HasMinMax());
        for (bool projected : {false, true}) {
            auto read_schema = arrow::schema(data_->type()->fields());
            std::shared_ptr<arrow::Array> expected = data_;
            if (projected) {
                auto nested = checked_pointer_cast<arrow::StructArray>(data_->field(6));
                auto projected_row =
                    arrow::StructArray::Make({nested->field(0)}, {nested->type()->field(0)});
                ASSERT_TRUE(projected_row.ok());
                read_schema =
                    arrow::schema({read_schema->field(6)->WithType((*projected_row)->type()),
                                   read_schema->field(7), read_schema->field(8)});
                auto projected_data = arrow::StructArray::Make(
                    {*projected_row, data_->field(7), data_->field(8)}, read_schema->fields());
                ASSERT_TRUE(projected_data.ok());
                expected = *projected_data;
            }
            ParquetFileFormat format({});
            ASSERT_OK_AND_ASSIGN(auto builder, format.CreateReaderBuilder(2));
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, fs_->Open(path));
            ASSERT_OK_AND_ASSIGN(auto reader, builder->Build(input));
            ASSERT_OK_AND_ASSIGN(auto c_file_schema, reader->GetFileSchema());
            auto file_schema = arrow::ImportSchema(c_file_schema.get());
            ASSERT_TRUE(file_schema.ok());
            ASSERT_TRUE(
                GeospatialType::Equals(*arrow::field("", arrow::struct_((*file_schema)->fields())),
                                       *arrow::field("", data_->type())));
            ArrowSchema c_schema;
            ASSERT_TRUE(arrow::ExportSchema(*read_schema, &c_schema).ok());
            ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, std::nullopt));
            ASSERT_OK_AND_ASSIGN(auto result, paimon::test::ReadResultCollector::CollectResult(
                                                  std::move(reader), true));
            ASSERT_TRUE(result->type()->Equals(*arrow::struct_(read_schema->fields()), true))
                << result->type()->ToString(true) << "\nExpected: " << read_schema->ToString(true);
            ASSERT_TRUE(result->Equals(arrow::ChunkedArray(expected))) << result->ToString();
        }
    }

 protected:
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<arrow::StructArray> data_;
};

TEST_F(GeospatialParquetTest, NativeAnnotationsNestedValuesAndStatistics) {
    std::string path = dir_->Str() + "/geospatial.parquet";
    ASSERT_NO_FATAL_FAILURE(Write(path));
    ASSERT_NO_FATAL_FAILURE(Check(path));
}

TEST_F(GeospatialParquetTest, NativeSchemaDefaultsAndEscapedCrs) {
    for (const std::string crs : {"", R"(a "quoted"\CRS)"}) {
        for (const auto& logical :
             {::parquet::LogicalType::Geometry(crs), ::parquet::LogicalType::Geography(crs)}) {
            for (bool nullable : {false, true}) {
                SCOPED_TRACE(logical->ToString());
                SCOPED_TRACE(nullable);
                auto node = ::parquet::schema::PrimitiveNode::Make(
                    "geo",
                    nullable ? ::parquet::Repetition::OPTIONAL : ::parquet::Repetition::REQUIRED,
                    logical, ::parquet::Type::BYTE_ARRAY, -1, 42);
                ::parquet::SchemaDescriptor native_schema;
                native_schema.Init(::parquet::schema::GroupNode::Make(
                    "root", ::parquet::Repetition::REQUIRED, {node}));
                std::shared_ptr<arrow::Schema> schema;
                ASSERT_TRUE(::parquet::arrow::FromParquetSchema(&native_schema, &schema).ok());
                ASSERT_EQ(schema->num_fields(), 1);
                ASSERT_EQ(schema->field(0)->nullable(), nullable);
                ASSERT_OK_AND_ASSIGN(auto type, GeospatialType::FromField(*schema->field(0)));
                ASSERT_EQ(type.Type(),
                          logical->is_geometry() ? FieldType::GEOMETRY : FieldType::GEOGRAPHY);
                ASSERT_EQ(type.Crs(), crs.empty() ? "OGC:CRS84" : crs);
                ASSERT_EQ(type.Edges(), logical->is_geometry() ? "" : "spherical");
                std::shared_ptr<::parquet::SchemaDescriptor> roundtrip;
                ASSERT_TRUE(::parquet::arrow::ToParquetSchema(
                                schema.get(), *::parquet::default_writer_properties(), &roundtrip)
                                .ok());
                const auto* column = roundtrip->Column(0);
                ASSERT_EQ(column->schema_node()->field_id(), 42);
                ASSERT_EQ(column->max_definition_level(), nullable ? 1 : 0);
                ASSERT_EQ(column->sort_order(), ::parquet::SortOrder::UNKNOWN);
                rapidjson::Document json;
                json.Parse(column->logical_type()->ToJSON().c_str());
                ASSERT_FALSE(json.HasParseError());
                ASSERT_TRUE(json.HasMember("crs"));
                ASSERT_TRUE(json["crs"].IsString());
                ASSERT_EQ(json["crs"].GetString(), type.Crs());
                std::shared_ptr<arrow::Schema> restored;
                ASSERT_TRUE(::parquet::arrow::FromParquetSchema(roundtrip.get(), &restored).ok());
                ASSERT_TRUE(schema->Equals(*restored, true));
            }
        }
    }
}

TEST_F(GeospatialParquetTest, MissingRowGroupStatisticsKeepsFileNullCountUnknown) {
    auto schema = arrow::schema({data_->type()->field(0), data_->type()->field(1)});
    auto table = arrow::Table::Make(schema, arrow::ArrayVector{data_->field(0), data_->field(1)});
    ParquetStatsExtractor extractor(schema);
    const std::vector<std::vector<std::pair<bool, std::optional<int64_t>>>> groups = {
        {{true, 1}, {true, 2}, {false, std::nullopt}, {true, std::nullopt}},
        {{false, std::nullopt}, {true, std::nullopt}}};
    for (const auto& sequence : groups) {
        std::shared_ptr<::parquet::FileMetaData> combined;
        for (const auto& [write_stats, expected_file_null_count] : sequence) {
            ::parquet::WriterProperties::Builder properties;
            if (!write_stats) {
                properties.disable_statistics();
            }
            auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
            ASSERT_TRUE(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink,
                                                     table->num_rows(), properties.build())
                            .ok());
            auto buffer = sink->Finish().ValueOrDie();
            auto reader = ::parquet::ParquetFileReader::Open(
                std::make_shared<arrow::io::BufferReader>(buffer));
            if (combined) {
                combined->AppendRowGroups(*reader->metadata());
            } else {
                combined = reader->metadata();
            }
            ASSERT_OK_AND_ASSIGN(auto stats,
                                 extractor.ExtractFromMetadata(*combined, GetDefaultPool()));
            ASSERT_EQ(stats.first.size(), 2u);
            for (const auto& column : stats.first) {
                ASSERT_EQ(column->NullCount(), expected_file_null_count);
                auto string_stats = dynamic_cast<StringColumnStats*>(column.get());
                ASSERT_TRUE(string_stats);
                ASSERT_FALSE(string_stats->Min());
                ASSERT_FALSE(string_stats->Max());
            }
        }
    }
}

TEST_F(GeospatialParquetTest, IncompatibleReadSchema) {
    std::string path = dir_->Str() + "/geospatial.parquet";
    ASSERT_NO_FATAL_FAILURE(Write(path));
    ParquetFileFormat format({});
    ASSERT_OK_AND_ASSIGN(auto builder, format.CreateReaderBuilder(2));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, fs_->Open(path));
    ASSERT_OK_AND_ASSIGN(auto reader, builder->Build(input));
    ASSERT_OK_AND_ASSIGN(auto different_crs, GeospatialType::GeometryField("geom", "EPSG:4326"));
    const auto& map_type = checked_cast<const arrow::MapType&>(*data_->field(8)->type());
    ASSERT_OK_AND_ASSIGN(auto different_edges,
                         GeospatialType::GeographyField("value", "EPSG:4326", "thomas"));
    for (const auto& field :
         {different_crs, data_->type()->field(0)->WithName("bytes"),
          arrow::field("geom", arrow::binary()), arrow::field("arr", arrow::list(arrow::binary())),
          arrow::field("mapping", arrow::map(arrow::binary(), map_type.item_field())),
          arrow::field("mapping",
                       std::make_shared<arrow::MapType>(map_type.key_field(), different_edges)),
          arrow::field("nested", arrow::struct_({arrow::field("h", arrow::binary())}))}) {
        ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(*arrow::schema({field}), &schema).ok());
        ASSERT_NOK_WITH_MSG(reader->SetReadSchema(&schema, nullptr, std::nullopt),
                            "Incompatible geospatial type change");
    }
}

TEST_F(GeospatialParquetTest, JavaInteroperability) {
    const char* directory = std::getenv("PAIMON_GEOSPATIAL_INTEROP_DIR");
    if (!directory) {
        GTEST_SKIP() << "Run test/inte/geospatial/run.sh to exercise Paimon Java interoperability";
    }
    ASSERT_NO_FATAL_FAILURE(Check(std::string(directory) + "/java.parquet"));
    ASSERT_NO_FATAL_FAILURE(Write(std::string(directory) + "/cpp.parquet"));
}

}  // namespace paimon::parquet::test
