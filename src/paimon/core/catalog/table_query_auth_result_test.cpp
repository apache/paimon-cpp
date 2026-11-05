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

#include "paimon/core/catalog/table_query_auth_result.h"

#include <cmath>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

std::string FieldRefJson(int32_t index, const std::string& name, const std::string& type) {
    return fmt::format(R"({{"index": {}, "name": "{}", "type": "{}"}})", index, name, type);
}

std::string LeafJson(const std::string& field_ref, const std::string& function,
                     const std::string& literals) {
    return fmt::format(R"({{"kind": "LEAF", "transform": {{"name": "FIELD_REF", "fieldRef": {}}},)"
                       R"( "function": "{}", "literals": {}}})",
                       field_ref, function, literals);
}

std::string ConstantJson(const std::string& function, const std::string& literals = "[]") {
    return fmt::format(R"({{"kind": "LEAF", "transform": {{"name": "NULL"}}, "function": "{}",)"
                       R"( "literals": {}}})",
                       function, literals);
}

std::string CompoundJson(const std::string& function, const std::string& left,
                         const std::string& right) {
    return fmt::format(R"({{"kind": "COMPOUND", "function": "{}", "children": [{}, {}]}})",
                       function, left, right);
}

Result<Literal> FilterLiteral(const std::string& field_ref, const std::string& function,
                              const std::string& literals) {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<TableQueryAuthResult> result,
        TableQueryAuthResult::Create({LeafJson(field_ref, function, literals)}, {}));
    auto leaf = std::dynamic_pointer_cast<LeafPredicate>(result->RowFilter());
    if (leaf == nullptr) {
        return Status::Invalid("the row filter is not a leaf");
    }
    return leaf->Literals().at(0);
}

std::string IdRef() {
    return FieldRefJson(0, "id", "INT NOT NULL");
}
std::string NameRef() {
    return FieldRefJson(1, "name", "STRING");
}
std::string PhoneRef() {
    return FieldRefJson(2, "phone", "STRING");
}

std::vector<DataField> TableFields() {
    return {DataField(0, arrow::field("id", arrow::int32(), /*nullable=*/false)),
            DataField(1, arrow::field("name", arrow::utf8())),
            DataField(2, arrow::field("phone", arrow::utf8())),
            DataField(3, arrow::field("salary", arrow::int64()))};
}

std::shared_ptr<arrow::StructArray> MakeBatch(const std::string& rows) {
    auto type =
        arrow::struct_({arrow::field("id", arrow::int32(), /*nullable=*/false),
                        arrow::field("name", arrow::utf8()), arrow::field("phone", arrow::utf8()),
                        arrow::field("salary", arrow::int64())});
    auto array = arrow::ipc::internal::json::ArrayFromJSON(type, rows).ValueOrDie();
    return checked_pointer_cast<arrow::StructArray>(array);
}

Result<std::vector<char>> Evaluate(const std::shared_ptr<Predicate>& predicate,
                                   const std::shared_ptr<arrow::StructArray>& batch) {
    std::map<std::string, int32_t> field_index;
    for (int32_t i = 0; i < batch->num_fields(); ++i) {
        field_index[batch->struct_type()->field(i)->name()] = i;
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> bound,
                           PredicateUtils::CreatePickedFieldFilter(predicate, field_index));
    auto filter = std::dynamic_pointer_cast<PredicateFilter>(bound);
    if (filter == nullptr) {
        return Status::Invalid("the bound predicate is not a filter");
    }
    return filter->Test(*batch, arrow::default_memory_pool());
}

Result<std::shared_ptr<arrow::Array>> Mask(const TableQueryAuthResult& result,
                                           const std::string& column,
                                           const std::shared_ptr<arrow::StructArray>& batch,
                                           const std::shared_ptr<arrow::DataType>& type) {
    auto mask = result.Masks().find(column);
    if (mask == result.Masks().end()) {
        return Status::Invalid(fmt::format("no mask on column '{}'", column));
    }
    return mask->second->Evaluate(*batch, type, arrow::default_memory_pool());
}

std::shared_ptr<arrow::Array> Strings(const std::string& json) {
    return arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), json).ValueOrDie();
}

}  // namespace

TEST(TableQueryAuthResultTest, ParsesTheRowFilter) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableQueryAuthResult> result,
                         TableQueryAuthResult::Create(
                             {LeafJson(IdRef(), "GREATER_THAN", "[1]"),
                              CompoundJson("OR", LeafJson(NameRef(), "STARTS_WITH", R"(["b"])"),
                                           LeafJson(NameRef(), "IS_NULL", "[]"))},
                             {}));
    ASSERT_TRUE(result->HasRules());
    ASSERT_NE(nullptr, result->RowFilter());
    ASSERT_EQ(std::set<std::string>({"id", "name"}), result->RowFilterFields());
    ASSERT_TRUE(result->Masks().empty());

    auto batch = MakeBatch(
        R"([[1, "bob", null, 1], [2, "bob", null, 2], [3, "carol", null, 3], [4, null, null, 4]])");
    ASSERT_OK_AND_ASSIGN(std::vector<char> selected, Evaluate(result->RowFilter(), batch));
    ASSERT_EQ(std::vector<char>({0, 1, 0, 1}), selected);
}

TEST(TableQueryAuthResultTest, NullLiteralsMatchAsInJava) {
    auto batch = MakeBatch(R"([[1, "a", null, 1], [2, "b", null, 2], [3, null, null, 3]])");
    auto evaluate = [&](const std::string& filter) -> Result<std::vector<char>> {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableQueryAuthResult> result,
                               TableQueryAuthResult::Create({filter}, {}));
        return Evaluate(result->RowFilter(), batch);
    };
    const std::vector<std::pair<std::string, std::vector<char>>> cases = {
        {LeafJson(IdRef(), "EQUAL", "[null]"), {0, 0, 0}},
        {LeafJson(IdRef(), "IN", "[1, null]"), {1, 0, 0}},
        {LeafJson(IdRef(), "IN", "[null]"), {0, 0, 0}},
        {LeafJson(NameRef(), "IN", "[]"), {0, 0, 0}},
        {LeafJson(IdRef(), "NOT_IN", "[1, null]"), {0, 0, 0}},
        {LeafJson(IdRef(), "NOT_IN", "[1]"), {0, 1, 1}},
        {LeafJson(NameRef(), "NOT_IN", "[]"), {1, 1, 0}},
        {LeafJson(NameRef(), "NOT_IN", "[null]"), {0, 0, 0}},
        {LeafJson(IdRef(), "BETWEEN", "[2, 2]"), {0, 1, 0}},
        {LeafJson(IdRef(), "NOT_BETWEEN", "[2, 2]"), {1, 0, 1}},
        {LeafJson(NameRef(), "IS_NOT_NULL", "[]"), {1, 1, 0}}};
    for (const auto& [filter, expected] : cases) {
        ASSERT_OK_AND_ASSIGN(std::vector<char> selected, evaluate(filter));
        ASSERT_EQ(expected, selected) << filter;
    }
}

TEST(TableQueryAuthResultTest, FoldsConstantFiltersAsJava) {
    const std::string always_true = ConstantJson("TRUE");
    const std::string always_false = ConstantJson("FALSE");
    const std::string id_is_1 = LeafJson(IdRef(), "EQUAL", "[1]");

    for (const std::string& filter : {always_true, LeafJson(NameRef(), "TRUE", "[]"),
                                      CompoundJson("AND", always_true, always_true),
                                      CompoundJson("OR", always_false, always_true),
                                      CompoundJson("OR", id_is_1, always_true)}) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableQueryAuthResult> result,
                             TableQueryAuthResult::Create({filter}, {}));
        ASSERT_FALSE(result->HasRules()) << filter;
        ASSERT_FALSE(result->RejectsAllRows()) << filter;
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableQueryAuthResult> no_rules,
                         TableQueryAuthResult::Create({}, {}));
    for (const std::vector<std::string>& filter :
         std::vector<std::vector<std::string>>{{always_false},
                                               {LeafJson(NameRef(), "FALSE", "[]")},
                                               {CompoundJson("AND", id_is_1, always_false)},
                                               {CompoundJson("OR", always_false, always_false)},
                                               {id_is_1, always_false}}) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableQueryAuthResult> result,
                             TableQueryAuthResult::Create(filter, {}));
        ASSERT_TRUE(result->HasRules()) << filter.back();
        ASSERT_TRUE(result->RejectsAllRows()) << filter.back();
        ASSERT_EQ(nullptr, result->RowFilter()) << filter.back();
        ASSERT_FALSE(result->SameFilter(*no_rules)) << filter.back();
    }

    auto batch = MakeBatch(R"([[1, "a", null, 1], [2, "b", null, 2]])");
    for (const std::string& filter :
         {CompoundJson("AND", id_is_1, always_true), CompoundJson("OR", always_false, id_is_1)}) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableQueryAuthResult> result,
                             TableQueryAuthResult::Create({filter}, {}));
        ASSERT_FALSE(result->RejectsAllRows()) << filter;
        ASSERT_EQ(std::set<std::string>({"id"}), result->RowFilterFields()) << filter;
        ASSERT_OK_AND_ASSIGN(std::vector<char> selected, Evaluate(result->RowFilter(), batch));
        ASSERT_EQ(std::vector<char>({1, 0}), selected) << filter;
    }

    Status unsupported_beside_constant =
        TableQueryAuthResult::Create(
            {CompoundJson("OR", always_true, LeafJson(NameRef(), "NOT_LIKE", R"(["a%"])"))}, {})
            .status();
    ASSERT_TRUE(unsupported_beside_constant.IsNotImplemented())
        << unsupported_beside_constant.ToString();
    Status computed_constant =
        TableQueryAuthResult::Create(
            {R"({"kind": "LEAF", "transform": {"name": "NULL"}, "function": "EQUAL",)"
             R"( "literals": [1]})"},
            {})
            .status();
    ASSERT_TRUE(computed_constant.IsNotImplemented()) << computed_constant.ToString();

    for (const std::string function : {"TRUE", "FALSE"}) {
        for (const std::string literals : {"{}", R"("x")", "1"}) {
            for (const std::string& filter :
                 {ConstantJson(function, literals), LeafJson(NameRef(), function, literals)}) {
                ASSERT_NOK_WITH_MSG(TableQueryAuthResult::Create({filter}, {}),
                                    "The 'literals' of a row filter must be a JSON array.");
            }
        }
    }
}

TEST(TableQueryAuthResultTest, MatchesLikeAsJava) {
    auto batch = MakeBatch(
        "[[1, \"a\\nb\", null, 1], [2, \"a\\r\\nb\", null, 2], [3, \"a\u2028b\", null, 3],"
        " [4, \"a_b\", null, 4], [5, \"a%b\", null, 5], [6, \"\", null, 6],"
        " [7, null, null, 7], [8, \"名字\", null, 8]]");
    const std::vector<std::pair<std::string, std::vector<char>>> cases = {
        {"a_b", {1, 0, 1, 1, 1, 0, 0, 0}},      {"a__b", {0, 1, 0, 0, 0, 0, 0, 0}},
        {"a%b", {1, 1, 1, 1, 1, 0, 0, 0}},      {R"(a\\_b)", {0, 0, 0, 1, 0, 0, 0, 0}},
        {R"(a\\%b)", {0, 0, 0, 0, 1, 0, 0, 0}}, {"", {0, 0, 0, 0, 0, 1, 0, 0}},
        {"%", {1, 1, 1, 1, 1, 1, 0, 1}},        {"名_", {0, 0, 0, 0, 0, 0, 0, 1}}};
    for (const auto& [pattern, expected] : cases) {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableQueryAuthResult> result,
            TableQueryAuthResult::Create(
                {LeafJson(NameRef(), "LIKE", fmt::format(R"(["{}"])", pattern))}, {}));
        ASSERT_OK_AND_ASSIGN(std::vector<char> selected, Evaluate(result->RowFilter(), batch));
        ASSERT_EQ(expected, selected) << pattern;
    }
}

TEST(TableQueryAuthResultTest, ParsesLiteralsOfSupportedTypes) {
    auto literal = [](const std::string& field_ref, const std::string& literals) {
        return FilterLiteral(field_ref, "EQUAL", literals);
    };
    ASSERT_OK_AND_ASSIGN(Literal boolean, literal(FieldRefJson(0, "f", "BOOLEAN"), "[true]"));
    ASSERT_TRUE(boolean.GetValue<bool>());
    ASSERT_OK_AND_ASSIGN(Literal tinyint, literal(FieldRefJson(0, "f", "TINYINT"), "[-3]"));
    ASSERT_EQ(-3, tinyint.GetValue<int8_t>());
    ASSERT_OK_AND_ASSIGN(Literal smallint, literal(FieldRefJson(0, "f", "SMALLINT"), "[300]"));
    ASSERT_EQ(300, smallint.GetValue<int16_t>());
    ASSERT_OK_AND_ASSIGN(Literal bigint, literal(FieldRefJson(0, "f", "BIGINT"), "[5000000000]"));
    ASSERT_EQ(5000000000LL, bigint.GetValue<int64_t>());
    ASSERT_OK_AND_ASSIGN(Literal float_literal, literal(FieldRefJson(0, "f", "FLOAT"), "[0.25]"));
    ASSERT_FLOAT_EQ(0.25f, float_literal.GetValue<float>());
    ASSERT_OK_AND_ASSIGN(Literal double_literal, literal(FieldRefJson(0, "f", "DOUBLE"), "[1.5]"));
    ASSERT_DOUBLE_EQ(1.5, double_literal.GetValue<double>());
    ASSERT_OK_AND_ASSIGN(Literal varchar, literal(FieldRefJson(0, "f", "VARCHAR(10)"), R"(["x"])"));
    ASSERT_EQ("x", varchar.GetValue<std::string>());
    ASSERT_OK_AND_ASSIGN(Literal decimal_literal,
                         literal(FieldRefJson(0, "f", "DECIMAL(10, 2)"), R"(["12.3"])"));
    auto decimal = decimal_literal.GetValue<Decimal>();
    ASSERT_EQ(10, decimal.Precision());
    ASSERT_EQ(2, decimal.Scale());
    ASSERT_EQ(1230, decimal.ToUnscaledLong());

    ASSERT_NOK_WITH_MSG(TableQueryAuthResult::Create(
                            {LeafJson(FieldRefJson(0, "f", "INT"), "EQUAL", R"(["1"])")}, {}),
                        "does not fit column 'f' of type int32");
    ASSERT_NOK_WITH_MSG(TableQueryAuthResult::Create(
                            {LeafJson(FieldRefJson(0, "f", "TINYINT"), "EQUAL", "[300]")}, {}),
                        "does not fit column 'f'");
    ASSERT_NOK_WITH_MSG(
        TableQueryAuthResult::Create(
            {LeafJson(FieldRefJson(0, "f", "DECIMAL(3, 2)"), "EQUAL", R"(["123.45"])")}, {}),
        "does not fit column 'f'");
}

TEST(TableQueryAuthResultTest, ParsesFloatLiteralsAsJava) {
    const std::string field = FieldRefJson(0, "f", "FLOAT");
    for (const std::string value :
         {"1e300", "-1e300", "3.403e38", "-3.403e38", R"("NaN")", R"("Infinity")"}) {
        ASSERT_NOK_WITH_MSG(FilterLiteral(field, "EQUAL", "[" + value + "]"),
                            "does not fit column 'f' of type float");
    }
    ASSERT_OK_AND_ASSIGN(Literal maximum, FilterLiteral(field, "EQUAL", "[3.4028234663852886e38]"));
    ASSERT_EQ(std::numeric_limits<float>::max(), maximum.GetValue<float>());
    ASSERT_OK_AND_ASSIGN(Literal minimum,
                         FilterLiteral(field, "EQUAL", "[-3.4028234663852886e38]"));
    ASSERT_EQ(std::numeric_limits<float>::lowest(), minimum.GetValue<float>());
    ASSERT_OK_AND_ASSIGN(Literal java_maximum, FilterLiteral(field, "EQUAL", "[3.4028235e38]"));
    ASSERT_EQ(std::numeric_limits<float>::max(), java_maximum.GetValue<float>());
    ASSERT_OK_AND_ASSIGN(Literal java_minimum, FilterLiteral(field, "EQUAL", "[-3.4028235e38]"));
    ASSERT_EQ(std::numeric_limits<float>::lowest(), java_minimum.GetValue<float>());
    ASSERT_OK_AND_ASSIGN(Literal double_literal,
                         FilterLiteral(FieldRefJson(0, "f", "DOUBLE"), "EQUAL", "[1e300]"));
    ASSERT_DOUBLE_EQ(1e300, double_literal.GetValue<double>());

    const float positive = std::ldexp(1.0f, 60) + std::ldexp(1.0f, 37);
    const float unsigned_value = std::ldexp(1.0f, 63) + std::ldexp(1.0f, 40);
    const float big_integer = std::ldexp(1.0f, 64) + std::ldexp(1.0f, 41);
    for (const auto& [text, expected] : std::vector<std::pair<std::string, float>>{
             {"1152921573326323713", positive},
             {"-1152921573326323713", -positive},
             {"9223372586610589697", unsigned_value},
             {"-9223372586610589697", -unsigned_value},
             {"18446745173221179393", big_integer},
             {"-18446745173221179393", -big_integer},
             {"340282346638528859811704183484516925440", std::numeric_limits<float>::max()}}) {
        ASSERT_OK_AND_ASSIGN(Literal literal, FilterLiteral(field, "EQUAL", "[" + text + "]"));
        ASSERT_EQ(expected, literal.GetValue<float>()) << text;
    }
    for (const std::string text : {"18446745173221179393.0", "1.8446745173221179393e19"}) {
        ASSERT_OK_AND_ASSIGN(Literal literal, FilterLiteral(field, "EQUAL", "[" + text + "]"));
        ASSERT_EQ(std::ldexp(1.0f, 64), literal.GetValue<float>()) << text;
    }
    ASSERT_NOK_WITH_MSG(FilterLiteral(field, "EQUAL", "[340282356779733661637539395458142568448]"),
                        "does not fit column 'f' of type float");
}

TEST(TableQueryAuthResultTest, PreservesDoubleLiteralPrecisionAsJava) {
    const std::string field = FieldRefJson(0, "f", "DOUBLE");
    const double infinity = std::numeric_limits<double>::infinity();
    for (const auto& [text, expected] : std::vector<std::pair<std::string, double>>{
             {"0.94109657376859879", 0.94109657376859879},
             {"0.92057493778151411", 0.92057493778151411},
             {"-0.94109657376859879", -0.94109657376859879},
             {"0.986801886310680574", 0.986801886310680574},
             {"1.7976931348623157e308", std::numeric_limits<double>::max()},
             {"4.9406564584124654e-324", std::numeric_limits<double>::denorm_min()}}) {
        SCOPED_TRACE(text);
        ASSERT_OK_AND_ASSIGN(Literal literal, FilterLiteral(field, "EQUAL", "[" + text + "]"));
        ASSERT_EQ(expected, literal.GetValue<double>());

        arrow::DoubleBuilder builder;
        ASSERT_TRUE(builder.Append(std::nextafter(expected, -infinity)).ok());
        ASSERT_TRUE(builder.Append(expected).ok());
        ASSERT_TRUE(builder.Append(std::nextafter(expected, infinity)).ok());
        ASSERT_TRUE(builder.AppendNull().ok());
        std::shared_ptr<arrow::Array> values;
        ASSERT_TRUE(builder.Finish(&values).ok());
        auto batch =
            arrow::StructArray::Make({values}, {arrow::field("f", arrow::float64())}).ValueOrDie();
        for (const auto& [function, selected] :
             std::vector<std::pair<std::string, std::vector<char>>>{
                 {"EQUAL", {0, 1, 0, 0}}, {"GREATER_THAN", {0, 0, 1, 0}}}) {
            ASSERT_OK_AND_ASSIGN(
                std::shared_ptr<TableQueryAuthResult> rules,
                TableQueryAuthResult::Create({LeafJson(field, function, "[" + text + "]")}, {}));
            ASSERT_OK_AND_ASSIGN(std::vector<char> actual, Evaluate(rules->RowFilter(), batch));
            ASSERT_EQ(selected, actual) << function;
        }
    }
}

TEST(TableQueryAuthResultTest, RefusesMalformedRuleJson) {
    const std::string suffix = std::string(1, '\0') + "not JSON";
    ASSERT_NOK_WITH_MSG(TableQueryAuthResult::Create({ConstantJson("TRUE") + suffix}, {}),
                        "Row filter contains an unescaped NUL byte");
    ASSERT_NOK_WITH_MSG(
        TableQueryAuthResult::Create({}, {{"name", R"({"name": "NULL"})" + suffix}}),
        "Column mask transform contains an unescaped NUL byte");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableQueryAuthResult> result,
                         TableQueryAuthResult::Create(
                             {}, {{"name", R"({"name": "CONCAT", "inputs": ["a\u0000b"]})"}}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> masked,
                         Mask(*result, "name", MakeBatch(R"([[1, "x", null, 1]])"), arrow::utf8()));
    ASSERT_TRUE(masked->Equals(*Strings(R"(["a\u0000b"])")));

    for (const std::string& invalid :
         {std::string(1, '\xFF'), std::string("\xC3\x28", 2), std::string("\xED\xA0\x80", 3)}) {
        ASSERT_NOK_WITH_MSG(TableQueryAuthResult::Create(
                                {LeafJson(NameRef(), "EQUAL", "[\"" + invalid + "\"]")}, {}),
                            "Row filter is not valid JSON");
        ASSERT_NOK_WITH_MSG(
            TableQueryAuthResult::Create(
                {}, {{"name", R"({"name": "CONCAT", "inputs": [")" + invalid + R"("]})"}}),
            "Column mask transform is not valid JSON");
    }

    ASSERT_NOK(TableQueryAuthResult::Create(
        {LeafJson(R"({"index": 0, "name": "id", "type": "INT\u0000suffix"})", "EQUAL", "[1]")},
        {}));
    ASSERT_NOK_WITH_MSG(
        TableQueryAuthResult::Create(
            {}, {{"name", R"({"name": "FIELD_REF", "fieldRef": {"index": 1, "name": "name",)"
                          R"("type": {"type": "ARRAY\u0000suffix", "element": "STRING"}}})"}}),
        "complex data type cannot contain a NUL byte");

    ASSERT_NOK_WITH_MSG(TableQueryAuthResult::Create({"{"}, {}), "Row filter is not valid JSON");
    ASSERT_NOK_WITH_MSG(
        TableQueryAuthResult::Create(
            {R"({"kind": "LEAF", "transform": {"name": "NULL"}, "function": "TRUE",)"
             R"( "function": "FALSE", "literals": []})"},
            {}),
        "Row filter is ambiguous: JSON object names member 'function' more than once.");
    ASSERT_NOK_WITH_MSG(
        TableQueryAuthResult::Create(
            {fmt::format(R"({{"kind": "COMPOUND", "function": "AND", "children": [{}],)"
                         R"( "children": [{}]}})",
                         ConstantJson("TRUE"), ConstantJson("FALSE"))},
            {}),
        "member 'children'");
    ASSERT_NOK_WITH_MSG(
        TableQueryAuthResult::Create(
            {LeafJson(R"({"index": 1, "name": "name", "name": "phone", "type": "STRING"})",
                      "IS_NULL", "[]")},
            {}),
        "member 'name'");
    ASSERT_NOK_WITH_MSG(
        TableQueryAuthResult::Create(
            {LeafJson(R"({"index": 0, "name": "f", "type": {"type": "ARRAY", "element": "INT",)"
                      R"( "element": "STRING"}})",
                      "IS_NULL", "[]")},
            {}),
        "member 'element'");
    ASSERT_NOK_WITH_MSG(
        TableQueryAuthResult::Create(
            {}, {{"name", fmt::format(R"({{"name": "FIELD_REF", "fieldRef": {}, "name": "NULL"}})",
                                      NameRef())}}),
        "Column mask transform is ambiguous: JSON object names member 'name' more than once.");
    ASSERT_NOK_WITH_MSG(
        TableQueryAuthResult::Create(
            {}, {{"name", R"({"name": "CONCAT", "inputs": ["***"], "inputs": []})"}}),
        "member 'inputs'");
    ASSERT_NOK_WITH_MSG(
        TableQueryAuthResult::Create(
            {}, {{"name", R"({"name": "SUBSTRING", "inputs": [{"index": 1, "name": "name",)"
                          R"( "type": "STRING", "type": "INT"}, 1, 3]})"}}),
        "member 'type'");
    ASSERT_NOK_WITH_MSG(
        TableQueryAuthResult::Create(
            {}, {{"name", R"({"name": "NULL", "n\u0061me": "CONCAT", "inputs": ["***"]})"}}),
        "member 'name'");
}

TEST(TableQueryAuthResultTest, ParsesTemporalLiteralsAsJava) {
    auto date = [](const std::string& literal) -> Result<int32_t> {
        PAIMON_ASSIGN_OR_RAISE(Literal parsed, FilterLiteral(FieldRefJson(0, "d", "DATE"), "EQUAL",
                                                             fmt::format("[{}]", literal)));
        return parsed.GetValue<int32_t>();
    };
    for (const auto& [text, epoch_day] :
         std::vector<std::pair<std::string, int32_t>>{{"1970-01-02", 1},
                                                      {"1969-12-31", -1},
                                                      {"2024-02-29", 19782},
                                                      {"-0001-01-01", -719893},
                                                      {"+10000-01-01", 2932897}}) {
        ASSERT_OK_AND_ASSIGN(int32_t parsed, date(fmt::format(R"("{}")", text)));
        ASSERT_EQ(epoch_day, parsed) << text;
    }
    for (const std::string text :
         {"123", "2024-01-01junk", "2024-01-01 ", " 2024-01-01", "2023-02-29", "2024-13-01",
          "2024-1-01", "+2024-01-01", "10000-01-01", "-0000-01-01"}) {
        ASSERT_NOK_WITH_MSG(date(fmt::format(R"("{}")", text)),
                            fmt::format("Cannot parse '{}' as a date literal", text));
    }
    ASSERT_NOK_WITH_MSG(date("19782"), "does not fit column 'd' of type date32");

    auto timestamp = [](const std::string& type, const std::string& text) -> Result<Timestamp> {
        PAIMON_ASSIGN_OR_RAISE(Literal literal,
                               FilterLiteral(FieldRefJson(0, "ts", type), "GREATER_THAN",
                                             fmt::format(R"(["{}"])", text)));
        return literal.GetValue<Timestamp>();
    };
    const std::string local = "TIMESTAMP(9)";
    const std::string instant = "TIMESTAMP(9) WITH LOCAL TIME ZONE";
    for (const auto& [type, text, millis, nano_of_millis] :
         std::vector<std::tuple<std::string, std::string, int64_t, int32_t>>{
             {local, "2263-01-01T00:00:00", 9246182400000LL, 0},
             {local, "1600-01-01T00:00:00", -11676096000000LL, 0},
             {local, "1969-12-31T23:59:59.5", -500, 0},
             {local, "1970-01-01T00:00:00.123456789", 123, 456789},
             {local, "1970-01-01T00:01", 60000, 0},
             {"TIMESTAMP(6)", "1970-01-01T00:00:01.000001", 1000, 1000},
             {local, "9999-12-31T23:59:59.999999999", 253402300799999LL, 999999},
             {local, "+10000-01-01T00:00", 253402300800000LL, 0},
             {local, "+292278994-08-17T07:12:55.807999999", std::numeric_limits<int64_t>::max(),
              999999},
             {local, "-292275055-05-16T16:47:04.192", std::numeric_limits<int64_t>::min(), 0},
             {instant, "1969-12-31T23:59:59.5Z", -500, 0},
             {instant, "+10000-01-01T00:00:00Z", 253402300800000LL, 0},
             {instant, "+292278994-08-17T07:12:55.807Z", std::numeric_limits<int64_t>::max(), 0},
             {instant, "-292275055-05-16T16:47:04.192999999Z", std::numeric_limits<int64_t>::min(),
              999999}}) {
        ASSERT_OK_AND_ASSIGN(Timestamp parsed, timestamp(type, text));
        ASSERT_EQ(millis, parsed.GetMillisecond()) << text;
        ASSERT_EQ(nano_of_millis, parsed.GetNanoOfMillisecond()) << text;
    }
    for (const auto& [type, text] : std::vector<std::pair<std::string, std::string>>{
             {local, "1970-01-01.5"},
             {local, "1970-01-01T00:00.5"},
             {local, "1970-01-01"},
             {local, "1970-01-01T24:00:00"},
             {local, "1970-01-01T00:00:00Z"},
             {local, "1970-01-01T00:00:00."},
             {local, "1970-01-01T00:00:00.1234567890"},
             {local, "1970-01-01T00:00:00." + std::string(64, '9')},
             {instant, "1970-01-01T00:00:01"},
             {instant, "1970-01-01T00:01Z"},
             {instant, "1970-01-01T00:00:01+08:00"}}) {
        ASSERT_NOK_WITH_MSG(timestamp(type, text),
                            fmt::format("Cannot parse '{}' as a timestamp literal", text));
    }
    ASSERT_NOK_WITH_MSG(timestamp(local, "+292278995-01-01T00:00:00"),
                        "is outside the range of epoch milliseconds");
    for (const std::string& type : {local, instant}) {
        const std::string suffix = type == instant ? "Z" : "";
        for (const std::string text :
             {"+292278994-08-17T07:12:55.808", "-292275055-05-16T16:47:04.191999999"}) {
            ASSERT_NOK_WITH_MSG(timestamp(type, text + suffix),
                                "is outside the range of epoch milliseconds");
        }
    }

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableQueryAuthResult> rule,
        TableQueryAuthResult::Create({LeafJson(FieldRefJson(0, "ts", "TIMESTAMP(3)"),
                                               "GREATER_THAN", R"(["2263-01-01T00:00:00"])")},
                                     {}));
    auto batch = checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MILLI))}),
            "[[1767225600000], [9246182400001]]")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(std::vector<char> selected, Evaluate(rule->RowFilter(), batch));
    ASSERT_EQ(std::vector<char>({0, 1}), selected);
}

TEST(TableQueryAuthResultTest, RefusesWhatItCannotApply) {
    Status unknown_function =
        TableQueryAuthResult::Create({LeafJson(NameRef(), "NOT_LIKE", R"(["a%"])")}, {}).status();
    ASSERT_TRUE(unknown_function.IsNotImplemented()) << unknown_function.ToString();
    Status unknown_kind =
        TableQueryAuthResult::Create({R"({"kind": "OTHER", "function": "AND"})"}, {}).status();
    ASSERT_TRUE(unknown_kind.IsNotImplemented()) << unknown_kind.ToString();
    Status computed_filter =
        TableQueryAuthResult::Create(
            {R"({"kind": "LEAF", "transform": {"name": "UPPER", "inputs": []},)"
             R"( "function": "EQUAL", "literals": ["A"]})"},
            {})
            .status();
    ASSERT_TRUE(computed_filter.IsNotImplemented()) << computed_filter.ToString();
    Status unknown_mask =
        TableQueryAuthResult::Create(
            {}, {{"name", fmt::format(R"({{"name": "UPPER", "inputs": [{}]}})", NameRef())}})
            .status();
    ASSERT_TRUE(unknown_mask.IsNotImplemented()) << unknown_mask.ToString();

    ASSERT_NOK_WITH_MSG(TableQueryAuthResult::Create({""}, {}), "Row filter cannot be empty.");
    ASSERT_NOK_WITH_MSG(TableQueryAuthResult::Create({"null"}, {}),
                        "Row filter cannot be JSON null.");
    ASSERT_NOK_WITH_MSG(TableQueryAuthResult::Create({}, {{"", R"({"name": "NULL"})"}}),
                        "Column mask target cannot be empty.");
    ASSERT_NOK_WITH_MSG(TableQueryAuthResult::Create({}, {{"name", ""}}),
                        "Column mask transform cannot be empty.");
    ASSERT_NOK_WITH_MSG(TableQueryAuthResult::Create({}, {{"name", "null"}}),
                        "Column mask transform cannot be JSON null.");
    ASSERT_NOK_WITH_MSG(
        TableQueryAuthResult::Create({LeafJson(FieldRefJson(0, "id", "INT"), "IS_NULL", "[]"),
                                      LeafJson(FieldRefJson(0, "id", "BIGINT"), "IS_NULL", "[]")},
                                     {}),
        "read column 'id' as both");
    ASSERT_NOK_WITH_MSG(
        TableQueryAuthResult::Create(
            {LeafJson(R"({"index": 0, "name": "f", "type": {"type": 123}})", "IS_NULL", "[]")}, {}),
        "complex data type must have a string type");
    ASSERT_NOK_WITH_MSG(
        TableQueryAuthResult::Create(
            {LeafJson(R"({"index": 0, "name": "f", "type": {"type": "ARRAY", "element": 5}})",
                      "IS_NULL", "[]")},
            {}),
        "cannot parse data type");
    for (const char* inputs : {"[]", R"(["-"])"}) {
        ASSERT_NOK_WITH_MSG(
            TableQueryAuthResult::Create(
                {}, {{"name", fmt::format(R"({{"name": "CONCAT_WS", "inputs": {}}})", inputs)}}),
            "A CONCAT_WS transform needs a separator and at least one input.");
    }
    for (const std::string function : {"IN", "NOT_IN"}) {
        const std::string missing_literals =
            fmt::format(R"({{"kind": "LEAF", "transform": {{"name": "FIELD_REF", "fieldRef": {}}},)"
                        R"( "function": "{}"}})",
                        NameRef(), function);
        for (const std::string& filter :
             {missing_literals, LeafJson(NameRef(), function, "null")}) {
            ASSERT_NOK_WITH_MSG(TableQueryAuthResult::Create({filter}, {}),
                                fmt::format("Invalid: Query auth row filter {} requires a "
                                            "non-null 'literals' array.",
                                            function));
        }
    }
}

TEST(TableQueryAuthResultTest, AppliesColumnMasks) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableQueryAuthResult> result,
        TableQueryAuthResult::Create(
            {},
            {{"salary", R"({"name": "NULL"})"},
             {"name", fmt::format(R"({{"name": "FIELD_REF", "fieldRef": {}}})", PhoneRef())},
             {"phone", fmt::format(R"({{"name": "SUBSTRING", "inputs": [{}, 1, 3]}})", PhoneRef())},
             {"concat", fmt::format(R"({{"name": "CONCAT", "inputs": [{}, "-", {}]}})", NameRef(),
                                    PhoneRef())},
             {"concat_ws", fmt::format(R"({{"name": "CONCAT_WS", "inputs": ["-", {}, null, )"
                                       R"("x", {}]}})",
                                       NameRef(), PhoneRef())},
             {"null_separator",
              fmt::format(R"({{"name": "CONCAT_WS", "inputs": [null, {}]}})", NameRef())},
             {"empty", R"({"name": "CONCAT_WS", "inputs": ["-", ""]})"},
             {"tail", fmt::format(R"({{"name": "SUBSTRING", "inputs": [{}, -4]}})", PhoneRef())},
             {"utf8", R"({"name": "SUBSTRING", "inputs": ["héllo", 2, 3]})"},
             {"null_position",
              fmt::format(R"({{"name": "SUBSTRING", "inputs": [{}, null]}})", PhoneRef())}}));
    ASSERT_TRUE(result->HasRules());
    ASSERT_EQ(nullptr, result->RowFilter());
    auto batch = MakeBatch(R"([[1, "bob", "13900002222", 10], [2, null, null, 20]])");

    auto assert_mask = [&](const std::string& column, const std::shared_ptr<arrow::DataType>& type,
                           const std::shared_ptr<arrow::Array>& expected) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> masked,
                             Mask(*result, column, batch, type));
        ASSERT_TRUE(masked->Equals(*expected))
            << column << ": " << masked->ToString() << " vs " << expected->ToString();
    };
    assert_mask(
        "salary", arrow::int64(),
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), "[null, null]").ValueOrDie());
    assert_mask("name", arrow::utf8(), Strings(R"(["13900002222", null])"));
    assert_mask("phone", arrow::utf8(), Strings(R"(["139", null])"));
    assert_mask("concat", arrow::utf8(), Strings(R"(["bob-13900002222", null])"));
    assert_mask("concat_ws", arrow::utf8(), Strings(R"(["bob-x-13900002222", "x"])"));
    assert_mask("null_separator", arrow::utf8(), Strings("[null, null]"));
    assert_mask("empty", arrow::utf8(), Strings(R"(["", ""])"));
    assert_mask("tail", arrow::utf8(), Strings(R"(["2222", null])"));
    assert_mask("utf8", arrow::utf8(), Strings("[\"\xC3\xA9ll\", \"\xC3\xA9ll\"]"));
    assert_mask("null_position", arrow::utf8(), Strings("[null, null]"));
}

TEST(TableQueryAuthResultTest, SubstringReadsPositionsAsJava) {
    const auto type =
        arrow::struct_({arrow::field("phone", arrow::utf8()), arrow::field("pos", arrow::int64()),
                        arrow::field("len", arrow::int64())});
    auto batch = checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(type, R"([["abc", 9223372036854775807, null],
                       ["abc", null, 9223372036854775807],
                       [null, 9223372036854775807, 9223372036854775807],
                       ["abc", 2, 1]])")
            .ValueOrDie());
    for (const auto& [pos, len, expected] :
         std::vector<std::tuple<std::string, std::string, std::string>>{
             {FieldRefJson(1, "pos", "BIGINT"), FieldRefJson(2, "len", "BIGINT"),
              R"([null, null, null, "b"])"},
             {"null", FieldRefJson(1, "pos", "BIGINT"), "[null, null, null, null]"},
             {FieldRefJson(2, "len", "BIGINT"), "null", "[null, null, null, null]"}}) {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableQueryAuthResult> result,
            TableQueryAuthResult::Create(
                {}, {{"phone", fmt::format(R"({{"name": "SUBSTRING", "inputs": [{}, {}, {}]}})",
                                           FieldRefJson(0, "phone", "STRING"), pos, len)}}));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> masked,
                             Mask(*result, "phone", batch, arrow::utf8()));
        ASSERT_TRUE(masked->Equals(*Strings(expected))) << masked->ToString();
    }

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableQueryAuthResult> result,
        TableQueryAuthResult::Create(
            {},
            {{"phone", fmt::format(R"({{"name": "SUBSTRING", "inputs": [{}, {}, 3]}})", PhoneRef(),
                                   FieldRefJson(1, "secret_position", "BIGINT"))}}));
    auto secret_batch = checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({arrow::field("phone", arrow::utf8()),
                            arrow::field("secret_position", arrow::int64())}),
            R"([["13900001111", 9223372036854775807]])")
            .ValueOrDie());
    const Status status = Mask(*result, "phone", secret_batch, arrow::utf8()).status();
    ASSERT_TRUE(status.IsInvalid()) << status.ToString();
    ASSERT_NOK_WITH_MSG(status, "SUBSTRING position of column 'secret_position' does not fit");
    ASSERT_EQ(std::string::npos, status.message().find("9223372036854775807"));
}

TEST(TableQueryAuthResultTest, CopiesATimeColumnThatNoFilterCanCompare) {
    const std::string src_ref = FieldRefJson(0, "src", "TIME(3)");
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableQueryAuthResult> result,
        TableQueryAuthResult::Create(
            {}, {{"dst", fmt::format(R"({{"name": "FIELD_REF", "fieldRef": {}}})", src_ref)}}));
    const auto time_type = arrow::time32(arrow::TimeUnit::MILLI);
    ASSERT_OK(result->ValidateAgainstSchema({DataField(0, arrow::field("src", time_type)),
                                             DataField(1, arrow::field("dst", time_type))}));
    ASSERT_NOK_WITH_MSG(
        result->ValidateAgainstSchema({DataField(0, arrow::field("src", time_type)),
                                       DataField(1, arrow::field("dst", arrow::int32()))}),
        "copies column 'src' of type time32[ms] into a column of type int32");
    auto batch = checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({arrow::field("src", time_type), arrow::field("dst", time_type)}),
            "[[1000, 5], [null, 6]]")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> masked,
                         Mask(*result, "dst", batch, time_type));
    ASSERT_TRUE(masked->Equals(
        *arrow::ipc::internal::json::ArrayFromJSON(time_type, "[1000, null]").ValueOrDie()))
        << masked->ToString();

    Status filter = TableQueryAuthResult::Create({LeafJson(src_ref, "IS_NULL", "[]")}, {}).status();
    ASSERT_TRUE(filter.IsNotImplemented()) << filter.ToString();
    ASSERT_NOK_WITH_MSG(filter, "Query auth row filter IS_NULL on column 'src' of type time32[ms]");
}

TEST(TableQueryAuthResultTest, ValidatesTheRulesAgainstTheSchema) {
    const std::vector<DataField> fields = TableFields();
    auto validate = [&](const std::vector<std::string>& filter,
                        const std::map<std::string, std::string>& masks) -> Status {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableQueryAuthResult> result,
                               TableQueryAuthResult::Create(filter, masks));
        return result->ValidateAgainstSchema(fields);
    };
    ASSERT_OK(validate(
        {LeafJson(IdRef(), "GREATER_THAN", "[1]")},
        {{"salary", R"({"name": "NULL"})"},
         {"phone", fmt::format(R"({{"name": "SUBSTRING", "inputs": [{}, 1, 3]}})", PhoneRef())}}));

    ASSERT_NOK_WITH_MSG(validate({LeafJson(FieldRefJson(9, "gone", "INT"), "IS_NULL", "[]")}, {}),
                        "Row filter references column 'gone' which does not exist");
    const std::string gone_is_1 = LeafJson(FieldRefJson(9, "gone", "INT"), "EQUAL", "[1]");
    for (const std::string& folded : {CompoundJson("OR", ConstantJson("TRUE"), gone_is_1),
                                      CompoundJson("AND", ConstantJson("FALSE"), gone_is_1),
                                      LeafJson(FieldRefJson(9, "gone", "INT"), "TRUE", "[]")}) {
        ASSERT_NOK_WITH_MSG(validate({folded}, {}),
                            "A query auth rule references column 'gone' which does not exist");
    }
    ASSERT_NOK_WITH_MSG(validate({}, {{"gone", R"({"name": "NULL"})"}}),
                        "Column masking references column 'gone' which does not exist");
    ASSERT_NOK_WITH_MSG(validate({LeafJson(FieldRefJson(3, "salary", "INT"), "IS_NULL", "[]")}, {}),
                        "read column 'salary' as int32, but the table holds it as int64");
    ASSERT_NOK_WITH_MSG(
        validate({},
                 {{"salary", fmt::format(R"({{"name": "CONCAT", "inputs": [{}]}})", NameRef())}}),
        "produces a string, but column 'salary' is int64");
    ASSERT_NOK_WITH_MSG(
        validate({},
                 {{"salary", fmt::format(R"({{"name": "FIELD_REF", "fieldRef": {}}})", IdRef())}}),
        "copies column 'id' of type int32 into a column of type int64");
    ASSERT_NOK_WITH_MSG(
        validate({},
                 {{"name", R"({"name": "NULL"})"},
                  {"phone", fmt::format(R"({{"name": "CONCAT", "inputs": [{}]}})", NameRef())}}),
        "Column masking on 'phone' reads column 'name', which is masked too");
}

TEST(TableQueryAuthResultTest, LeavesMasksOfSystemColumnsInert) {
    const std::string row_id_ref = FieldRefJson(-1, "_ROW_ID", "BIGINT NOT NULL");
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableQueryAuthResult> system_mask,
        TableQueryAuthResult::Create(
            {}, {{"_ROW_ID", fmt::format(R"({{"name": "FIELD_REF", "fieldRef": {}}})", row_id_ref)},
                 {"rowkind", R"({"name": "NULL"})"},
                 {"name", R"({"name": "NULL"})"}}));
    ASSERT_EQ(std::set<std::string>{"name"}, system_mask->MaskedFields());
    ASSERT_EQ(std::set<std::string>{"name"}, system_mask->ReferencedFields());
    ASSERT_EQ(3u, system_mask->ColumnMasking().size());
    ASSERT_OK(system_mask->ValidateAgainstSchema(TableFields()));
    Status unknown_system_mask =
        TableQueryAuthResult::Create({}, {{"_ROW_ID", R"({"name": "UPPER", "inputs": []})"}})
            .status();
    ASSERT_TRUE(unknown_system_mask.IsNotImplemented()) << unknown_system_mask.ToString();

    for (const std::string name :
         {"_SEQUENCE_NUMBER", "_VALUE_KIND", "_LEVEL", "rowkind", "_ROW_ID"}) {
        SCOPED_TRACE(name);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableQueryAuthResult> rules,
                             TableQueryAuthResult::Create({}, {{name, R"({"name": "NULL"})"}}));
        ASSERT_OK(rules->ValidateAgainstSchema(TableFields()));
        ASSERT_NOK_WITH_MSG(
            rules->ValidateAgainstSchema({DataField(0, arrow::field(name, arrow::utf8()))}),
            "refusing to ignore its mask");
    }
}

TEST(TableQueryAuthResultTest, RefusesAColumnRenamedOrReplacedSinceTheRulesWereBound) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableQueryAuthResult> result,
                         TableQueryAuthResult::Create({LeafJson(NameRef(), "IS_NULL", "[]")},
                                                      {{"phone", R"({"name": "NULL"})"}}));
    const std::vector<DataField> latest = TableFields();
    const std::map<std::string, int32_t> field_ids = result->BindFieldIds(latest);
    ASSERT_EQ((std::map<std::string, int32_t>{{"name", 1}, {"phone", 2}}), field_ids);
    ASSERT_OK(result->ValidateFieldIds(field_ids, latest));

    std::vector<DataField> renamed = latest;
    renamed[1] = DataField(1, arrow::field("old_name", arrow::utf8()));
    ASSERT_NOK_WITH_MSG(result->ValidateFieldIds(field_ids, renamed),
                        "exposes as 'old_name' (renamed since)");

    std::vector<DataField> replaced = latest;
    replaced[1] = DataField(7, arrow::field("name", arrow::utf8()));
    ASSERT_NOK_WITH_MSG(result->ValidateFieldIds(field_ids, replaced),
                        "as a different column of the same name");

    std::vector<DataField> swapped = latest;
    swapped[1] = DataField(1, arrow::field("phone", arrow::utf8()));
    swapped[2] = DataField(2, arrow::field("name", arrow::utf8()));
    ASSERT_OK(result->ValidateAgainstSchema(swapped));
    ASSERT_NOK_WITH_MSG(result->ValidateFieldIds(field_ids, swapped),
                        "as a different column of the same name");

    std::map<std::string, int32_t> incomplete = field_ids;
    incomplete.erase("phone");
    ASSERT_NOK_WITH_MSG(result->ValidateFieldIds(incomplete, latest),
                        "Query auth rules reference column 'phone', which the schema the scan "
                        "planned with did not hold");
    ASSERT_EQ((std::set<std::string>{"name", "phone"}), result->ReferencedFields());

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableQueryAuthResult> folded,
        TableQueryAuthResult::Create(
            {CompoundJson("OR", ConstantJson("TRUE"), LeafJson(NameRef(), "IS_NULL", "[]"))}, {}));
    ASSERT_EQ(nullptr, folded->RowFilter());
    ASSERT_EQ((std::set<std::string>{"name"}), folded->ReferencedFields());
    const std::map<std::string, int32_t> folded_ids = folded->BindFieldIds(latest);
    ASSERT_EQ((std::map<std::string, int32_t>{{"name", 1}}), folded_ids);
    ASSERT_NOK_WITH_MSG(folded->ValidateFieldIds(folded_ids, renamed),
                        "exposes as 'old_name' (renamed since)");
    ASSERT_NOK_WITH_MSG(folded->ValidateFieldIds(folded_ids, replaced),
                        "as a different column of the same name");
}

TEST(TableQueryAuthResultTest, RequiredFieldsFollowTheMasksTransitively) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableQueryAuthResult> result,
        TableQueryAuthResult::Create(
            {LeafJson(IdRef(), "GREATER_THAN", "[1]")},
            {{"phone", fmt::format(R"({{"name": "FIELD_REF", "fieldRef": {}}})", NameRef())},
             {"salary", R"({"name": "NULL"})"}}));
    ASSERT_EQ(std::set<std::string>({"id", "name"}), result->RequiredFields({"phone"}));
    ASSERT_EQ(std::set<std::string>({"id"}), result->RequiredFields({"salary"}));
    ASSERT_EQ(std::set<std::string>({"phone", "salary"}), result->MaskedFields());
}

TEST(TableQueryAuthResultTest, SameFilterComparesTheParsedConjuncts) {
    const std::string first = LeafJson(IdRef(), "GREATER_THAN", "[1]");
    const std::string second = LeafJson(NameRef(), "IS_NULL", "[]");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableQueryAuthResult> result,
                         TableQueryAuthResult::Create({first, second}, {}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableQueryAuthResult> reordered,
                         TableQueryAuthResult::Create({second, first}, {}));
    ASSERT_TRUE(result->SameFilter(*reordered));
    const std::string reserialized =
        fmt::format(R"({{"literals":[1],"function":"GREATER_THAN","kind":"LEAF",)"
                    R"("transform":{{"fieldRef":{},"name":"FIELD_REF"}}}})",
                    IdRef());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableQueryAuthResult> compound,
                         TableQueryAuthResult::Create({CompoundJson("AND", second, reserialized)},
                                                      {{"name", R"({"name": "NULL"})"}}));
    ASSERT_TRUE(result->SameFilter(*compound));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableQueryAuthResult> fewer,
                         TableQueryAuthResult::Create({first}, {}));
    ASSERT_FALSE(result->SameFilter(*fewer));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableQueryAuthResult> other,
        TableQueryAuthResult::Create({first, LeafJson(NameRef(), "IS_NOT_NULL", "[]")}, {}));
    ASSERT_FALSE(result->SameFilter(*other));
}

}  // namespace paimon::test
