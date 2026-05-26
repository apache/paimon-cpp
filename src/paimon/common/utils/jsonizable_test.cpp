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

#include "paimon/common/utils/jsonizable.h"

#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon::test {

TEST(JsonizableTest, TestNestedClass) {
    class ClassA : public Jsonizable<ClassA> {
     public:
        bool operator==(const ClassA& other) const {
            return vec_ == other.vec_ && string_ == other.string_ && map_ == other.map_;
        }
        rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
            noexcept(false) override {
            rapidjson::Value value(rapidjson::kObjectType);
            value.AddMember("vec", RapidJsonUtil::SerializeValue(vec_, allocator).Move(),
                            *allocator);
            value.AddMember("string", RapidJsonUtil::SerializeValue(string_, allocator).Move(),
                            *allocator);
            value.AddMember("map_a", RapidJsonUtil::SerializeValue(map_, allocator).Move(),
                            *allocator);
            return value;
        }
        void FromJson(const rapidjson::Value& value) noexcept(false) override {
            vec_ = RapidJsonUtil::DeserializeKeyValue<std::vector<double>>(value, "vec", vec_);
            string_ = RapidJsonUtil::DeserializeKeyValue<std::string>(value, "string", string_);
            map_ = RapidJsonUtil::DeserializeKeyValue<std::map<std::string, std::string>>(
                value, "map_a", map_);
        }

     private:
        JSONIZABLE_FRIEND_AND_DEFAULT_CTOR(ClassA);

        std::vector<double> vec_;
        std::string string_;
        std::map<std::string, std::string> map_;
    };

    class ClassB : public Jsonizable<ClassB> {
     public:
        bool operator==(const ClassB& other) const {
            return a_ == other.a_ && a_vec_ == other.a_vec_ && f_ == other.f_ && map_ == other.map_;
        }
        rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
            noexcept(false) override {
            rapidjson::Value obj(rapidjson::kObjectType);
            obj.AddMember("ClassA", RapidJsonUtil::SerializeValue(a_, allocator).Move(),
                          *allocator);
            obj.AddMember("ClassA_vec", RapidJsonUtil::SerializeValue(a_vec_, allocator).Move(),
                          *allocator);
            obj.AddMember("float", RapidJsonUtil::SerializeValue(f_, allocator).Move(), *allocator);
            obj.AddMember("map_b", RapidJsonUtil::SerializeValue(map_, allocator).Move(),
                          *allocator);
            return obj;
        }
        void FromJson(const rapidjson::Value& obj) noexcept(false) override {
            a_ = RapidJsonUtil::DeserializeKeyValue<ClassA>(obj, "ClassA", a_);
            a_vec_ =
                RapidJsonUtil::DeserializeKeyValue<std::vector<ClassA>>(obj, "ClassA_vec", a_vec_);
            f_ = RapidJsonUtil::DeserializeKeyValue<float>(obj, "float", f_);
            map_ = RapidJsonUtil::DeserializeKeyValue<std::map<std::string, std::vector<int>>>(
                obj, "map_b", map_);
        }

     private:
        JSONIZABLE_FRIEND_AND_DEFAULT_CTOR(ClassB);

        ClassA a_;
        std::vector<ClassA> a_vec_;
        float f_;
        std::map<std::string, std::vector<int>> map_;
    };

    ClassA obj_a1, obj_a2;
    obj_a1.vec_ = {11.0, 12.0, 13.0, 14.0};
    obj_a1.string_ = "string_value_1";
    obj_a1.map_ = {{"10", "a1"}, {"11", "b1"}, {"12", "c1"}};

    obj_a2.vec_ = {21.0, 22.0, 23.0, 24.0};
    obj_a2.string_ = "string_value_2";
    obj_a2.map_ = {{"20", "a2"}, {"21", "b2"}, {"22", "c2"}};

    ClassB obj_b;
    obj_b.a_.vec_ = {1.0, 2.0, 3.0, 4.0};
    obj_b.a_.string_ = "string_value";
    obj_b.a_.map_ = {{"0", "a"}, {"1", "b"}, {"2", "c"}};

    obj_b.a_vec_.push_back(obj_a1);
    obj_b.a_vec_.push_back(obj_a2);
    obj_b.f_ = 10.5;
    obj_b.map_ = {{"aa", {0, 1}}, {"bb", {1, 2}}, {"cc", {2, 3}}};

    ASSERT_OK_AND_ASSIGN(std::string json_str, obj_b.ToJsonString());
    ASSERT_OK_AND_ASSIGN(ClassB obj_b_2, ClassB::FromJsonString(json_str));
    ASSERT_EQ(obj_b, obj_b_2);

    // test invalid json_str
    auto invalid_json_str = json_str.substr(0, json_str.length() / 2);
    ASSERT_NOK_WITH_MSG(ClassB::FromJsonString(invalid_json_str), "deserialize failed");
}

TEST(JsonizableTest, TestUpgradeClass) {
    class ClassA : public Jsonizable<ClassA> {
     public:
        bool operator==(const ClassA& other) const {
            return vec_ == other.vec_ && string_ == other.string_;
        }

        rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
            noexcept(false) override {
            rapidjson::Value value(rapidjson::kObjectType);
            value.AddMember("vec", RapidJsonUtil::SerializeValue(vec_, allocator).Move(),
                            *allocator);
            value.AddMember("string", RapidJsonUtil::SerializeValue(string_, allocator).Move(),
                            *allocator);
            return value;
        }
        void FromJson(const rapidjson::Value& value) noexcept(false) override {
            vec_ = RapidJsonUtil::DeserializeKeyValue<std::vector<double>>(value, "vec", vec_);
            string_ = RapidJsonUtil::DeserializeKeyValue<std::string>(value, "string", string_);
        }

     private:
        JSONIZABLE_FRIEND_AND_DEFAULT_CTOR(ClassA);

        std::vector<double> vec_;
        std::string string_;
    };

    // modify vec_ from vector<double> to vector<string>
    class NewClassA : public Jsonizable<NewClassA> {
     public:
        rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
            noexcept(false) override {
            rapidjson::Value value(rapidjson::kObjectType);
            value.AddMember("vec", RapidJsonUtil::SerializeValue(vec_, allocator).Move(),
                            *allocator);
            value.AddMember("string", RapidJsonUtil::SerializeValue(string_, allocator).Move(),
                            *allocator);
            return value;
        }
        void FromJson(const rapidjson::Value& value) noexcept(false) override {
            vec_ = RapidJsonUtil::DeserializeKeyValue<std::vector<std::string>>(value, "vec", vec_);
            string_ = RapidJsonUtil::DeserializeKeyValue<std::string>(value, "string", string_);
        }

     private:
        JSONIZABLE_FRIEND_AND_DEFAULT_CTOR(NewClassA);

        std::vector<std::string> vec_;
        std::string string_;
    };

    ClassA obj_a;
    obj_a.vec_ = {1, 2, 3};
    obj_a.string_ = "abcd";

    ASSERT_OK_AND_ASSIGN(std::string json_str, obj_a.ToJsonString());
    ASSERT_OK_AND_ASSIGN(ClassA obj_a_2, ClassA::FromJsonString(json_str));
    ASSERT_EQ(obj_a, obj_a_2);

    // test serialize with ClassA and deserialize with NewClassA
    ASSERT_NOK_WITH_MSG(NewClassA::FromJsonString(json_str), "value must be string");
}

}  // namespace paimon::test
