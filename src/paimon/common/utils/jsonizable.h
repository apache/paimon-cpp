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

#include <string>
#include <utility>

#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon {

#define JSONIZABLE_FRIEND_AND_DEFAULT_CTOR(Type) \
    friend class RapidJsonUtil;                  \
    friend class Jsonizable;                     \
    Type() = default;

template <typename Derived>
class Jsonizable {
 public:
    Jsonizable() = default;
    virtual ~Jsonizable() = default;

    virtual rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) = 0;
    virtual void FromJson(const rapidjson::Value& obj) noexcept(false) = 0;

    Result<std::string> ToJsonString() const {
        std::string json_str;
        PAIMON_RETURN_NOT_OK(RapidJsonUtil::ToJsonString(*this, &json_str));
        return json_str;
    }
    static Result<Derived> FromJsonString(const std::string& json_str) {
        Derived obj;
        PAIMON_RETURN_NOT_OK(RapidJsonUtil::FromJsonString(json_str, &obj));
        return obj;
    }
};

}  // namespace paimon
