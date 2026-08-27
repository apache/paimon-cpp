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

#include <cxxabi.h>

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <optional>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class OptionsUtils {
 public:
    template <typename T>
    using is_supported_type =
        std::disjunction<std::is_trivially_copyable<T>, std::is_same<T, std::string>>;

    OptionsUtils() = delete;
    ~OptionsUtils() = delete;

    template <typename T>
    static Result<T> GetValueFromMap(const std::map<std::string, std::string>& key_value_map,
                                     const std::string& key, const T& default_value) {
        auto value = GetValueFromMap<T>(key_value_map, key);
        if (value.ok()) {
            return value.value();
        } else if (value.status().IsNotExist()) {
            return default_value;
        }
        return value.status();
    }

    template <typename T>
    static Result<T> GetValueFromMap(const std::map<std::string, std::string>& key_value_map,
                                     const std::string& key) {
        static_assert(is_supported_type<T>::value, "T must be trivially copyable or string");
        auto iter = key_value_map.find(key);
        if (iter == key_value_map.end()) {
            return Status::NotExist(fmt::format("key {} does not exist in map", key));
        }
        const auto& value_str = iter->second;
        std::optional<T> value = StringUtils::StringToValue<T>(value_str);
        if (value == std::nullopt) {
            return Status::Invalid(fmt::format("convert key {}, value {} to {} failed", key,
                                               value_str, GetTypeName<T>()));
        }
        return value.value();
    }

    template <typename T>
    static Result<std::optional<T>> GetOptionalValueFromMap(
        const std::map<std::string, std::string>& key_value_map, const std::string& key) {
        Result<T> value = GetValueFromMap<T>(key_value_map, key);
        if (value.ok()) {
            return std::optional<T>(value.value());
        }
        if (value.status().IsNotExist()) {
            return std::optional<T>();
        }
        return value.status();
    }

    static Result<std::string> GetNonEmptyValueFromMap(
        const std::map<std::string, std::string>& key_value_map, const std::string& key) {
        Result<std::string> value = GetValueFromMap<std::string>(key_value_map, key);
        if (!value.ok()) {
            return value.status();
        }
        if (value.value().empty()) {
            return Status::Invalid(fmt::format("value for key {} must not be empty", key));
        }
        return value.value();
    }

    /// Fetch options with specific prefix and remove prefix for key.
    /// @param prefix Prefix used to select options and removed from the returned keys.
    /// @param options Options to select from.
    /// @return Options whose keys start with and are longer than `prefix`, with the prefix removed
    /// from each key.
    static std::map<std::string, std::string> FetchOptionsWithPrefix(
        const std::string& prefix, const std::map<std::string, std::string>& options) {
        std::map<std::string, std::string> options_with_prefix;
        const std::string::size_type prefix_len = prefix.size();
        for (const auto& [key, value] : options) {
            if (key.size() > prefix_len && StringUtils::StartsWith(key, prefix)) {
                options_with_prefix[key.substr(prefix_len)] = value;
            }
        }
        return options_with_prefix;
    }

    template <typename T>
    static std::string GetTypeName() {
        int32_t status;
        char* demangled = abi::__cxa_demangle(typeid(T).name(), nullptr, nullptr, &status);
        if (status == 0) {
            std::string result(demangled);
            free(demangled);
            return result;
        }
        assert(demangled == nullptr);
        return typeid(T).name();
    }
};
}  // namespace paimon
