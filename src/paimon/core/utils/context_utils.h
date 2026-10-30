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

#pragma once

#include "fmt/format.h"

#include <map>
#include <string>

#include "paimon/common/utils/sensitive_config_utils.h"
#include "paimon/status.h"

namespace paimon {

/// The checks every kind of table's operator entry points share.
class ContextUtils {
 public:
    ContextUtils() = delete;
    ~ContextUtils() = delete;

    /// The checks every operator entry point opens with: a context, a memory pool and an executor
    /// must all be there for the operator to be built at all, so a null one is refused rather than
    /// dereferenced. `kind` names the operator the first message speaks of, which is all that sets
    /// the entry points' messages apart.
    template <typename ContextPointer>
    static Status CheckOperatorContext(const ContextPointer& context, const char* kind) {
        if (context == nullptr) {
            return Status::Invalid(fmt::format("{} context is null pointer", kind));
        }
        if (context->GetMemoryPool() == nullptr) {
            return Status::Invalid("memory pool is null pointer");
        }
        if (context->GetExecutor() == nullptr) {
            return Status::Invalid("executor is null pointer");
        }
        return Status::OK();
    }

    /// An operator created from a table serves that table's own configuration and adds nothing
    /// from the context a caller hands it, so a context carrying any option at all is refused:
    /// dynamic configuration belongs to `Table::Copy()`, and a branch is named through
    /// `WithBranch()` or the identifier of `WithCatalog()`, never through an option here. The
    /// one-step `Create()` entry points fold the options they are given into the table they load
    /// and clear them, so their contexts arrive empty. `kind` names the operator the message
    /// speaks of.
    static Status CheckContextCarriesNoOptions(const std::string& kind,
                                              const std::map<std::string, std::string>& options) {
        if (options.empty()) {
            return Status::OK();
        }
        const auto& [key, value] = *options.begin();
        return Status::Invalid(fmt::format(
            "the {} context carries option '{}' as '{}', but an operator created from a table "
            "serves that table's own configuration and adds nothing from the context: apply "
            "dynamic configuration through Table::Copy()",
            kind, key, SensitiveConfigUtils::RedactValue(key, value)));
    }
};

}  // namespace paimon
