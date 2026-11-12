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
#include <map>
#include <memory>

#include "paimon/core/casting/cast_executor.h"
#include "paimon/defs.h"
#include "paimon/result.h"

namespace paimon {
class DataField;
enum class FieldType;

class CastExecutorFactory {
 public:
    static CastExecutorFactory* GetCastExecutorFactory();

    std::shared_ptr<CastExecutor> GetCastExecutor(const FieldType& src,
                                                  const FieldType& target) const;

    /// Returns the executor casting `src_field` to `target`, or nullptr if there is none. Like
    /// Java `TimeToStringCastRule`, a TIME field is cast to STRING with the precision it declares.
    /// Like Java `TimestampToTimeCastRule`, a TIMESTAMP_LTZ field is cast to TIME in the local
    /// time zone, also for a literal.
    Result<std::shared_ptr<CastExecutor>> GetCastExecutor(const DataField& src_field,
                                                          const FieldType& target) const;

 private:
    CastExecutorFactory();

 private:
    // {target type: {src type: cast executor}}
    std::map<FieldType, std::map<FieldType, std::shared_ptr<CastExecutor>>> executor_map_;
};

}  // namespace paimon
