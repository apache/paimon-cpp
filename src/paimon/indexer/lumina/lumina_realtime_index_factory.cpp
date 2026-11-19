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

#include "paimon/indexer/lumina/lumina_realtime_index_factory.h"

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/indexer/lumina/lumina_realtime_index.h"
#include "paimon/indexer/lumina/lumina_utils.h"

namespace paimon::lumina {

const char LuminaRealtimeIndexFactory::IDENTIFIER[] = "lumina-realtime";

Result<std::shared_ptr<RealtimeIndex>> LuminaRealtimeIndexFactory::Create(
    ::ArrowSchema* write_schema, const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& pool) const {
    if (!write_schema || !write_schema->release) {
        return Status::Invalid("Lumina realtime factory requires a schema");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                      arrow::ImportSchema(write_schema));
    if (schema->num_fields() != 2) {
        return Status::Invalid("Lumina realtime factory requires offset and one vector field");
    }
    const std::string prefix = "realtime.index.lumina." + schema->field(1)->name() + ".";
    const std::map<std::string, std::string> column_options =
        OptionsUtils::FetchOptionsWithPrefix(prefix, options);
    std::map<std::string, std::string> lumina_options;
    for (const auto& [key, value] : column_options) {
        lumina_options.emplace(std::string(LuminaDefines::kOptionKeyPrefix) + key, value);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeIndex> index,
                           LuminaRealtimeIndex::Create(schema, lumina_options, pool));
    return index;
}

REGISTER_PAIMON_FACTORY(LuminaRealtimeIndexFactory);

}  // namespace paimon::lumina
