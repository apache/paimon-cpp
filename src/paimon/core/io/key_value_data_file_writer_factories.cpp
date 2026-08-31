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

#include "paimon/core/io/key_value_data_file_writer_factories.h"

#include "paimon/common/data/shredding/shredding_write_plan_factories.h"
#include "paimon/core/io/key_value_data_file_writer_factory.h"
#include "paimon/core/io/shredding_key_value_data_file_writer_factory.h"

namespace paimon {

Result<std::shared_ptr<KeyValueDataFileWriterFactories::WriterFactory>>
KeyValueDataFileWriterFactories::Create(const CoreOptions& options, int64_t schema_id,
                                        const std::shared_ptr<arrow::Schema>& write_schema,
                                        int32_t level, FileSource file_source,
                                        const std::vector<std::string>& primary_keys,
                                        const std::shared_ptr<DataFilePathFactory>& path_factory,
                                        bool create_stats_extractor, bool is_changelog,
                                        const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ShreddingWritePlanFactory> plan_factory,
                           ShreddingWritePlanFactories::SelectActive(options, write_schema, pool));
    std::shared_ptr<WriterFactory> writer_factory;
    if (plan_factory != nullptr) {
        writer_factory = std::make_shared<ShreddingKeyValueDataFileWriterFactory>(
            options, schema_id, write_schema, level, file_source, primary_keys, path_factory,
            create_stats_extractor, plan_factory, is_changelog, pool);
    } else {
        writer_factory = std::make_shared<KeyValueDataFileWriterFactory>(
            options, schema_id, write_schema, level, file_source, primary_keys, path_factory,
            create_stats_extractor, is_changelog, pool);
    }
    return writer_factory;
}

}  // namespace paimon
