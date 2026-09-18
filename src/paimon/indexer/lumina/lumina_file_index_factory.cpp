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

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "paimon/factories/factory.h"
#include "paimon/file_index/file_indexer_factory.h"
#include "paimon/indexer/lumina/lumina_file_index.h"

namespace paimon::lumina {
namespace {

class LuminaFileIndexFactory final : public FileIndexerFactory {
 public:
    const char* Identifier() const override {
        return "lumina";
    }

    Result<std::unique_ptr<FileIndexer>> Create(
        const std::map<std::string, std::string>& options) const override {
        return std::make_unique<LuminaFileIndexer>(options);
    }
};

REGISTER_PAIMON_FACTORY(LuminaFileIndexFactory);

}  // namespace
}  // namespace paimon::lumina
