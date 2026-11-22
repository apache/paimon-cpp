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

#include "paimon/common/global_index/bitmap_legacy/bitmap_legacy_global_index_factory.h"

#include "paimon/common/file_index/bitmap/bitmap_file_index.h"
#include "paimon/common/global_index/bitmap_legacy/bitmap_legacy_global_index.h"

namespace paimon {

const char BitmapLegacyGlobalIndexFactory::IDENTIFIER[] = "bitmap-legacy-global";

Result<std::unique_ptr<GlobalIndexer>> BitmapLegacyGlobalIndexFactory::Create(
    const std::map<std::string, std::string>& options) const {
    return std::make_unique<BitmapLegacyGlobalIndex>(std::make_shared<BitmapFileIndex>(options));
}

REGISTER_PAIMON_FACTORY(BitmapLegacyGlobalIndexFactory);

}  // namespace paimon
