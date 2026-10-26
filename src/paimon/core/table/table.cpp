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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/catalog/table.h"

namespace paimon {

Table::Table(const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
             const std::shared_ptr<Schema>& schema, const Identifier& identifier,
             const std::string& uuid)
    : file_system_(file_system),
      root_path_(root_path),
      schema_(schema),
      identifier_(identifier),
      uuid_(uuid) {}

}  // namespace paimon
