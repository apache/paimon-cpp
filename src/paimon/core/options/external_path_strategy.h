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

namespace paimon {
/// Specifies the strategy for selecting external storage paths.
enum class ExternalPathStrategy {
    // Do not choose any external storage, data will still be written to the default warehouse path.
    NONE = 1,
    // Select a specific file system as the external path. Currently supported are S3 and OSS.
    SPECIFIC_FS = 2,
    // When writing a new file, a path is chosen from data-file.external-paths in turn.
    ROUND_ROBIN = 3
};
}  // namespace paimon
