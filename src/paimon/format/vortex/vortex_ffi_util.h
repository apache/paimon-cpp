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

#pragma once

#include <memory>
#include <string>

#include "paimon/format/vortex/vortex_ffi.h"
#include "paimon/status.h"

namespace paimon::vortex {

/// Build a paimon `Status` from a Vortex error and free it. Returns OK when `error` is null, so it
/// can be called unconditionally after an FFI call that reports errors through a `vx_error**`
/// out-parameter.
Status VortexFfiError(const std::string& operation, vx_error* error);

// Owning RAII aliases for the Vortex C handles; each frees its handle on scope exit. The element
// type matches the constness returned by the corresponding constructor function.
using VxSessionPtr = std::unique_ptr<vx_session, decltype(&vx_session_free)>;
using VxDataSourcePtr = std::unique_ptr<const vx_data_source, decltype(&vx_data_source_free)>;
using VxScanPtr = std::unique_ptr<vx_scan, decltype(&vx_scan_free)>;
using VxPartitionPtr = std::unique_ptr<vx_partition, decltype(&vx_partition_free)>;
using VxDtypePtr = std::unique_ptr<const vx_dtype, decltype(&vx_dtype_free)>;
using VxArrayPtr = std::unique_ptr<const vx_array, decltype(&vx_array_free)>;

}  // namespace paimon::vortex
