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

/// Stateless deleter bound to a Vortex C release function.
template <typename T, auto FreeFn>
struct VxDeleter {
    void operator()(T* ptr) const noexcept {
        FreeFn(ptr);
    }
};

template <typename T, auto FreeFn>
using VxUniquePtr = std::unique_ptr<T, VxDeleter<T, FreeFn>>;

// Owning RAII aliases for the Vortex C handles; each frees its handle on scope exit. The element
// type matches the constness returned by the corresponding constructor function.
using VxSessionPtr = VxUniquePtr<vx_session, vx_session_free>;
using VxDataSourcePtr = VxUniquePtr<const vx_data_source, vx_data_source_free>;
using VxScanPtr = VxUniquePtr<vx_scan, vx_scan_free>;
using VxPartitionPtr = VxUniquePtr<vx_partition, vx_partition_free>;
using VxDtypePtr = VxUniquePtr<const vx_dtype, vx_dtype_free>;
using VxArrayPtr = VxUniquePtr<const vx_array, vx_array_free>;
using VxExpressionPtr = VxUniquePtr<vx_expression, vx_expression_free>;
using VxScalarPtr = VxUniquePtr<vx_scalar, vx_scalar_free>;

}  // namespace paimon::vortex
