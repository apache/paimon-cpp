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

#include <memory>
#include <utility>

#include "arrow/util/checked_cast.h"

namespace paimon {

/// Casts using Arrow's debug-checked cast implementation. In release builds this is a static
/// cast, so callers must validate any recoverable runtime type mismatch before calling it.
template <typename OutputType, typename InputType>
inline OutputType checked_cast(InputType&& value) {
    return arrow::internal::checked_cast<OutputType>(std::forward<InputType>(value));
}

template <typename OutputType, typename InputType>
inline std::shared_ptr<OutputType> checked_pointer_cast(std::shared_ptr<InputType> value) noexcept {
    return arrow::internal::checked_pointer_cast<OutputType>(std::move(value));
}

template <typename OutputType, typename InputType>
inline std::unique_ptr<OutputType> checked_pointer_cast(std::unique_ptr<InputType> value) noexcept {
    return arrow::internal::checked_pointer_cast<OutputType>(std::move(value));
}

}  // namespace paimon
