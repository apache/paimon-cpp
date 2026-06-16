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
#include "paimon/core/manifest/file_kind.h"

#include "fmt/format.h"
#include "paimon/status.h"

namespace paimon {

const FileKind& FileKind::Add() {
    static const FileKind file_kind = FileKind(static_cast<int8_t>(0));
    return file_kind;
}

const FileKind& FileKind::Delete() {
    static const FileKind file_kind = FileKind(static_cast<int8_t>(1));
    return file_kind;
}

Result<FileKind> FileKind::FromByteValue(int8_t value) {
    switch (value) {
        case 0:
            return Add();
        case 1:
            return Delete();
        default:
            return Status::Invalid(fmt::format("Unsupported byte value {} for file kind.",
                                               static_cast<int32_t>(value)));
    }
}

}  // namespace paimon
