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

#include <string>
#include <string_view>

#include "paimon/result.h"

namespace paimon {
/// Converts between standard UTF-8 and the "modified UTF-8" used by Java's
/// `DataOutputStream#writeUTF` / `DataInputStream#readUTF`:
/// - U+0000 is encoded as the two-byte sequence 0xC0 0x80 instead of a single zero byte;
/// - supplementary code points (U+10000 and above) are encoded as a UTF-16 surrogate pair,
///   each surrogate written as an independent three-byte sequence (CESU-8), instead of the
///   four-byte standard UTF-8 form.
class JavaModifiedUtf8 {
 public:
    JavaModifiedUtf8() = delete;
    ~JavaModifiedUtf8() = delete;

    /// Encodes a standard UTF-8 string into Java modified UTF-8 bytes.
    /// @return An error status if `utf8` is not well-formed UTF-8.
    static Result<std::string> Encode(std::string_view utf8);

    /// Decodes Java modified UTF-8 bytes into a standard UTF-8 string, mirroring the
    /// validation of Java's `DataInputStream#readUTF`.
    /// @return An error status on any malformed byte sequence.
    static Result<std::string> Decode(std::string_view modified_utf8);
};

}  // namespace paimon
