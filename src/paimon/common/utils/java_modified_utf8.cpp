/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/common/utils/java_modified_utf8.h"

#include <cstdint>

#include "fmt/format.h"

namespace paimon {
namespace {
constexpr uint32_t kSupplementaryStart = 0x10000;
constexpr uint32_t kMaxCodePoint = 0x10FFFF;
constexpr uint32_t kHighSurrogateStart = 0xD800;
constexpr uint32_t kLowSurrogateStart = 0xDC00;
constexpr uint32_t kSurrogateEnd = 0xDFFF;

void AppendTwoBytes(uint32_t code_point, std::string* out) {
    out->push_back(static_cast<char>(0xC0 | ((code_point >> 6) & 0x1F)));
    out->push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
}

void AppendThreeBytes(uint32_t code_point, std::string* out) {
    out->push_back(static_cast<char>(0xE0 | ((code_point >> 12) & 0x0F)));
    out->push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
    out->push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
}

Status MalformedInput(const std::string& what, size_t position) {
    return Status::Invalid(fmt::format("Malformed UTF-8 input: {} around byte {}", what, position));
}
}  // namespace

Result<std::string> JavaModifiedUtf8::Encode(std::string_view utf8) {
    std::string out;
    out.reserve(utf8.size());
    size_t i = 0;
    while (i < utf8.size()) {
        uint8_t byte0 = static_cast<uint8_t>(utf8[i]);
        if (byte0 < 0x80) {
            if (byte0 == 0) {
                // Java encodes U+0000 as the overlong two-byte form.
                AppendTwoBytes(0, &out);
            } else {
                out.push_back(static_cast<char>(byte0));
            }
            i += 1;
            continue;
        }
        int32_t continuation_count = 0;
        uint32_t code_point = 0;
        if ((byte0 & 0xE0) == 0xC0) {
            continuation_count = 1;
            code_point = byte0 & 0x1F;
        } else if ((byte0 & 0xF0) == 0xE0) {
            continuation_count = 2;
            code_point = byte0 & 0x0F;
        } else if ((byte0 & 0xF8) == 0xF0) {
            continuation_count = 3;
            code_point = byte0 & 0x07;
        } else {
            return MalformedInput("invalid leading byte", i);
        }
        if (i + continuation_count >= utf8.size()) {
            return MalformedInput("truncated sequence", i);
        }
        for (int32_t k = 1; k <= continuation_count; k++) {
            uint8_t continuation = static_cast<uint8_t>(utf8[i + k]);
            if ((continuation & 0xC0) != 0x80) {
                return MalformedInput("invalid continuation byte", i + k);
            }
            code_point = (code_point << 6) | (continuation & 0x3F);
        }
        // Reject overlong forms and code points outside Unicode.
        static constexpr uint32_t kMinByLength[4] = {0, 0x80, 0x800, kSupplementaryStart};
        if (code_point < kMinByLength[continuation_count] || code_point > kMaxCodePoint ||
            (code_point >= kHighSurrogateStart && code_point <= kSurrogateEnd)) {
            return MalformedInput("invalid code point", i);
        }
        if (code_point < 0x800) {
            AppendTwoBytes(code_point, &out);
        } else if (code_point < kSupplementaryStart) {
            AppendThreeBytes(code_point, &out);
        } else {
            // Java writes supplementary code points as a CESU-8 surrogate pair.
            uint32_t offset = code_point - kSupplementaryStart;
            AppendThreeBytes(kHighSurrogateStart + (offset >> 10), &out);
            AppendThreeBytes(kLowSurrogateStart + (offset & 0x3FF), &out);
        }
        i += 1 + continuation_count;
    }
    return out;
}

Result<std::string> JavaModifiedUtf8::Decode(std::string_view modified_utf8) {
    std::string out;
    out.reserve(modified_utf8.size());
    size_t i = 0;
    // Decoded UTF-16 code units, kept across iterations to pair surrogates.
    uint32_t pending_high_surrogate = 0;
    bool has_pending_high_surrogate = false;
    while (i < modified_utf8.size()) {
        uint8_t byte0 = static_cast<uint8_t>(modified_utf8[i]);
        uint32_t unit = 0;
        if (byte0 < 0x80) {
            if (byte0 == 0) {
                // Java's writeUTF never emits a raw zero byte.
                return MalformedInput("unexpected raw zero byte", i);
            }
            unit = byte0;
            i += 1;
        } else if ((byte0 & 0xE0) == 0xC0) {
            if (i + 1 >= modified_utf8.size()) {
                return MalformedInput("truncated two-byte sequence", i);
            }
            uint8_t byte1 = static_cast<uint8_t>(modified_utf8[i + 1]);
            if ((byte1 & 0xC0) != 0x80) {
                return MalformedInput("invalid continuation byte", i + 1);
            }
            unit = ((byte0 & 0x1F) << 6) | (byte1 & 0x3F);
            i += 2;
        } else if ((byte0 & 0xF0) == 0xE0) {
            if (i + 2 >= modified_utf8.size()) {
                return MalformedInput("truncated three-byte sequence", i);
            }
            uint8_t byte1 = static_cast<uint8_t>(modified_utf8[i + 1]);
            uint8_t byte2 = static_cast<uint8_t>(modified_utf8[i + 2]);
            if ((byte1 & 0xC0) != 0x80 || (byte2 & 0xC0) != 0x80) {
                return MalformedInput("invalid continuation byte", i + 1);
            }
            unit = ((byte0 & 0x0F) << 12) | ((byte1 & 0x3F) << 6) | (byte2 & 0x3F);
            i += 3;
        } else {
            // Java's readUTF rejects four-byte sequences and stray continuation bytes.
            return MalformedInput("invalid leading byte", i);
        }

        if (has_pending_high_surrogate) {
            if (unit >= kLowSurrogateStart && unit <= kSurrogateEnd) {
                uint32_t code_point = kSupplementaryStart +
                                      ((pending_high_surrogate - kHighSurrogateStart) << 10) +
                                      (unit - kLowSurrogateStart);
                out.push_back(static_cast<char>(0xF0 | ((code_point >> 18) & 0x07)));
                out.push_back(static_cast<char>(0x80 | ((code_point >> 12) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
                has_pending_high_surrogate = false;
                continue;
            }
            return MalformedInput("unpaired high surrogate", i);
        }
        if (unit >= kHighSurrogateStart && unit < kLowSurrogateStart) {
            pending_high_surrogate = unit;
            has_pending_high_surrogate = true;
            continue;
        }
        if (unit >= kLowSurrogateStart && unit <= kSurrogateEnd) {
            return MalformedInput("unpaired low surrogate", i);
        }
        if (unit < 0x80) {
            out.push_back(static_cast<char>(unit));
        } else if (unit < 0x800) {
            AppendTwoBytes(unit, &out);
        } else {
            AppendThreeBytes(unit, &out);
        }
    }
    if (has_pending_high_surrogate) {
        return MalformedInput("unpaired high surrogate at end", modified_utf8.size());
    }
    return out;
}

}  // namespace paimon
