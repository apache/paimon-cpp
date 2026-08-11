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

#include <string>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(JavaModifiedUtf8Test, AsciiEncodeIsIdentity) {
    std::string ascii = "data-8b2f1a-0.parquet";
    ASSERT_OK_AND_ASSIGN(std::string encoded, JavaModifiedUtf8::Encode(ascii));
    ASSERT_EQ(ascii, encoded);
    ASSERT_OK_AND_ASSIGN(std::string decoded, JavaModifiedUtf8::Decode(encoded));
    ASSERT_EQ(ascii, decoded);
}

TEST(JavaModifiedUtf8Test, BmpTextRoundTrip) {
    // Chinese characters are three-byte sequences, identical in both encodings.
    std::string utf8 = "订单表-文件.parquet";
    ASSERT_OK_AND_ASSIGN(std::string encoded, JavaModifiedUtf8::Encode(utf8));
    ASSERT_EQ(utf8, encoded);
    ASSERT_OK_AND_ASSIGN(std::string decoded, JavaModifiedUtf8::Decode(encoded));
    ASSERT_EQ(utf8, decoded);
}

TEST(JavaModifiedUtf8Test, NulByteUsesOverlongTwoByteForm) {
    std::string nul(1, '\0');
    ASSERT_OK_AND_ASSIGN(std::string encoded, JavaModifiedUtf8::Encode(nul));
    ASSERT_EQ("\xC0\x80", encoded);
    ASSERT_OK_AND_ASSIGN(std::string decoded, JavaModifiedUtf8::Decode("\xC0\x80"));
    ASSERT_EQ(nul, decoded);

    // U+0000 embedded in surrounding ASCII leaves its neighbors untouched.
    std::string embedded("ab\0cd", 5);
    ASSERT_OK_AND_ASSIGN(std::string embedded_encoded, JavaModifiedUtf8::Encode(embedded));
    ASSERT_EQ(std::string("ab\xC0\x80"
                          "cd",
                          6),
              embedded_encoded);
    ASSERT_OK_AND_ASSIGN(std::string embedded_decoded, JavaModifiedUtf8::Decode(embedded_encoded));
    ASSERT_EQ(embedded, embedded_decoded);
}

TEST(JavaModifiedUtf8Test, SupplementaryCharUsesSurrogatePair) {
    // U+1F600 in standard four-byte UTF-8.
    std::string standard = "\xF0\x9F\x98\x80";
    ASSERT_OK_AND_ASSIGN(std::string encoded, JavaModifiedUtf8::Encode(standard));
    // CESU-8: surrogate pair U+D83D U+DE00, each written as a three-byte sequence.
    ASSERT_EQ("\xED\xA0\xBD\xED\xB8\x80", encoded);
    ASSERT_OK_AND_ASSIGN(std::string decoded, JavaModifiedUtf8::Decode(encoded));
    ASSERT_EQ(standard, decoded);
}

TEST(JavaModifiedUtf8Test, DecodeRejectsMalformedInput) {
    // Java's writeUTF never emits a raw zero byte.
    ASSERT_NOK(JavaModifiedUtf8::Decode(std::string(1, '\0')));
    ASSERT_NOK(JavaModifiedUtf8::Decode(std::string("a\0b", 3)));
    // Truncated two-byte and three-byte sequences.
    ASSERT_NOK(JavaModifiedUtf8::Decode("\xC3"));
    ASSERT_NOK(JavaModifiedUtf8::Decode("\xE8\xB8"));
    // Continuation bytes must match 10xxxxxx.
    ASSERT_NOK(JavaModifiedUtf8::Decode("\xC3\x28"));
    ASSERT_NOK(JavaModifiedUtf8::Decode("\xE8\x28\xB8"));
    // readUTF rejects four-byte leading bytes; supplementary chars must arrive as CESU-8.
    ASSERT_NOK(JavaModifiedUtf8::Decode("\xF0\x9F\x98\x80"));
    // Unpaired high surrogate at end of input.
    ASSERT_NOK(JavaModifiedUtf8::Decode("\xED\xA0\xBD"));
    // High surrogate followed by a non-surrogate unit.
    ASSERT_NOK(
        JavaModifiedUtf8::Decode("\xED\xA0\xBD"
                                 "z"));
    // Low surrogate without a preceding high surrogate.
    ASSERT_NOK(JavaModifiedUtf8::Decode("\xED\xB8\x80"));
}

TEST(JavaModifiedUtf8Test, EncodeRejectsInvalidUtf8) {
    // Stray continuation byte.
    ASSERT_NOK(JavaModifiedUtf8::Encode("\x80"));
    ASSERT_NOK(JavaModifiedUtf8::Encode("a\x80"));
    // Truncated multi-byte sequences.
    ASSERT_NOK(JavaModifiedUtf8::Encode("\xC3"));
    ASSERT_NOK(JavaModifiedUtf8::Encode("\xE8\xB8"));
    ASSERT_NOK(JavaModifiedUtf8::Encode("\xF0\x9F\x98"));
    // Overlong two-byte encoding of U+002F.
    ASSERT_NOK(JavaModifiedUtf8::Encode("\xC0\xAF"));
    // Surrogate code point U+D800 encoded directly as a three-byte sequence.
    ASSERT_NOK(JavaModifiedUtf8::Encode("\xED\xA0\x80"));
    // Code point above U+10FFFF.
    ASSERT_NOK(JavaModifiedUtf8::Encode("\xF4\x90\x80\x80"));
}

TEST(JavaModifiedUtf8Test, DecodeAcceptsJavaLenientOverlongTwoByteForm) {
    // Java's readUTF only pattern-matches the bit layout of two-byte sequences, so the
    // overlong encoding 0xC1 0xBF of U+007F is accepted; Decode mirrors that leniency.
    ASSERT_OK_AND_ASSIGN(std::string decoded, JavaModifiedUtf8::Decode("\xC1\xBF"));
    ASSERT_EQ("\x7F", decoded);
}

}  // namespace paimon::test
