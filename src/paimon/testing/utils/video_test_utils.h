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

#include <charconv>
#include <fstream>
#include <string>

#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

// Apache Paimon's Java fixtures before (ff63bf8a6c24) and after (5faef60f6fee) #10905.
inline Result<std::string> ReadJavaVideoFixture(const std::string& name) {
    std::ifstream input(GetDataDir() + "/video_format.data/" + name);
    if (!input) {
        return Status::IOError("cannot open Java video fixture ", name);
    }
    std::string bytes;
    for (std::string line; std::getline(input, line);) {
        if (line.empty() || line.front() == '#') {
            continue;
        }
        if (line.size() % 2 != 0) {
            return Status::Invalid("odd length in Java video fixture ", name);
        }
        for (size_t i = 0; i < line.size(); i += 2) {
            unsigned char byte = 0;
            const char* end = line.data() + i + 2;
            const auto parsed = std::from_chars(line.data() + i, end, byte, 16);
            if (parsed.ec != std::errc() || parsed.ptr != end) {
                return Status::Invalid("invalid hex in Java video fixture ", name);
            }
            bytes.push_back(static_cast<char>(byte));
        }
    }
    if (bytes.empty() || !input.eof()) {
        return Status::Invalid("empty or unreadable Java video fixture ", name);
    }
    return bytes;
}

}  // namespace paimon::test
