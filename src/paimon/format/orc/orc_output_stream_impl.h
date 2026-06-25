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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "orc/OrcFile.hh"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace paimon {
class OutputStream;
}  // namespace paimon

namespace paimon::orc {
class OrcOutputStreamImpl : public ::orc::OutputStream {
 public:
    static Result<std::unique_ptr<OrcOutputStreamImpl>> Create(
        const std::shared_ptr<paimon::OutputStream>& output_stream);

    ~OrcOutputStreamImpl() override = default;

    uint64_t getLength() const override;
    uint64_t getNaturalWriteSize() const override {
        return ORC_NATURAL_WRITE_SIZE;
    }
    void write(const void* buf, size_t length) override;

    const std::string& getName() const override {
        return file_name_;
    }
    void close() override;

 private:
    OrcOutputStreamImpl(const std::shared_ptr<paimon::OutputStream>& output_stream,
                        const std::string& name);

 private:
    static constexpr uint64_t ORC_NATURAL_WRITE_SIZE = 128 * 1024;

    std::shared_ptr<paimon::OutputStream> output_stream_;
    std::string file_name_;
};
}  // namespace paimon::orc
