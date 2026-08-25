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

#include <array>
#include <optional>
#include <string>

#include "paimon/common/utils/string_utils.h"

namespace paimon {

/// The compressions hadoop names in a file's extension.
///
/// A data file's name carries its compression when it is one of these -
/// `data-<uuid>-0.snappy.parquet` - and the option's own text otherwise.
///
/// Every name and extension below is fixed by what the rest of the paimon ecosystem writes and
/// reads, not chosen here: `zstd` is spelled `zst` in a file name because that is the extension
/// those files carry. Changing one makes files written elsewhere unreadable, and files written
/// here unreadable elsewhere.
class HadoopCompression {
 public:
    enum class Kind {
        NONE,
        GZIP,
        BZIP2,
        DEFLATE,
        SNAPPY,
        LZ4,
        ZSTD,
    };

    HadoopCompression() = delete;
    ~HadoopCompression() = delete;

    /// The compression a `file.compression` value names, case-insensitively. An empty value and
    /// "none" are `NONE`; a value naming no compression at all is nullopt, and a caller writes
    /// that one into the file name verbatim. Only the names listed here are recognised, so
    /// "uncompressed" is not one of them.
    static std::optional<Kind> FromName(const std::string& name) {
        std::string normalized = StringUtils::ToLowerCase(name);
        if (normalized.empty() || normalized == "none") {
            return Kind::NONE;
        }
        for (const Kind kind : AllCompressions()) {
            if (normalized == ToName(kind)) {
                return kind;
            }
        }
        return std::nullopt;
    }

    /// The extension the compression adds to a file name, without its dot. Empty for `NONE`.
    static std::string ToFileExtension(Kind kind) {
        switch (kind) {
            case Kind::GZIP:
                return "gz";
            case Kind::BZIP2:
                return "bz2";
            case Kind::DEFLATE:
                return "deflate";
            case Kind::SNAPPY:
                return "snappy";
            case Kind::LZ4:
                return "lz4";
            case Kind::ZSTD:
                return "zst";
            case Kind::NONE:
                break;
        }
        return std::string();
    }

 private:
    /// The name a `file.compression` value carries for the compression.
    static std::string ToName(Kind kind) {
        switch (kind) {
            case Kind::GZIP:
                return "gzip";
            case Kind::BZIP2:
                return "bzip2";
            case Kind::DEFLATE:
                return "deflate";
            case Kind::SNAPPY:
                return "snappy";
            case Kind::LZ4:
                return "lz4";
            case Kind::ZSTD:
                return "zstd";
            case Kind::NONE:
                break;
        }
        return std::string();
    }

    static const std::array<Kind, 6>& AllCompressions() {
        static const std::array<Kind, 6> kAll = {Kind::GZIP,   Kind::BZIP2, Kind::DEFLATE,
                                                 Kind::SNAPPY, Kind::LZ4,   Kind::ZSTD};
        return kAll;
    }
};

}  // namespace paimon
