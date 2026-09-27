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

#include <cstdint>

namespace paimon {

/// Options for bitmap global index.
struct BitmapGlobalIndexOptions {
    BitmapGlobalIndexOptions() = delete;
    ~BitmapGlobalIndexOptions() = delete;

    /// The target dictionary block size for bitmap global index. Default value is 16 KB.
    static inline const char kBitmapIndexDictionaryBlockSize[] =
        "bitmap-index.dictionary-block-size";

    /// The compression algorithm to use for bitmap dictionary blocks. Default value is "none".
    static inline const char kBitmapIndexCompression[] = "bitmap-index.compression";

    /// The compression level of the bitmap dictionary block codec. Default value is 1.
    static inline const char kBitmapIndexCompressionLevel[] = "bitmap-index.compression-level";

    /// The maximum total bitmap global index file size to allow fallback dictionary scans for
    /// predicates that cannot use direct bitmap lookup. Default value is 256 MB.
    static inline const char kBitmapIndexFallbackScanMaxSize[] =
        "bitmap-index.fallback-scan-max-size";

    static inline const char kDefaultBitmapIndexDictionaryBlockSize[] = "16KB";
    static inline const char kDefaultBitmapIndexCompression[] = "none";
    static inline const int32_t kDefaultBitmapIndexCompressionLevel = 1;
    static inline const char kDefaultBitmapIndexFallbackScanMaxSize[] = "256MB";
};

}  // namespace paimon
