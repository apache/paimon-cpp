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
#include <string>

#include "paimon/status.h"
#include "paimon_ftindex.h"  // NOLINT(build/include_subdir)

namespace paimon::full_text {

struct FtindexWriterDeleter {
    void operator()(PaimonFtindexWriterHandle* writer) const {
        paimon_ftindex_writer_free(writer);
    }
};

struct FtindexReaderDeleter {
    void operator()(PaimonFtindexReaderHandle* reader) const {
        paimon_ftindex_reader_free(reader);
    }
};

using FtindexWriterPtr = std::unique_ptr<PaimonFtindexWriterHandle, FtindexWriterDeleter>;
using FtindexReaderPtr = std::unique_ptr<PaimonFtindexReaderHandle, FtindexReaderDeleter>;

/// Converts the error of the last failed native call into a `Status`, preferring `callback_error`,
/// the error raised by a file system callback during that call. The native library keeps its last
/// error per thread, so this must be called on the failing thread before any other native call.
Status LastFtindexError(const std::string& action, const Status& callback_error = Status::OK());

}  // namespace paimon::full_text
