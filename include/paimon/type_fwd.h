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

#include <limits>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/defs.h"
#include "paimon/macros.h"
#include "paimon/visibility.h"

namespace paimon {

// TODO(yonghao.fyh): type fwd is not complete, refine this in the future

template <typename T>
class Result;

class Status;

class MemoryPool;

class FileFormat;

class ColumnStats;
class IntegerColumnStats;
class StringColumnStats;
class DoubleColumnStats;

using ColumnStatsVector = std::vector<std::shared_ptr<ColumnStats>>;

class FileSystem;
class OutputStream;
class InputStream;
class FileStatus;
class BasicFileStatus;

class RecordBatch;
class RealtimeContext;

class FormatWriter;

class FileBatchReader;

class BatchReader;

class ScanContext;
class ReadContext;

class FileStoreWrite;
class FileStoreCommit;

class WriteContext;
class CommitContext;

class CommitMessage;

class Metrics;

class Executor;
class OrphanFilesCleaner;

}  // namespace paimon
