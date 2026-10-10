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

#include "paimon/global_index/full_text/full_text_global_index_reader.h"

#include <map>
#include <utility>

#include "fmt/format.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/memory/bytes.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon::full_text {

struct FullTextInputContext {
    std::unique_ptr<InputStream> stream;
    /// Serializes reads, like Java: the engine may read concurrently, but some `InputStream`
    /// implementations are not safe for concurrent positional reads.
    std::mutex mutex;
};

namespace {

/// Error raised by the input stream in the last failed read on this thread, reported instead of the
/// generic native error. It is cleared before each native call that reads the index.
thread_local Status last_read_error;

int PreadFromInputStream(void* ctx, uint64_t pos, uint8_t* buf, size_t len) {
    auto* input = static_cast<FullTextInputContext*>(ctx);
    std::lock_guard<std::mutex> lock(input->mutex);
    size_t total = 0;
    while (total < len) {
        Result<int64_t> result = input->stream->Read(reinterpret_cast<char*>(buf + total),
                                                     static_cast<int64_t>(len - total),
                                                     static_cast<int64_t>(pos + total));
        if (!result.ok()) {
            last_read_error = result.status();
            return -1;
        }
        if (result.value() <= 0) {
            last_read_error = Status::IOError(
                fmt::format("unexpected end of full-text index file at offset {}", pos + total));
            return -1;
        }
        total += static_cast<size_t>(result.value());
    }
    return 0;
}

Status TakeLastError(const std::string& action) {
    return LastFtindexError(action, std::exchange(last_read_error, Status::OK()));
}

/// Converts the engine output, ordered by descending score, into a result ordered by row id.
std::shared_ptr<ScoredGlobalIndexResult> ToScoredResult(const std::vector<int64_t>& row_ids,
                                                        const std::vector<float>& scores) {
    // As in Java, a row id that was indexed more than once keeps the score of its last hit.
    std::map<int64_t, float> scores_by_row_id;
    for (size_t i = 0; i < row_ids.size(); ++i) {
        scores_by_row_id[row_ids[i]] = scores[i];
    }
    RoaringBitmap64 bitmap;
    std::vector<float> sorted_scores;
    sorted_scores.reserve(scores_by_row_id.size());
    for (const auto& [row_id, score] : scores_by_row_id) {
        bitmap.Add(row_id);
        sorted_scores.push_back(score);
    }
    return std::make_shared<BitmapScoredGlobalIndexResult>(std::move(bitmap),
                                                           std::move(sorted_scores));
}

}  // namespace

Result<std::shared_ptr<FullTextGlobalIndexReader>> FullTextGlobalIndexReader::Create(
    const GlobalIndexIOMeta& io_meta, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::shared_ptr<MemoryPool>& pool) {
    if (!file_reader) {
        return Status::Invalid(
            "file reader must not be null when create FullTextGlobalIndexReader");
    }
    return std::shared_ptr<FullTextGlobalIndexReader>(
        new FullTextGlobalIndexReader(io_meta, file_reader, pool));
}

FullTextGlobalIndexReader::FullTextGlobalIndexReader(
    const GlobalIndexIOMeta& io_meta, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::shared_ptr<MemoryPool>& pool)
    : io_meta_(io_meta),
      file_reader_(file_reader),
      pool_(pool ? pool : GetDefaultPool()),
      logger_(Logger::GetLogger("FullTextGlobalIndexReader")) {}

FullTextGlobalIndexReader::~FullTextGlobalIndexReader() {
    // Free the native reader before closing the stream it reads through.
    reader_.reset();
    if (input_) {
        CloseInputStream(input_->stream.get());
    }
}

void FullTextGlobalIndexReader::CloseInputStream(InputStream* stream) const {
    Status status = stream->Close();
    if (!status.ok()) {
        PAIMON_LOG_WARN(logger_, "failed to close full-text index file %s: %s",
                        io_meta_.file_path.c_str(), status.ToString().c_str());
    }
}

Result<PaimonFtindexReaderHandle*> FullTextGlobalIndexReader::GetOrOpenReader() {
    std::lock_guard<std::mutex> lock(open_mutex_);
    if (reader_) {
        return reader_.get();
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> stream,
                           file_reader_->GetInputStream(io_meta_.file_path));
    auto input = std::make_unique<FullTextInputContext>();
    input->stream = std::move(stream);
    PaimonFtindexInputFile input_file{static_cast<void*>(input.get()), &PreadFromInputStream};
    last_read_error = Status::OK();
    FtindexReaderPtr reader(paimon_ftindex_reader_open(input_file));
    if (!reader) {
        Status status = TakeLastError(fmt::format("open full-text index {}", io_meta_.file_path));
        CloseInputStream(input->stream.get());
        return status;
    }
    input_ = std::move(input);
    reader_ = std::move(reader);
    return reader_.get();
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> FullTextGlobalIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    if (!full_text_search) {
        return Status::Invalid("VisitFullTextSearch: null FullTextSearch pointer");
    }
    if (full_text_search->limit <= 0) {
        return Status::Invalid("full-text index search requires a positive limit");
    }
    const std::string& query = full_text_search->query;
    if (query.find('\0') != std::string::npos) {
        return Status::Invalid("full-text index query must not contain NUL characters");
    }

    PAIMON_UNIQUE_PTR<Bytes> filter_bytes;
    if (full_text_search->include_row_ids) {
        const RoaringBitmap64& include_row_ids = full_text_search->include_row_ids.value();
        if (include_row_ids.IsEmpty()) {
            return ToScoredResult({}, {});
        }
        // Serialize() optimizes the bitmap in place and the same search may be visited by
        // concurrent readers, so serialize a copy.
        RoaringBitmap64 filter = include_row_ids;
        filter_bytes = filter.Serialize(pool_.get());
    }

    PAIMON_ASSIGN_OR_RAISE(PaimonFtindexReaderHandle * reader, GetOrOpenReader());
    auto limit = static_cast<size_t>(full_text_search->limit);
    std::vector<int64_t> row_ids(limit);
    std::vector<float> scores(limit);
    size_t result_len = 0;
    int status = 0;
    last_read_error = Status::OK();
    if (filter_bytes) {
        status = paimon_ftindex_reader_search_with_roaring_filter(
            reader, query.c_str(), limit, reinterpret_cast<const uint8_t*>(filter_bytes->data()),
            filter_bytes->size(), row_ids.data(), scores.data(), limit, &result_len);
    } else {
        status = paimon_ftindex_reader_search(reader, query.c_str(), limit, row_ids.data(),
                                              scores.data(), limit, &result_len);
    }
    if (status != 0) {
        return TakeLastError(fmt::format("search full-text index {}", io_meta_.file_path));
    }
    row_ids.resize(result_len);
    scores.resize(result_len);
    return ToScoredResult(row_ids, scores);
}

}  // namespace paimon::full_text
