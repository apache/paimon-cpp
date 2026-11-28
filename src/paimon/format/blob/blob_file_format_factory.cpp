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

#include "paimon/format/blob/blob_file_format_factory.h"

#include <utility>

#include "paimon/common/data/blob_defs.h"
#include "paimon/common/reader/blob_read_context.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/factories/factory.h"
#include "paimon/format/blob/blob_file_format.h"
#include "paimon/format/blob/video_file_batch_reader.h"

namespace paimon::blob {
namespace {

class VideoReaderBuilder : public ReaderBuilder, public BlobReadContextAwareBuilder {
 public:
    VideoReaderBuilder(int32_t batch_size, bool emit_placeholder_sentinel)
        : batch_size_(batch_size), emit_placeholder_sentinel_(emit_placeholder_sentinel) {}

    ReaderBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        pool_ = pool;
        return this;
    }

    void SetBlobReadContext(const std::shared_ptr<BlobReadContext>& context) override {
        blob_context_ = context;
    }

    Result<std::unique_ptr<FileBatchReader>> Build(
        const std::shared_ptr<InputStream>& input) const override {
        return VideoFileBatchReader::Create(input, batch_size_, pool_, emit_placeholder_sentinel_,
                                            blob_context_);
    }

 private:
    int32_t batch_size_;
    bool emit_placeholder_sentinel_;
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<BlobReadContext> blob_context_;
};

class VideoFileFormat : public FileFormat {
 public:
    explicit VideoFileFormat(bool emit_placeholder_sentinel)
        : emit_placeholder_sentinel_(emit_placeholder_sentinel) {}

    const std::string& Identifier() const override {
        static const std::string identifier = "video";
        return identifier;
    }

    Result<std::unique_ptr<ReaderBuilder>> CreateReaderBuilder(int32_t batch_size) const override {
        return std::make_unique<VideoReaderBuilder>(batch_size, emit_placeholder_sentinel_);
    }

    Result<std::unique_ptr<WriterBuilder>> CreateWriterBuilder(::ArrowSchema*,
                                                               int32_t) const override {
        return Status::NotImplemented("video writing is not supported");
    }

    Result<std::unique_ptr<FormatStatsExtractor>> CreateStatsExtractor(
        ::ArrowSchema*) const override {
        return Status::NotImplemented("video stats extraction is not supported");
    }

 private:
    bool emit_placeholder_sentinel_;
};

class VideoFileFormatFactory : public FileFormatFactory {
 public:
    const char* Identifier() const override {
        return "video";
    }

    Result<std::unique_ptr<FileFormat>> Create(
        const std::map<std::string, std::string>& options) const override {
        PAIMON_ASSIGN_OR_RAISE(bool emit_placeholder_sentinel,
                               OptionsUtils::GetValueFromMap<bool>(
                                   options, BlobDefs::kEmitPlaceholderSentinelKey, false));
        return std::make_unique<VideoFileFormat>(emit_placeholder_sentinel);
    }
};

}  // namespace

const char BlobFileFormatFactory::IDENTIFIER[] = "blob";

Result<std::unique_ptr<FileFormat>> BlobFileFormatFactory::Create(
    const std::map<std::string, std::string>& options) const {
    return std::make_unique<BlobFileFormat>(options);
}

REGISTER_PAIMON_FACTORY(BlobFileFormatFactory);
REGISTER_PAIMON_FACTORY(VideoFileFormatFactory);

}  // namespace paimon::blob
