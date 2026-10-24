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

#define ZSTD_STATIC_LINKING_ONLY
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zstd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "paimon/common/memory/planning_budget_internal.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_scan.h"
#include "rapidjson/document.h"

namespace paimon {
#if defined(__linux__) && defined(__GLIBCXX__) && _GLIBCXX_USE_CXX11_ABI && _GLIBCXX_RELEASE >= 9
namespace {
constexpr size_t kMaximumDepth = 64;

// RapidJSON's DOM AND parser stack use this allocator. No default arena is used.
class JsonAllocator {
 public:
    static constexpr bool kNeedFree = true;
    explicit JsonAllocator(PlanningResource* resource = nullptr) : resource_(resource) {}
    ~JsonAllocator() {
        // RapidJSON may pop owned children before allocating their parent. Its
        // exception unwinding cannot reclaim those children after admission fails.
        while (head_) {
            Free(head_ + 1);
        }
    }
    void* Malloc(size_t size) {
        if (size == 0) {
            return nullptr;
        }
        if (size > std::numeric_limits<size_t>::max() - sizeof(Header)) {
            throw std::bad_alloc();
        }
        auto* header =
            static_cast<Header*>(resource_->allocate(size + sizeof(Header), alignof(Header)));
        header->owner = this;
        header->size = size;
        header->previous = nullptr;
        header->next = head_;
        if (head_) {
            head_->previous = header;
        }
        head_ = header;
        return header + 1;
    }
    void* Realloc(void* ptr, size_t, size_t size) {
        if (!ptr) {
            return Malloc(size);
        }
        if (size == 0) {
            Free(ptr);
            return nullptr;
        }
        auto* old = static_cast<Header*>(ptr) - 1;
        void* result = Malloc(size);
        memcpy(result, ptr, std::min(size, old->size));
        Free(ptr);
        return result;
    }
    static void Free(void* ptr) {
        if (ptr) {
            auto* header = static_cast<Header*>(ptr) - 1;
            auto* owner = header->owner;
            if (header->previous) {
                header->previous->next = header->next;
            } else {
                owner->head_ = header->next;
            }
            if (header->next) {
                header->next->previous = header->previous;
            }
            owner->resource_->deallocate(header, header->size + sizeof(Header), alignof(Header));
        }
    }

 private:
    struct alignas(std::max_align_t) Header {
        JsonAllocator* owner;
        size_t size;
        Header* previous;
        Header* next;
    };
    PlanningResource* resource_;
    Header* head_ = nullptr;
};
using Json = rapidjson::GenericValue<rapidjson::UTF8<>, JsonAllocator>;
using Document = rapidjson::GenericDocument<rapidjson::UTF8<>, JsonAllocator, JsonAllocator>;

const Json* Member(const Json& object, std::string_view name) {
    if (!object.IsObject()) {
        return nullptr;
    }
    auto it = object.FindMember(rapidjson::GenericStringRef<char>(name.data(), name.size()));
    return it == object.MemberEnd() ? nullptr : &it->value;
}
std::string_view Text(const Json& value) {
    return {value.GetString(), value.GetStringLength()};
}

class JsonInput {
 public:
    explicit JsonInput(PlanningResource* resource)
        : allocator_(resource), document_(&allocator_, 256, &allocator_) {}
    Status Parse(std::string_view input) {
        size_t depth = 0;
        bool quoted = false;
        bool escaped = false;
        for (char ch : input) {
            if (ch == '\0') {
                return Status::Invalid("Embedded NUL in planning JSON");
            }
            if (quoted) {
                if (escaped) {
                    escaped = false;
                } else if (ch == '\\') {
                    escaped = true;
                } else if (ch == '"') {
                    quoted = false;
                }
            } else if (ch == '"') {
                quoted = true;
            } else if (ch == '{' || ch == '[') {
                if (++depth > kMaximumDepth) {
                    return Status::Invalid("Planning JSON nesting limit exceeded");
                }
            } else if (ch == '}' || ch == ']') {
                if (depth == 0) {
                    return Status::Invalid("Unbalanced planning JSON");
                }
                --depth;
            }
        }
        document_.Parse<rapidjson::kParseIterativeFlag | rapidjson::kParseValidateEncodingFlag>(
            input.data(), input.size());
        if (document_.HasParseError()) {
            return Status::Invalid("Invalid planning JSON");
        }
        return UniqueMembers(document_);
    }
    const Json& Root() const {
        return document_;
    }

 private:
    static Status UniqueMembers(const Json& value) {
        if (value.IsObject()) {
            for (auto member = value.MemberBegin(); member != value.MemberEnd(); ++member) {
                for (auto previous = value.MemberBegin(); previous != member; ++previous) {
                    if (Text(previous->name) == Text(member->name)) {
                        return Status::Invalid("Duplicate planning JSON member");
                    }
                }
                PAIMON_RETURN_NOT_OK(UniqueMembers(member->value));
            }
        } else if (value.IsArray()) {
            for (const auto& item : value.GetArray()) {
                PAIMON_RETURN_NOT_OK(UniqueMembers(item));
            }
        }
        return Status::OK();
    }
    JsonAllocator allocator_;
    Document document_;
};

class FileDescriptor {
 public:
    explicit FileDescriptor(int32_t fd) : fd_(fd) {}
    ~FileDescriptor() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }
    int32_t Get() const {
        return fd_;
    }
    Status Close() {
        int32_t fd = std::exchange(fd_, -1);
        if (::close(fd) != 0) {
            return Status::IOError("Cannot close planning input");
        }
        return Status::OK();
    }

 private:
    int32_t fd_;
};

Result<std::pmr::vector<char>> ReadFile(const std::pmr::string& path, PlanningResource* resource) {
    FileDescriptor fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
    if (fd.Get() < 0) {
        return Status::IOError("Cannot open bound planning input");
    }
    struct stat info{};
    if (::fstat(fd.Get(), &info) != 0 || !S_ISREG(info.st_mode) || info.st_size < 0) {
        return Status::IOError("Invalid bound planning input");
    }
    if (static_cast<uint64_t>(info.st_size) > static_cast<uint64_t>(PTRDIFF_MAX)) {
        return Status::CapacityError("Planning input length exceeds addressable storage");
    }
    std::pmr::vector<char> bytes(static_cast<size_t>(info.st_size), resource);
    size_t done = 0;
    while (done < bytes.size()) {
        ssize_t count = ::read(fd.Get(), bytes.data() + done,
                               std::min(bytes.size() - done, static_cast<size_t>(SSIZE_MAX)));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            return Status::IOError("Truncated bound planning input");
        }
        done += count;
    }
    // Refuse changing inputs rather than accepting a prefix of a file which grew.
    char extra;
    ssize_t tail;
    do {
        tail = ::read(fd.Get(), &extra, 1);
    } while (tail < 0 && errno == EINTR);
    if (tail != 0) {
        return Status::IOError("Bound planning input changed while reading");
    }
    PAIMON_RETURN_NOT_OK(fd.Close());
    return bytes;
}

bool Component(std::string_view value) {
    return !value.empty() && value != "." && value != ".." &&
           value.find_first_of("/\\\0", 0, 3) == std::string_view::npos;
}

class Cursor {
 public:
    explicit Cursor(std::string_view data) : data_(data) {}
    size_t Remaining() const {
        return data_.size() - position_;
    }
    size_t Position() const {
        return position_;
    }
    Result<std::string_view> Take(uint64_t count) {
        if (count > Remaining()) {
            return Status::Invalid("Truncated Avro metadata");
        }
        auto result = data_.substr(position_, count);
        position_ += count;
        return result;
    }
    Result<int64_t> Long() {
        uint64_t bits = 0;
        for (uint32_t shift = 0; shift < 70; shift += 7) {
            PAIMON_ASSIGN_OR_RAISE(std::string_view byte, Take(1));
            uint8_t value = static_cast<uint8_t>(byte[0]);
            if (shift == 63 && value > 1) {
                return Status::Invalid("Avro integer overflow");
            }
            bits |= static_cast<uint64_t>(value & 127) << shift;
            if ((value & 128) == 0) {
                return static_cast<int64_t>(bits >> 1) ^ -static_cast<int64_t>(bits & 1);
            }
        }
        return Status::Invalid("Avro integer overflow");
    }
    Result<std::string_view> Bytes() {
        PAIMON_ASSIGN_OR_RAISE(int64_t size, Long());
        if (size < 0) {
            return Status::Invalid("Negative Avro byte length");
        }
        return Take(size);
    }

 private:
    std::string_view data_;
    size_t position_ = 0;
};

struct Datum {
    enum class Kind { Null, Integer, Bytes, Record, Array, Unsupported };
    explicit Datum(PlanningResource* resource) : children(resource) {}
    Kind kind = Kind::Null;
    int64_t number = 0;
    std::string_view bytes;
    const Json* schema = nullptr;
    std::pmr::vector<Datum> children;
    const Datum* Find(std::string_view name) const {
        if (kind != Kind::Record) {
            return nullptr;
        }
        const auto* fields = Member(*schema, "fields");
        if (!fields || !fields->IsArray()) {
            return nullptr;
        }
        for (size_t i = 0; i < fields->Size(); ++i) {
            const Json* field_name = Member((*fields)[i], "name");
            if (field_name && field_name->IsString() && Text(*field_name) == name) {
                return &children[i];
            }
        }
        return nullptr;
    }
};

Result<int64_t> Integer(const Datum& row, std::string_view field) {
    const Datum* value = row.Find(field);
    if (!value || value->kind != Datum::Kind::Integer) {
        return Status::Invalid("Missing integer metadata field");
    }
    return value->number;
}
Result<std::string_view> String(const Datum& row, std::string_view field) {
    const Datum* value = row.Find(field);
    if (!value || value->kind != Datum::Kind::Bytes) {
        return Status::Invalid("Missing byte metadata field");
    }
    return value->bytes;
}
bool Absent(const Datum& row, std::string_view field) {
    const Datum* value = row.Find(field);
    return !value || value->kind == Datum::Kind::Null;
}
bool EmptyArray(const Datum& row, std::string_view field) {
    const Datum* value = row.Find(field);
    return Absent(row, field) || (value->kind == Datum::Kind::Array && value->children.empty());
}
Result<std::optional<int64_t>> OptionalInteger(const Datum& row, std::string_view field) {
    const Datum* value = row.Find(field);
    if (!value || value->kind == Datum::Kind::Null) {
        return std::optional<int64_t>();
    }
    if (value->kind != Datum::Kind::Integer) {
        return Status::Invalid("Invalid optional integer metadata");
    }
    return std::optional<int64_t>(value->number);
}

class AvroSchema {
 public:
    explicit AvroSchema(PlanningResource* resource)
        : json_(resource), names_(resource), resource_(resource) {}
    Status Parse(std::string_view text) {
        PAIMON_RETURN_NOT_OK(json_.Parse(text));
        return Collect(json_.Root(), 0);
    }
    const Json& Root() const {
        return json_.Root();
    }
    Result<Datum> Read(const Json& schema, Cursor* cursor, size_t depth = 0,
                       bool skip = false) const {
        if (depth > kMaximumDepth) {
            return Status::Invalid("Recursive Avro metadata schema");
        }
        Datum result(resource_);
        if (schema.IsArray()) {
            PAIMON_ASSIGN_OR_RAISE(int64_t branch, cursor->Long());
            if (branch < 0 || static_cast<uint64_t>(branch) >= schema.Size()) {
                return Status::Invalid("Invalid Avro union branch");
            }
            return Read(schema[branch], cursor, depth + 1, skip);
        }
        const Json* type = schema.IsObject() ? Member(schema, "type") : &schema;
        if (!type || !type->IsString()) {
            return Status::Invalid("Invalid Avro metadata type");
        }
        std::string_view name = Text(*type);
        if (name == "null") {
            return result;
        }
        if (name == "int" || name == "long") {
            PAIMON_ASSIGN_OR_RAISE(result.number, cursor->Long());
            if (name == "int" && (result.number < INT32_MIN || result.number > INT32_MAX)) {
                return Status::Invalid("Avro int overflow");
            }
            result.kind = Datum::Kind::Integer;
        } else if (name == "boolean") {
            PAIMON_ASSIGN_OR_RAISE(std::string_view byte, cursor->Take(1));
            if (static_cast<uint8_t>(byte[0]) > 1) {
                return Status::Invalid("Invalid Avro boolean");
            }
            result.kind = Datum::Kind::Unsupported;
            result.number = byte[0];
        } else if (name == "string" || name == "bytes") {
            PAIMON_ASSIGN_OR_RAISE(result.bytes, cursor->Bytes());
            result.kind = Datum::Kind::Bytes;
        } else if (name == "float" || name == "double") {
            result.kind = Datum::Kind::Unsupported;
            PAIMON_RETURN_NOT_OK(cursor->Take(name == "float" ? 4 : 8).status());
        } else if (name == "record") {
            const Json* fields = Member(schema, "fields");
            if (!fields || !fields->IsArray()) {
                return Status::Invalid("Invalid Avro record fields");
            }
            result.kind = Datum::Kind::Record;
            result.schema = &schema;
            if (!skip) {
                result.children.reserve(fields->Size());
            }
            for (const auto& field : fields->GetArray()) {
                const Json* child = Member(field, "type");
                const Json* field_name = Member(field, "name");
                if (!child || !field_name || !field_name->IsString()) {
                    return Status::Invalid("Invalid Avro record field");
                }
                auto label = Text(*field_name);
                const bool discard = skip || label == "_KEY_STATS" || label == "_VALUE_STATS" ||
                                     label == "_PARTITION_STATS";
                PAIMON_ASSIGN_OR_RAISE(Datum value, Read(*child, cursor, depth + 1, discard));
                if (!skip) {
                    result.children.push_back(std::move(value));
                }
            }
        } else if (name == "array" || name == "map") {
            const Json* item = Member(schema, name == "array" ? "items" : "values");
            if (!item) {
                return Status::Invalid("Invalid Avro collection schema");
            }
            result.kind = Datum::Kind::Array;
            while (true) {
                PAIMON_ASSIGN_OR_RAISE(int64_t count, cursor->Long());
                if (count == 0) {
                    break;
                }
                std::optional<size_t> end;
                if (count < 0) {
                    if (count == INT64_MIN) {
                        return Status::Invalid("Avro collection count overflow");
                    }
                    count = -count;
                    PAIMON_ASSIGN_OR_RAISE(int64_t size, cursor->Long());
                    if (size < 0 || static_cast<uint64_t>(size) > cursor->Remaining()) {
                        return Status::Invalid("Invalid Avro collection block size");
                    }
                    end = cursor->Position() + size;
                }
                // Admitted metadata arrays contain a union tag or scalar per item.
                if (static_cast<uint64_t>(count) > cursor->Remaining() + 1) {
                    return Status::Invalid("Impossible Avro collection count");
                }
                for (int64_t i = 0; i < count; ++i) {
                    if (name == "map") {
                        PAIMON_RETURN_NOT_OK(cursor->Bytes().status());
                    }
                    PAIMON_ASSIGN_OR_RAISE(Datum value, Read(*item, cursor, depth + 1, skip));
                    if (!skip) {
                        result.children.push_back(std::move(value));
                    }
                }
                if (end && cursor->Position() != *end) {
                    return Status::Invalid("Avro collection block size mismatch");
                }
            }
        } else if (name == "fixed") {
            const Json* size = Member(schema, "size");
            if (!size || !size->IsUint64()) {
                return Status::Invalid("Invalid Avro fixed size");
            }
            PAIMON_ASSIGN_OR_RAISE(result.bytes, cursor->Take(size->GetUint64()));
            result.kind = Datum::Kind::Bytes;
        } else if (name == "enum") {
            const Json* symbols = Member(schema, "symbols");
            PAIMON_ASSIGN_OR_RAISE(result.number, cursor->Long());
            if (!symbols || !symbols->IsArray() || result.number < 0 ||
                static_cast<uint64_t>(result.number) >= symbols->Size()) {
                return Status::Invalid("Invalid Avro enum");
            }
            result.kind = Datum::Kind::Unsupported;
        } else {
            auto it = names_.find(name);
            if (it == names_.end()) {
                return Status::NotImplemented("Unknown Avro metadata schema reference");
            }
            return Read(*it->second, cursor, depth + 1, skip);
        }
        return result;
    }

 private:
    Status Collect(const Json& node, size_t depth) {
        if (depth > kMaximumDepth) {
            return Status::Invalid("Avro schema nesting limit exceeded");
        }
        if (node.IsArray()) {
            for (const auto& item : node.GetArray()) {
                PAIMON_RETURN_NOT_OK(Collect(item, depth + 1));
            }
        } else if (node.IsObject()) {
            const Json* type = Member(node, "type");
            if (!type) {
                return Status::Invalid("Missing Avro schema type");
            }
            const Json* name = Member(node, "name");
            if (name) {
                if (!name->IsString() || !names_.emplace(Text(*name), &node).second) {
                    return Status::Invalid("Duplicate Avro metadata type name");
                }
            }
            if (type->IsString() && Text(*type) == "record") {
                const Json* fields = Member(node, "fields");
                if (!fields || !fields->IsArray()) {
                    return Status::Invalid("Invalid Avro record schema");
                }
                for (auto field = fields->Begin(); field != fields->End(); ++field) {
                    const Json* child = Member(*field, "type");
                    const Json* label = Member(*field, "name");
                    if (!child || !label || !label->IsString()) {
                        return Status::Invalid("Missing Avro field name or type");
                    }
                    for (auto previous = fields->Begin(); previous != field; ++previous) {
                        const Json* other = Member(*previous, "name");
                        if (other && other->IsString() && Text(*other) == Text(*label)) {
                            return Status::Invalid("Duplicate Avro record field name");
                        }
                    }
                    PAIMON_RETURN_NOT_OK(Collect(*child, depth + 1));
                }
            } else {
                for (auto label : {"items", "values"}) {
                    const Json* child = Member(node, label);
                    if (child) {
                        PAIMON_RETURN_NOT_OK(Collect(*child, depth + 1));
                    }
                }
            }
        } else if (!node.IsString()) {
            return Status::Invalid("Invalid Avro schema");
        }
        return Status::OK();
    }
    JsonInput json_;
    std::pmr::unordered_map<std::string_view, const Json*> names_;
    PlanningResource* resource_;
};

// Validate known fields against their writer schema even for null/empty values.
// Value-only checks cannot distinguish an absent optional from an ignored float.
const Json* Unwrap(const Json& type) {
    if (!type.IsArray()) {
        return &type;
    }
    if (type.Size() != 2) {
        return nullptr;
    }
    for (size_t i = 0; i < 2; ++i) {
        if (type[i].IsString() && Text(type[i]) == "null") {
            return &type[1 - i];
        }
    }
    return nullptr;
}
bool Matches(const Json& schema, std::string_view expected, std::string_view items = {}) {
    const Json* value = Unwrap(schema);
    if (!value) {
        return false;
    }
    const Json* type = value->IsObject() ? Member(*value, "type") : value;
    if (!type || !type->IsString() || Text(*type) != expected) {
        return false;
    }
    if (!items.empty()) {
        const Json* item = Member(*value, "items");
        return item && Matches(*item, items);
    }
    return true;
}
const Json* FieldType(const Json& record, std::string_view name) {
    const Json* value = Unwrap(record);
    const Json* fields = value ? Member(*value, "fields") : nullptr;
    if (!fields || !fields->IsArray()) {
        return nullptr;
    }
    for (const auto& field : fields->GetArray()) {
        const Json* label = Member(field, "name");
        if (label && label->IsString() && Text(*label) == name) {
            return Member(field, "type");
        }
    }
    return nullptr;
}
Status ValidateMetadataSchema(const Json& root, bool manifest_entry) {
    if (!Matches(root, "record")) {
        return Status::Invalid("Metadata writer root must be a record");
    }
    auto required = [](const Json& record, std::string_view name, std::string_view type) -> Status {
        const Json* field = FieldType(record, name);
        if (!field || !Matches(*field, type)) {
            return Status::Invalid("Unsupported required metadata writer field type");
        }
        return Status::OK();
    };
    PAIMON_RETURN_NOT_OK(required(root, "_VERSION", "int"));
    if (!manifest_entry) {
        return required(root, "_FILE_NAME", "string");
    }
    PAIMON_RETURN_NOT_OK(required(root, "_KIND", "int"));
    PAIMON_RETURN_NOT_OK(required(root, "_PARTITION", "bytes"));
    PAIMON_RETURN_NOT_OK(required(root, "_BUCKET", "int"));
    PAIMON_RETURN_NOT_OK(required(root, "_TOTAL_BUCKETS", "int"));
    PAIMON_RETURN_NOT_OK(required(root, "_FILE", "record"));
    const Json& file = *FieldType(root, "_FILE");
    PAIMON_RETURN_NOT_OK(required(file, "_FILE_NAME", "string"));
    PAIMON_RETURN_NOT_OK(required(file, "_LEVEL", "int"));
    for (auto name : {"_FILE_SIZE", "_ROW_COUNT", "_SCHEMA_ID", "_MIN_SEQUENCE_NUMBER",
                      "_MAX_SEQUENCE_NUMBER"}) {
        PAIMON_RETURN_NOT_OK(required(file, name, "long"));
    }
    for (auto [name, type] : {std::pair{"_CREATION_TIME", "long"},
                              {"_DELETE_ROW_COUNT", "long"},
                              {"_FILE_SOURCE", "int"},
                              {"_FIRST_ROW_ID", "long"},
                              {"_EXTERNAL_PATH", "string"},
                              {"_EMBEDDED_FILE_INDEX", "bytes"}}) {
        if (const Json* field = FieldType(file, name); field && !Matches(*field, type)) {
            return Status::Invalid("Unsupported optional metadata writer field type");
        }
    }
    for (auto name :
         {"_EXTRA_FILES", "_VALUE_STATS_COLS", "_WRITE_COLS", "_WRITE_COLS_SEQUENCES"}) {
        const Json* field = FieldType(file, name);
        if (field &&
            !Matches(*field, "array",
                     std::string_view(name) == "_WRITE_COLS_SEQUENCES" ? "long" : "string")) {
            return Status::Invalid("Unsupported collection metadata writer field type");
        }
    }
    return Status::OK();
}

template <typename Consumer>
Status ReadOcf(const std::pmr::string& path, PlanningResource* resource, bool manifest_entry,
               Consumer consumer) {
    PAIMON_ASSIGN_OR_RAISE(std::pmr::vector<char> file, ReadFile(path, resource));
    Cursor cursor({file.data(), file.size()});
    PAIMON_ASSIGN_OR_RAISE(std::string_view magic, cursor.Take(4));
    if (magic != std::string_view("Obj\1", 4)) {
        return Status::NotImplemented("Bounded planning requires Avro OCF metadata");
    }
    std::string_view schema_text;
    std::string_view codec = "null";
    bool has_schema = false;
    bool has_codec = false;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(int64_t count, cursor.Long());
        if (count == 0) {
            break;
        }
        std::optional<size_t> end;
        if (count < 0) {
            if (count == INT64_MIN) {
                return Status::Invalid("Avro header count overflow");
            }
            count = -count;
            PAIMON_ASSIGN_OR_RAISE(int64_t size, cursor.Long());
            if (size < 0 || static_cast<uint64_t>(size) > cursor.Remaining()) {
                return Status::Invalid("Invalid Avro header block");
            }
            end = cursor.Position() + size;
        }
        if (static_cast<uint64_t>(count) > cursor.Remaining() / 2) {
            return Status::Invalid("Impossible Avro header count");
        }
        for (int64_t i = 0; i < count; ++i) {
            PAIMON_ASSIGN_OR_RAISE(std::string_view key, cursor.Bytes());
            PAIMON_ASSIGN_OR_RAISE(std::string_view value, cursor.Bytes());
            if (key == "avro.schema") {
                if (has_schema) {
                    return Status::Invalid("Duplicate Avro schema header");
                }
                schema_text = value;
                has_schema = true;
            } else if (key == "avro.codec") {
                if (has_codec) {
                    return Status::Invalid("Duplicate Avro codec header");
                }
                codec = value;
                has_codec = true;
            }
        }
        if (end && cursor.Position() != *end) {
            return Status::Invalid("Avro header block size mismatch");
        }
    }
    if (!has_schema) {
        return Status::Invalid("Missing Avro metadata schema");
    }
    if (codec != "null" && codec != "zstandard") {
        return Status::NotImplemented("Unsupported bounded metadata codec");
    }
    PAIMON_ASSIGN_OR_RAISE(std::string_view sync, cursor.Take(16));
    AvroSchema schema(resource);
    PAIMON_RETURN_NOT_OK(schema.Parse(schema_text));
    PAIMON_RETURN_NOT_OK(ValidateMetadataSchema(schema.Root(), manifest_entry));
    std::pmr::vector<uint64_t> workspace(resource);
    while (cursor.Remaining() != 0) {
        PAIMON_ASSIGN_OR_RAISE(int64_t count, cursor.Long());
        PAIMON_ASSIGN_OR_RAISE(int64_t length, cursor.Long());
        if (count <= 0 || length < 0) {
            return Status::Invalid("Invalid Avro data block");
        }
        PAIMON_ASSIGN_OR_RAISE(std::string_view compressed, cursor.Take(length));
        PAIMON_ASSIGN_OR_RAISE(std::string_view marker, cursor.Take(16));
        if (marker != sync) {
            return Status::Invalid("Avro sync marker mismatch");
        }
        std::pmr::vector<char> output(resource);
        std::string_view payload = compressed;
        if (codec == "zstandard") {
            if (ZSTD_getFrameContentSize(compressed.data(), compressed.size()) ==
                    ZSTD_CONTENTSIZE_ERROR ||
                ZSTD_getDictID_fromFrame(compressed.data(), compressed.size()) != 0 ||
                ZSTD_findFrameCompressedSize(compressed.data(), compressed.size()) !=
                    compressed.size()) {
                return Status::Invalid("Invalid bounded Zstandard frame");
            }
            const size_t required =
                ZSTD_estimateDStreamSize_fromFrame(compressed.data(), compressed.size());
            if (ZSTD_isError(required) ||
                required > static_cast<size_t>(PTRDIFF_MAX) - sizeof(uint64_t)) {
                return Status::Invalid("Invalid bounded Zstandard workspace size");
            }
            workspace.resize((required + sizeof(uint64_t) - 1) / sizeof(uint64_t));
            auto* stream =
                ZSTD_initStaticDStream(workspace.data(), workspace.size() * sizeof(uint64_t));
            if (!stream || ZSTD_isError(ZSTD_initDStream(stream))) {
                return Status::Invalid("Cannot initialize bounded Zstandard decoder");
            }
            ZSTD_inBuffer input{compressed.data(), compressed.size(), 0};
            size_t remaining = 1;
            while (remaining != 0) {
                const size_t before = output.size();
                if (before > static_cast<size_t>(PTRDIFF_MAX) - 65536) {
                    return Status::CapacityError("Decoded metadata exceeds addressable storage");
                }
                output.resize(before + 65536);
                ZSTD_outBuffer destination{output.data() + before, output.size() - before, 0};
                const size_t consumed = input.pos;
                remaining = ZSTD_decompressStream(stream, &destination, &input);
                if (ZSTD_isError(remaining) ||
                    (remaining != 0 && input.pos == consumed && destination.pos == 0)) {
                    return Status::Invalid("Cannot decode bounded Zstandard frame");
                }
                output.resize(before + destination.pos);
            }
            if (input.pos != input.size) {
                return Status::Invalid("Trailing bounded Zstandard frame bytes");
            }
            payload = {output.data(), output.size()};
        }
        if (static_cast<uint64_t>(count) > payload.size()) {
            return Status::Invalid("Impossible Avro record count");
        }
        Cursor records(payload);
        for (int64_t i = 0; i < count; ++i) {
            PAIMON_ASSIGN_OR_RAISE(Datum row, schema.Read(schema.Root(), &records));
            PAIMON_RETURN_NOT_OK(consumer(row));
        }
        if (records.Remaining() != 0) {
            return Status::Invalid("Avro record count or block size mismatch");
        }
    }
    return Status::OK();
}

// The only ordinary-STL variable allocations in finite planning are exact-size
// native publication constructors. This profile is qualified for the libstdc++
// C++11 ABI: basic_string's forward-range/copy constructors request n+1 chars;
// vector(n) requests n elements. See the source/allocator-observed proof in docs.
std::string NativeString(std::string_view text, const std::shared_ptr<PlanningBudget>& budget) {
    PlanningBudgetAccess::Reserve(budget, text.size() + 1);
    return std::string(text.data(), text.size());
}

struct Entry {
    explicit Entry(PlanningResource* resource) : name(resource) {}
    std::pmr::string name;
    int32_t bucket = 0;
    int32_t total_buckets = 0;
    int32_t level = 0;
    int64_t file_size = 0;
    int64_t rows = 0;
    int64_t schema_id = 0;
    int64_t min_sequence = 0;
    int64_t max_sequence = 0;
    int64_t creation = 0;
    std::optional<int64_t> first_row;
    std::optional<FileSource> source;
};

class BudgetPlan : public Plan {
 public:
    BudgetPlan(int64_t snapshot, std::vector<std::shared_ptr<Split>> splits)
        : snapshot_(snapshot), splits_(std::move(splits)) {}
    std::optional<int64_t> SnapshotId() const override {
        return snapshot_;
    }
    const std::vector<std::shared_ptr<Split>>& Splits() const override {
        return splits_;
    }

 private:
    int64_t snapshot_;
    std::vector<std::shared_ptr<Split>> splits_;
};

class BoundedTableScan : public TableScan {
 public:
    BoundedTableScan(std::unique_ptr<ScanContext> context, std::shared_ptr<PlanningBudget> budget)
        : context_(std::move(context)), resource_(std::move(budget)) {}

    Result<std::shared_ptr<Plan>> CreatePlan() override {
        if (failure_) {
            return *failure_;
        }
        if (finished_) {
            return Status::Invalid("end of scan");
        }
        finished_ = true;
        try {
            auto result = BuildPlan();
            if (!result.ok()) {
                failure_ = result.status();
            }
            return result;
        } catch (const std::bad_alloc&) {
            PlanningBudgetAccess::MarkAllocationFailure(resource_.Budget());
            failure_ = PlanningBudgetAccess::Failure(resource_.Budget());
        } catch (const std::length_error&) {
            failure_ =
                Status::CapacityError("Planning container length exceeds addressable storage");
        }
        return *failure_;
    }

 private:
    std::string_view Option(const Json& schema, std::string_view key,
                            std::string_view fallback = {}) const {
        for (const auto& item : context_->GetOptions()) {
            if (item.first == key) {
                return item.second;
            }
        }
        const Json* options = Member(schema, "options");
        const Json* value = options ? Member(*options, key) : nullptr;
        return value && value->IsString() ? Text(*value) : fallback;
    }
    Result<std::shared_ptr<Plan>> BuildPlan() {
        JsonInput schema(&resource_);
        PAIMON_RETURN_NOT_OK(schema.Parse(*context_->GetSpecificTableSchema()));
        const Json& table = schema.Root();
        const Json* schema_id = Member(table, "id");
        const Json* keys = Member(table, "primaryKeys");
        const Json* partitions = Member(table, "partitionKeys");
        if (!schema_id || !schema_id->IsInt64() || !keys || !keys->IsArray() || !keys->Empty() ||
            !partitions || !partitions->IsArray() || !partitions->Empty()) {
            return Status::NotImplemented(
                "Bounded planning requires an unpartitioned append-only schema");
        }
        if (Option(table, "bucket", "-1") != "-1" || Option(table, "branch", "main") != "main" ||
            Option(table, "manifest.format", "avro") != "avro" ||
            Option(table, "data-evolution.enabled", "false") != "false" ||
            Option(table, "deletion-vectors.enabled", "false") != "false") {
            return Status::NotImplemented("Unsupported bounded planning profile");
        }
        auto check_option = [](std::string_view key) -> Status {
            if (key.substr(0, 5) == "scan." && key != "scan.snapshot-id") {
                return Status::NotImplemented(
                    "Bounded planning requires explicit numeric snapshot selection");
            }
            return Status::OK();
        };
        if (const Json* options = Member(table, "options")) {
            if (!options->IsObject()) {
                return Status::Invalid("Invalid retained schema options");
            }
            for (const auto& item : options->GetObject()) {
                if (!item.value.IsString()) {
                    return Status::Invalid("Invalid retained schema option value");
                }
                PAIMON_RETURN_NOT_OK(check_option(Text(item.name)));
            }
        }
        for (const auto& item : context_->GetOptions()) {
            PAIMON_RETURN_NOT_OK(check_option(item.first));
        }
        auto format = Option(table, "file.format", "parquet");
        if (format != "parquet" && format != "orc") {
            return Status::NotImplemented("Unsupported bounded data format");
        }
        std::string_view snapshot_text = Option(table, "scan.snapshot-id");
        int64_t snapshot_id = 0;
        if (snapshot_text.empty()) {
            return Status::Invalid("Missing bound numeric snapshot id");
        }
        auto parsed = std::from_chars(snapshot_text.data(),
                                      snapshot_text.data() + snapshot_text.size(), snapshot_id);
        if (parsed.ec != std::errc() || parsed.ptr != snapshot_text.data() + snapshot_text.size() ||
            snapshot_id < 1) {
            return Status::Invalid("Bounded planning requires a positive numeric snapshot id");
        }
        std::pmr::string root(context_->GetPath(), &resource_);
        if (std::string_view(root).substr(0, 7) == "file://") {
            root.erase(0, 7);
        }
        if (root.find("://") != std::pmr::string::npos || root.empty()) {
            return Status::NotImplemented("Bounded planning currently supports local files");
        }
        while (root.size() > 1 && root.back() == '/') {
            root.pop_back();
        }
        std::pmr::string snapshot_path(root, &resource_);
        snapshot_path += "/snapshot/snapshot-";
        char id[32];
        auto id_result = std::to_chars(id, id + sizeof(id), snapshot_id);
        snapshot_path.append(id, id_result.ptr);
        PAIMON_ASSIGN_OR_RAISE(std::pmr::vector<char> snapshot_bytes,
                               ReadFile(snapshot_path, &resource_));
        JsonInput snapshot(&resource_);
        PAIMON_RETURN_NOT_OK(snapshot.Parse({snapshot_bytes.data(), snapshot_bytes.size()}));
        const Json* actual_id = Member(snapshot.Root(), "id");
        const Json* actual_schema = Member(snapshot.Root(), "schemaId");
        if (!actual_id || !actual_id->IsInt64() || actual_id->GetInt64() != snapshot_id ||
            !actual_schema || !actual_schema->IsInt64() ||
            actual_schema->GetInt64() != schema_id->GetInt64()) {
            return Status::Invalid("Bound snapshot/schema identity mismatch");
        }
        std::pmr::vector<std::pmr::string> manifests(&resource_);
        for (auto label : {"baseManifestList", "deltaManifestList"}) {
            const Json* name = Member(snapshot.Root(), label);
            if (!name || !name->IsString() || !Component(Text(*name))) {
                return Status::Invalid("Invalid bound manifest-list identity");
            }
            std::pmr::string path(root, &resource_);
            path += "/manifest/";
            path.append(name->GetString(), name->GetStringLength());
            PAIMON_RETURN_NOT_OK(ReadOcf(path, &resource_, false, [&](const Datum& row) -> Status {
                PAIMON_ASSIGN_OR_RAISE(int64_t version, Integer(row, "_VERSION"));
                PAIMON_ASSIGN_OR_RAISE(std::string_view filename, String(row, "_FILE_NAME"));
                if (version != 2 || !Component(filename)) {
                    return Status::Invalid("Invalid manifest-list record");
                }
                manifests.emplace_back(filename);
                return Status::OK();
            }));
        }
        std::pmr::vector<Entry> entries(&resource_);
        for (const auto& manifest : manifests) {
            std::pmr::string path(root, &resource_);
            path += "/manifest/";
            path += manifest;
            PAIMON_RETURN_NOT_OK(ReadOcf(path, &resource_, true, [&](const Datum& row) -> Status {
                PAIMON_ASSIGN_OR_RAISE(int64_t version, Integer(row, "_VERSION"));
                PAIMON_ASSIGN_OR_RAISE(int64_t kind, Integer(row, "_KIND"));
                if (version != 2) {
                    return Status::NotImplemented("Unsupported manifest version");
                }
                if (kind == 1) {
                    return Status::NotImplemented(
                        "Bounded ADD-only profile does not admit delete manifests");
                }
                if (kind != 0) {
                    return Status::Invalid("Invalid manifest row kind");
                }
                PAIMON_ASSIGN_OR_RAISE(std::string_view partition, String(row, "_PARTITION"));
                if (partition.size() != 12 || std::any_of(partition.begin(), partition.end(),
                                                          [](char c) { return c != 0; })) {
                    return Status::NotImplemented("Bounded planning requires empty partitions");
                }
                const Datum* file = row.Find("_FILE");
                if (!file || file->kind != Datum::Kind::Record) {
                    return Status::Invalid("Missing file metadata");
                }
                Entry entry(&resource_);
                PAIMON_ASSIGN_OR_RAISE(std::string_view filename, String(*file, "_FILE_NAME"));
                const auto dot = filename.find_last_of('.');
                if (!Component(filename) || dot == std::string_view::npos ||
                    filename.substr(dot + 1) != format) {
                    return Status::NotImplemented(
                        "Bounded planning requires Parquet or ORC data files");
                }
                entry.name = filename;
                PAIMON_ASSIGN_OR_RAISE(int64_t bucket, Integer(row, "_BUCKET"));
                PAIMON_ASSIGN_OR_RAISE(int64_t total, Integer(row, "_TOTAL_BUCKETS"));
                PAIMON_ASSIGN_OR_RAISE(int64_t level, Integer(*file, "_LEVEL"));
                if (bucket < 0 || bucket > INT32_MAX || total < INT32_MIN || total > INT32_MAX ||
                    level < 0 || level > INT32_MAX) {
                    return Status::Invalid("Invalid file bucket or level");
                }
                entry.bucket = bucket;
                entry.total_buckets = total;
                entry.level = level;
                PAIMON_ASSIGN_OR_RAISE(entry.file_size, Integer(*file, "_FILE_SIZE"));
                PAIMON_ASSIGN_OR_RAISE(entry.rows, Integer(*file, "_ROW_COUNT"));
                PAIMON_ASSIGN_OR_RAISE(entry.schema_id, Integer(*file, "_SCHEMA_ID"));
                PAIMON_ASSIGN_OR_RAISE(entry.min_sequence, Integer(*file, "_MIN_SEQUENCE_NUMBER"));
                PAIMON_ASSIGN_OR_RAISE(entry.max_sequence, Integer(*file, "_MAX_SEQUENCE_NUMBER"));
                PAIMON_ASSIGN_OR_RAISE(entry.first_row, OptionalInteger(*file, "_FIRST_ROW_ID"));
                PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> creation,
                                       OptionalInteger(*file, "_CREATION_TIME"));
                entry.creation = creation.value_or(0);
                PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> deleted,
                                       OptionalInteger(*file, "_DELETE_ROW_COUNT"));
                PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> source,
                                       OptionalInteger(*file, "_FILE_SOURCE"));
                if (source) {
                    if (*source < 0 || *source > 1) {
                        return Status::Invalid("Invalid data file source");
                    }
                    PAIMON_ASSIGN_OR_RAISE(entry.source, FileSource::FromByteValue(*source));
                }
                if (entry.file_size < 0 || entry.rows < 0 || (deleted && *deleted != 0) ||
                    (entry.first_row &&
                     (*entry.first_row < 0 || entry.rows > INT64_MAX - *entry.first_row)) ||
                    entry.schema_id != schema_id->GetInt64() ||
                    !EmptyArray(*file, "_EXTRA_FILES") || !Absent(*file, "_EMBEDDED_FILE_INDEX") ||
                    !Absent(*file, "_EXTERNAL_PATH") || !Absent(*file, "_WRITE_COLS") ||
                    !Absent(*file, "_WRITE_COLS_SEQUENCES")) {
                    return Status::NotImplemented("Unsupported bounded data-file metadata");
                }
                entries.push_back(std::move(entry));
                return Status::OK();
            }));
        }
        std::pmr::vector<size_t> sorted(entries.size(), &resource_);
        for (size_t i = 0; i < entries.size(); ++i) {
            sorted[i] = i;
        }
        auto key = [&](size_t i) {
            return std::tie(entries[i].bucket, entries[i].level, entries[i].name);
        };
        std::sort(sorted.begin(), sorted.end(),
                  [&](size_t left, size_t right) { return key(left) < key(right); });
        for (size_t i = 1; i < sorted.size(); ++i) {
            if (key(sorted[i - 1]) == key(sorted[i])) {
                return Status::Invalid("Duplicate ADD metadata file");
            }
        }
        if (entries.size() > SIZE_MAX / sizeof(std::shared_ptr<Split>)) {
            throw std::bad_alloc();
        }
        PlanningBudgetAccess::Reserve(resource_.Budget(),
                                      entries.size() * sizeof(std::shared_ptr<Split>));
        std::vector<std::shared_ptr<Split>> splits(entries.size());
        for (size_t i = 0; i < entries.size(); ++i) {
            const Entry& entry = entries[i];
            std::string filename = NativeString(entry.name, resource_.Budget());
            // DataFileMeta copies its sole nonempty string. Other admitted fields
            // below are empty/optional scalars; the shared block is allocator-routed.
            PlanningBudgetAccess::Reserve(resource_.Budget(), entry.name.size() + 1);
            auto meta = std::allocate_shared<DataFileMeta>(
                PlanningAllocator<DataFileMeta>(resource_.Budget()), filename, entry.file_size,
                entry.rows, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(), SimpleStats::EmptyStats(),
                SimpleStats::EmptyStats(), entry.min_sequence, entry.max_sequence, entry.schema_id,
                entry.level, std::vector<std::optional<std::string>>(),
                Timestamp(entry.creation, 0), std::optional<int64_t>(0), nullptr, entry.source,
                std::nullopt, std::nullopt, entry.first_row, std::nullopt, std::nullopt);
            PlanningBudgetAccess::Reserve(resource_.Budget(),
                                          sizeof(std::shared_ptr<DataFileMeta>));
            std::vector<std::shared_ptr<DataFileMeta>> files(1);
            files[0] = std::move(meta);
            std::pmr::string bucket_path(root, &resource_);
            bucket_path += "/bucket-";
            char bucket_digits[32];
            auto number =
                std::to_chars(bucket_digits, bucket_digits + sizeof(bucket_digits), entry.bucket);
            bucket_path.append(bucket_digits, number.ptr);
            std::string native_path = NativeString(bucket_path, resource_.Budget());
#if defined(__GLIBCXX__) && _GLIBCXX_USE_CXX11_ABI
            PlanningBudgetAccess::Reserve(
                resource_.Budget(),
                sizeof(DataSplitImpl) +
                    sizeof(std::_Sp_counted_ptr<DataSplitImpl*, __gnu_cxx::_S_atomic>) +
                    native_path.size() + 1);
#endif
            DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), entry.bucket, native_path,
                                           std::move(files));
            builder.WithSnapshot(snapshot_id)
                .WithTotalBuckets(entry.total_buckets)
                .RawConvertible(true);
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataSplitImpl> split, builder.Build());
            splits[i] = std::move(split);
        }
        PlanningBudgetAccess::Reserve(resource_.Budget(), 0);
        std::shared_ptr<Plan> plan = std::allocate_shared<BudgetPlan>(
            PlanningAllocator<BudgetPlan>(resource_.Budget()), snapshot_id, std::move(splits));
        return plan;
    }
    std::unique_ptr<ScanContext> context_;
    PlanningResource resource_;
    bool finished_ = false;
    std::optional<Status> failure_;
};

}  // namespace
#endif

Result<std::unique_ptr<TableScan>> TableScan::CreateWithPlanningBudget(
    std::unique_ptr<ScanContext> context, const std::shared_ptr<PlanningBudget>& budget) {
#if !defined(__GLIBCXX__) || !_GLIBCXX_USE_CXX11_ABI || !defined(__linux__) || _GLIBCXX_RELEASE < 9
    return Status::NotImplemented(
        "Bounded planning requires a qualified Linux libstdc++ 9+ C++11 ABI build");
#else
    if (!context || !budget) {
        return Status::Invalid("Bounded planning requires context and budget");
    }
    if (!context->GetSpecificTableSchema() || context->GetSpecificFileSystem() ||
        context->IsStreamingMode() || context->GetFormatTable() || context->GetCatalog() ||
        context->GetRealtimeContext() || context->GetGlobalIndexResult() ||
        context->GetIdentifier() || context->GetLimit()) {
        return Status::NotImplemented("Unsupported bounded scan context");
    }
    const auto& filter = context->GetScanFilters();
    if (filter && (filter->GetPredicate() || filter->GetBucketFilter() ||
                   !filter->GetPartitionFilters().empty())) {
        return Status::NotImplemented("Bounded planning does not admit planning filters");
    }
    if (context->GetMemoryPool() != PlanningBudgetAccess::Upstream(budget)) {
        return Status::Invalid("Planning budget and scan must use the same upstream allocator");
    }
    try {
        // Retaining caller-created input is charged before accepting the scan.
        PlanningBudgetAccess::Reserve(budget, sizeof(ScanContext) + sizeof(BoundedTableScan));
        PlanningBudgetAccess::Reserve(budget, context->GetPath().capacity() + 1);
        PlanningBudgetAccess::Reserve(budget, context->GetSpecificTableSchema()->capacity() + 1);
        if (filter) {
            PlanningBudgetAccess::Reserve(
                budget, sizeof(std::_Sp_counted_ptr_inplace<ScanFilter, std::allocator<void>,
                                                            __gnu_cxx::_S_atomic>));
        }
        using OptionNode = std::_Rb_tree_node<std::pair<const std::string, std::string>>;
        for (const auto& option : context->GetOptions()) {
            PlanningBudgetAccess::Reserve(budget, sizeof(OptionNode));
            PlanningBudgetAccess::Reserve(budget, option.first.capacity() + 1);
            PlanningBudgetAccess::Reserve(budget, option.second.capacity() + 1);
        }
        // Shared immutable SDK constants are initialized outside the per-query
        // payload arena; their fixed process lifetime is documented separately.
        (void)BinaryRow::EmptyRow();
        (void)SimpleStats::EmptyStats();
        return std::unique_ptr<TableScan>(new BoundedTableScan(std::move(context), budget));
    } catch (const std::bad_alloc&) {
        PlanningBudgetAccess::MarkAllocationFailure(budget);
        return PlanningBudgetAccess::Failure(budget);
    }
#endif
}
}  // namespace paimon
