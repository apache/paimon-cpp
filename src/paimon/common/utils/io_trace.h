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

// TEMPORARY DIAGNOSTIC INSTRUMENTATION.
//
// Traces every underlying IO issued by the read ahead cache and every wait a
// reader spends on a cache fetch, so that the read pattern of a run can be
// analysed offline (is the prefetch issued early enough, is the underlying
// filesystem serialising the concurrent fetches, how much of the read latency
// is actually hidden). Not part of the production read path: remove together
// with its call sites once the analysis is done.
//
// Off unless PAIMON_IO_TRACE is set to something other than "0". When on, one
// line per event goes to stderr, in a fixed key=value shape meant for awk:
//
//   [paimon-io-trace] event=prefetch-done ts=2026-09-04T15:04:05.123456
//       epoch_us=1788513245123456 elapsed_us=8321 off=1048576 len=262144
//       inflight=3 tid=1234567 status=ok file=oss://bucket/.../data-0.parquet
//
// `epoch_us`/`ts` are the absolute wall clock time the event was TRIGGERED at
// (the dispatch of the IO, the start of the wait), so a `*-done` line carries
// both the trigger time and the duration and can be read on its own.
// `elapsed_us` and `inflight` are -1 when they do not apply to the event.

#pragma once

#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#if defined(__linux__)
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace paimon {
namespace io_trace {

/// Value reported for a field that does not apply to an event.
constexpr int64_t kNotApplicable = -1;

/// Whether the tracing is turned on, read once from PAIMON_IO_TRACE.
inline bool Enabled() {
    static const bool enabled = []() {
        const char* value = std::getenv("PAIMON_IO_TRACE");
        return value != nullptr && value[0] != '\0' && std::strcmp(value, "0") != 0;
    }();
    return enabled;
}

/// The moment an event was triggered at: the wall clock for the absolute
/// timestamp of the line, the steady clock for the duration.
struct Instant {
    int64_t wall_micros = 0;
    std::chrono::steady_clock::time_point mono{};
};

inline Instant Now() {
    Instant instant;
    instant.wall_micros = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();
    instant.mono = std::chrono::steady_clock::now();
    return instant;
}

/// Microseconds from `start` until now.
inline int64_t ElapsedMicros(const Instant& start) {
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
                                                                 start.mono)
        .count();
}

/// The IOs this process has dispatched to the underlying streams and not seen
/// completed yet, counted across all the caches and files: the number to look at
/// when asking whether the filesystem below runs the fetches concurrently.
inline std::atomic<int64_t>& InflightCounter() {
    static std::atomic<int64_t> inflight{0};
    return inflight;
}

/// @return the in-flight count after the increment.
inline int64_t EnterInflight() {
    return InflightCounter().fetch_add(1) + 1;
}

/// @return the in-flight count after the decrement.
inline int64_t LeaveInflight() {
    return InflightCounter().fetch_sub(1) - 1;
}

/// The uri of the stream, resolved once so that the per-IO lines do not pay for
/// it. Empty when the tracing is off.
inline std::string Uri(const std::shared_ptr<InputStream>& stream) {
    if (!Enabled() || stream == nullptr) {
        return std::string();
    }
    Result<std::string> uri = stream->GetUri();
    return uri.ok() && !uri.value().empty() ? uri.value() : std::string("<unknown>");
}

inline std::string FormatWallMicros(int64_t wall_micros) {
    const std::time_t seconds = static_cast<std::time_t>(wall_micros / 1000000);
    std::tm broken_down{};
    localtime_r(&seconds, &broken_down);
    char buffer[48];
    const size_t length = std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%S", &broken_down);
    std::snprintf(buffer + length, sizeof(buffer) - length, ".%06d",
                  static_cast<int>(wall_micros % 1000000));
    return std::string(buffer);
}

/// Identifier of the calling thread, resolved once per thread. The OS thread id
/// where there is one, so that the lines can be lined up with the output of the
/// system tools.
inline uint64_t ThreadId() {
#if defined(__linux__)
    static thread_local const uint64_t id = static_cast<uint64_t>(::syscall(SYS_gettid));
#else
    static thread_local const uint64_t id =
        static_cast<uint64_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
#endif
    return id;
}

/// Fold the whitespace of a value into underscores, so that one field stays one
/// token: a failure status is a sentence.
inline std::string SanitizeToken(const char* value) {
    if (value == nullptr || value[0] == '\0') {
        return std::string("-");
    }
    std::string token(value);
    for (char& c : token) {
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
            c = '_';
        }
    }
    return token;
}

/// Write one event line to stderr.
///
/// The line is assembled first and written with a single call, so that the lines
/// of the concurrent readers do not interleave.
/// @param event What happened, see the events used by the cache.
/// @param file Uri of the file the event is about, from Uri().
/// @param offset Offset the IO reads at, or the read is served from.
/// @param length Bytes the IO reads, or the read asks for.
/// @param triggered_at When the event was triggered, from Now().
/// @param elapsed_micros How long it took, or kNotApplicable.
/// @param inflight In-flight IOs at that point, or kNotApplicable.
/// @param status Outcome of the event, or null when it has none.
inline void Emit(const char* event, const std::string& file, uint64_t offset, uint64_t length,
                 const Instant& triggered_at, int64_t elapsed_micros, int64_t inflight,
                 const char* status) {
    char head[384];
    const int written = std::snprintf(
        head, sizeof(head),
        "[paimon-io-trace] event=%s ts=%s epoch_us=%" PRId64 " elapsed_us=%" PRId64 " off=%" PRIu64
        " len=%" PRIu64 " inflight=%" PRId64 " tid=%" PRIu64 " status=%s file=",
        event, FormatWallMicros(triggered_at.wall_micros).c_str(), triggered_at.wall_micros,
        elapsed_micros, offset, length, inflight, ThreadId(), SanitizeToken(status).c_str());
    std::string line(head, written > 0 ? static_cast<size_t>(written) : 0);
    line += file.empty() ? "-" : file;
    line += '\n';
    std::fwrite(line.data(), 1, line.size(), stderr);
}

}  // namespace io_trace
}  // namespace paimon
