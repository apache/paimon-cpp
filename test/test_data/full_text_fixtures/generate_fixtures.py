#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Generates the full-text index cross-read fixtures, or verifies archives written by Paimon C++.

Requires the `paimon-ftindex==0.1.0` wheel, which wraps the same native engine as Java
`paimon-full-text`. Rows are added the way Java `NativeFullTextGlobalIndexWriter` adds them: a
null value advances the row id but is not indexed.

Usage:
  python3 generate_fixtures.py <output-dir>
      Writes the fixtures to <output-dir> and prints the golden results of every search.
  python3 generate_fixtures.py --verify <archive-dir>
      Reads the archives in <archive-dir> written by Paimon C++ and checks that every search
      returns the same row ids and scores as the checked-in fixtures.
"""

import json
import os
import struct
import sys

from paimon_ftindex import FullTextIndexReader, FullTextIndexWriter

FIXTURES = [
    {
        "name": "default",
        "options": {},
        "rows": [
            "Apache Paimon is a streaming data lake platform",
            None,
            "Paimon supports real-time data ingestion and running queries",
            "The runner runs quickly",
            "Lake formats store huge tables",
        ],
        "searches": [
            {"query": {"match": {"query": "paimon"}}, "limit": 10},
            {"query": {"match": {"query": "run"}}, "limit": 10},
            {"query": {"match": {"query": "paimon lake", "operator": "And"}}, "limit": 10},
            {"query": {"match": {"query": "paimon lake"}}, "limit": 1},
            {"query": {"match_phrase": {"query": "data lake"}}, "limit": 10},
            {
                "query": {
                    "boolean": {
                        "must": [{"match": {"query": "paimon"}}],
                        "must_not": [{"match": {"query": "streaming"}}],
                    }
                },
                "limit": 10,
            },
            {"query": {"match": {"query": "lake"}}, "limit": 10, "filter": [0, 3]},
        ],
    },
    {
        "name": "ngram",
        "options": {"tokenizer": "ngram", "ngram.min-gram": "2", "ngram.max-gram": "3"},
        "rows": ["paimon", "lakehouse", None, "streaming"],
        "searches": [
            {"query": {"match": {"query": "aim"}}, "limit": 10},
            {"query": {"match": {"query": "house"}}, "limit": 10},
        ],
    },
    {
        "name": "jieba",
        "options": {"tokenizer": "jieba"},
        "rows": [
            "张华在百货公司当售货员",
            "Apache Paimon supports full text search",
            None,
            "我们在数据湖中存储数据",
        ],
        "searches": [
            {"query": {"match": {"query": "售货员"}}, "limit": 10},
            {"query": {"match": {"query": "数据湖"}}, "limit": 10},
            {"query": {"match": {"query": "paimon"}}, "limit": 10},
        ],
    },
]


def roaring32_portable(values):
    """Portable 32-bit Roaring format with array containers only."""
    groups = {}
    for value in sorted(values):
        groups.setdefault(value >> 16, []).append(value & 0xFFFF)
    keys = sorted(groups)
    out = struct.pack("<II", 12346, len(keys))
    for key in keys:
        out += struct.pack("<HH", key, len(groups[key]) - 1)
    offset = 8 + 8 * len(keys)
    for key in keys:
        out += struct.pack("<I", offset)
        offset += 2 * len(groups[key])
    for key in keys:
        out += b"".join(struct.pack("<H", low) for low in groups[key])
    return out


def roaring64_portable(values):
    """Portable 64-bit Roaring format (`RoaringTreemap`, CRoaring `Roaring64Map`)."""
    buckets = {}
    for value in values:
        buckets.setdefault(value >> 32, []).append(value & 0xFFFFFFFF)
    out = struct.pack("<Q", len(buckets))
    for high in sorted(buckets):
        out += struct.pack("<I", high) + roaring32_portable(buckets[high])
    return out


class FileInput:
    def __init__(self, file):
        self._file = file

    def pread(self, pos, length):
        self._file.seek(pos)
        return self._file.read(length)


def search_all(path, fixture):
    results = []
    with open(path, "rb") as input_file, FullTextIndexReader(FileInput(input_file)) as reader:
        for search in fixture["searches"]:
            query = json.dumps(search["query"], ensure_ascii=False, separators=(",", ":"))
            filter_bytes = None
            if "filter" in search:
                filter_bytes = roaring64_portable(search["filter"])
            row_ids, scores = reader.search(query, search["limit"], filter_bytes)
            results.append(
                {
                    "query": query,
                    "limit": search["limit"],
                    "filter": search.get("filter"),
                    "row_ids": row_ids,
                    "scores": [float("%.9g" % score) for score in scores],
                }
            )
    return results


def generate(out_dir):
    os.makedirs(out_dir, exist_ok=True)
    golden = {}
    for fixture in FIXTURES:
        path = os.path.join(out_dir, fixture["name"] + ".archive")
        with FullTextIndexWriter(fixture["options"]) as writer:
            for row_id, text in enumerate(fixture["rows"]):
                if text is not None:
                    writer.add_document(row_id, text)
            with open(path, "wb") as output:
                writer.write(output)
        golden[fixture["name"]] = {
            "options": fixture["options"],
            "row_count": len(fixture["rows"]),
            "searches": search_all(path, fixture),
        }
    print(json.dumps(golden, ensure_ascii=False, indent=2))


def same_results(expected, actual):
    return expected["row_ids"] == actual["row_ids"] and all(
        abs(e - a) <= 1e-5 for e, a in zip(expected["scores"], actual["scores"])
    )


def verify(archive_dir):
    fixture_dir = os.path.dirname(os.path.abspath(__file__))
    failures = 0
    for fixture in FIXTURES:
        file_name = fixture["name"] + ".archive"
        expected = search_all(os.path.join(fixture_dir, file_name), fixture)
        actual = search_all(os.path.join(archive_dir, file_name), fixture)
        for expected_result, actual_result in zip(expected, actual):
            if not same_results(expected_result, actual_result):
                failures += 1
                print(f"{fixture['name']}: expected {expected_result}, got {actual_result}")
    if failures:
        sys.exit(f"{failures} searches returned unexpected results")
    print("all searches returned the expected results")


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--verify":
        verify(sys.argv[2])
    elif len(sys.argv) == 2:
        generate(sys.argv[1])
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
