<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Full-text index cross-read fixtures

These archives are read by `full_text_global_index_test.cpp` (`paimon-full-text-index-test`) to
check that Paimon C++ reads index files written by the `paimon-full-text-index` 0.1.0 engine, which
Java Paimon's `paimon-full-text` module (`NativeFullTextGlobalIndexWriter`) also uses. The test also
indexes the same rows with Paimon C++ and checks that the written archives have the same header
metadata and return the same search results. CI also reads the archives written by Paimon C++ with
the Python reader; see [Checking archives written by Paimon C++](#checking-archives-written-by-paimon-c).

They were written by `generate_fixtures.py` with the `paimon-ftindex` 0.1.0 Python wheel, which
wraps the same native engine as the Java binding. Rows are added like the Java writer adds them: a
null value advances the row id but is not indexed.

| File | Index options (`full-text.` prefix stripped) | Rows |
|---|---|---|
| `default.archive` | none (`default` tokenizer) | 5 rows, row 1 is null |
| `ngram.archive` | `tokenizer=ngram`, `ngram.min-gram=2`, `ngram.max-gram=3` | 4 rows, row 2 is null |
| `jieba.archive` | `tokenizer=jieba` | 4 rows, row 2 is null |

Each archive uses storage format version 1 (`PFTIDX01` magic) and records
`tantivy v0.26.1, index_format v7` as its Tantivy version. The engine rejects archives whose
recorded Tantivy version differs from the linked one, so the fixtures must be regenerated whenever
the pinned engine version changes.

## Regenerating

```bash
pip install paimon-ftindex==0.1.0
python3 generate_fixtures.py <output-dir>
```

The script prints the expected row ids and scores of every search. Copy the new archives here and
update the golden values in `src/paimon/global_index/full_text/full_text_global_index_test.cpp`.
Tests must never rewrite these files.

## Checking archives written by Paimon C++

When `PAIMON_FULL_TEXT_ARCHIVE_OUTPUT_DIR` is set to an existing directory, `TestCrossReadFixtures`
copies the archives it writes there. Read them with the Python wheel and compare the results with
the checked-in fixtures. The `gcc-debug-x86_64` CI job does this through
`ci/scripts/build_paimon.sh --verify_full_text_archives`. To run it locally, from the repository
root:

```bash
PAIMON_FULL_TEXT_ARCHIVE_OUTPUT_DIR=<archive-dir> \
    build/debug/paimon-full-text-index-test --gtest_filter='*TestCrossReadFixtures'
python3 test/test_data/full_text_fixtures/generate_fixtures.py --verify <archive-dir>
```
