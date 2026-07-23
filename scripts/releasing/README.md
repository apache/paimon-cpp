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

# Apache Paimon C++ release scripts

These scripts create and verify the source artifact voted on by the Apache Paimon
PMC. They do not publish artifacts, create tags, or move files between Apache
distribution repositories.

## Create a release candidate

Create and push a signed RC tag before creating the source artifact:

```bash
git tag -s release-0.2.3-rc1 -m "Apache Paimon C++ 0.2.3 RC1"
git push upstream release-0.2.3-rc1
```

Create the source artifact, SHA-512 checksum, and detached OpenPGP signature:

```bash
scripts/releasing/create_source_release.sh \
  --version 0.2.3 \
  --git-ref release-0.2.3-rc1 \
  --output-dir release/0.2.3-rc1 \
  --signing-key ASF_GPG_KEY_ID
```

The output files are:

```text
apache-paimon-cpp-0.2.3-src.tgz
apache-paimon-cpp-0.2.3-src.tgz.asc
apache-paimon-cpp-0.2.3-src.tgz.sha512
```

Existing files are never overwritten. A changed candidate must use a new RC
directory and a new vote.

## Verify a release candidate

Download the source artifact, its `.asc` and `.sha512` files, and the Paimon
`KEYS` file. Import `KEYS` into a temporary or dedicated GPG keyring, then run:

```bash
RAT_JAR=/path/to/apache-rat-0.16.1.jar \
  scripts/releasing/verify_release_candidate.sh \
  apache-paimon-cpp-0.2.3-src.tgz
```

The verifier checks:

- the SHA-512 checksum and detached OpenPGP signature;
- archive paths and the single `paimon-cpp-0.2.3/` root directory;
- required `LICENSE`, `NOTICE`, build, and documentation files;
- the CMake and documentation versions;
- absence of common compiled artifact types;
- Apache RAT results;
- a release build and the test suite from the extracted source archive.

The `--allow-unsigned` and `--skip-rat` options are only for local development
of the release process. A voter may use `--skip-build` when the repository's
Linux CI build command is not suitable for their platform, but must then build
and test the extracted source distribution separately before casting a binding
vote.
