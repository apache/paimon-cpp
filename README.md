<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Apache Paimon C++

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

Paimon C++ is the C++ implementation of [Apache Paimon](https://paimon.apache.org).
It provides native, high-performance, and extensible access to the Paimon lake format for C++ engines and services without JVM dependencies.

Background and documentation are available at [paimon.apache.org](https://paimon.apache.org).

## Features

Paimon C++ currently provides:

- **Write**: append table and primary key table write support with compaction.
- **Commit**: append table commit support for simple append-only tables.
- **Scan**: batch and stream scan for append tables and primary key tables without changelog.
- **Read**: append table read, primary key table read with deletion vector, and primary key table merge-on-read.
- **Arrow integration**: batch read and write interfaces based on the [Arrow Columnar In-Memory Format](https://arrow.apache.org).
- **File systems**: file system abstraction with built-in local and Jindo file system support.
- **File formats**: file format abstraction with built-in ORC, Parquet, and Avro support.
- **Runtime utilities**: memory pool and thread pool abstractions with default implementations.
- **AI-Oriented Features**: supports RowTracking and DataEvolution mode and provides Global Index
  capabilities including B-tree index, DiskANN-based vector search with Lumina, and Lucene-based
  full-text search.
- **Compatibility**: compatibility with Apache Paimon Java format and communication protocols,
  including commit messages, data splits, and manifests.

> **Bitmap global index compatibility:** Java Paimon now uses a dedicated bitmap global index
> format instead of the previously shared wrapped bitmap file index format.
> Paimon C++ therefore currently treats the `bitmap` global index type as unsupported. The legacy
> implementation remains in the codebase pending migration to the Java-compatible format.

Note: Only Linux x86_64 builds are currently supported and verified.

## Building

```bash
git clone https://github.com/apache/paimon-cpp.git
cd paimon-cpp
```

Build with CMake:

```bash
cmake -B build
cmake --build build
```
### Dev Containers

We provide Dev Container configuration file templates.

To use a Dev Container as your development environment, follow the steps below, then select `Dev Containers: Reopen in Container` from VS Code's Command Palette.

```
cd .devcontainer
cp Dockerfile.template Dockerfile
cp devcontainer.json.template devcontainer.json
```

## Collaboration

Paimon C++ is an active open-source project and we welcome people who want to contribute or share good ideas!
Before contributing, please read the [Contributing Guide](CONTRIBUTING.md) and the [Code Style Guide](docs/code-style.md). You are encouraged to check out our [documentation](https://paimon.apache.org/docs/cpp/).

## License

This project is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
