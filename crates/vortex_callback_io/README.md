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

# Vortex callback I/O

[callback_io.rs](callback_io.rs) lets Vortex read from Paimon's `InputStream` and write to its
`OutputStream` through C callbacks. The `vortex_ep` CMake target copies the module into the
upstream `vortex-ffi` crate so it can construct Vortex handles using crate-private APIs.

## Callback contract

- Vortex may issue concurrent positional reads on the same input context.
- Writes to a single sink are sequential and can run on a Vortex runtime thread. Closing the sink
  finishes the Vortex file and flushes the output. The caller owns and closes the Paimon stream.
- The callback context must remain valid until Vortex calls `release_fn`. When supplied, this
  callback runs once, including when opening the source or sink fails.

The [FFI declarations](../../src/paimon/format/vortex/vortex_ffi.h) and
[Paimon callbacks](../../src/paimon/format/vortex/vortex_io_callbacks.cpp) define the C++ side of
this contract.

## Running the module tests

From the repository root, with Vortex enabled in the `build` directory, build the patched crate
and run its callback I/O tests with the same Rust toolchain used for the build.

```bash
cmake --build build --target vortex_ep
cd build/vortex_ep-prefix/src/vortex_ep
CARGO_TARGET_DIR=../vortex_ep-build cargo test --locked --release -p vortex-ffi callback_io::tests
```
