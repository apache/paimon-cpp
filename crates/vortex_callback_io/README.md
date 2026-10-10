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

# vortex_callback_io

Callback-based I/O for the Vortex C API, so Vortex reads and writes through paimon's
`InputStream` / `OutputStream` instead of materializing whole files in memory or staging them in
local temporary files.

## This is not a standalone crate

There is deliberately no `Cargo.toml` here. `callback_io.rs` is compiled **as a module of the
upstream `vortex-ffi` crate**, because the handles it must construct (`vx_data_source` and friends)
are created through `box_wrapper`-generated constructors that are `pub(crate)`; an external crate
cannot build them. Being inside `vortex-ffi` also means all symbols land in the single
`libvortex_ffi.a` that paimon already links.

The write side still defines its own handle (`vx_callback_sink`) instead of reusing `vx_array_sink`,
whose fields are private to `crate::sink` and therefore unreachable even from a sibling module.

The build wiring lives in `build_vortex` in
[`cmake_modules/ThirdpartyToolchain.cmake`](../../cmake_modules/ThirdpartyToolchain.cmake):

1. `callback_io.rs` is copied into `<vortex source>/vortex-ffi/src/` before cargo runs.
2. [`cmake_modules/vortex.diff`](../../cmake_modules/vortex.diff) adds the single line
   `mod callback_io;` to `vortex-ffi/src/lib.rs`. That one line is the entire patch, which keeps
   Vortex version bumps from conflicting.

Keeping the code here rather than inside the diff means it is reviewed, formatted, and
license-checked like the rest of the repository.

## Working on it

The module is built and tested through cargo in the Vortex source tree. With an existing configured
build directory (`build-clang` below), the source directory and cargo cache are already set up:

```bash
# Rebuild the static library through the normal CMake path.
cmake --build build-clang --target vortex_ep

# Run this module's Rust tests directly.
cd "$(sed -n 's/^source_dir=//p' \
  build-clang/vortex_ep-prefix/src/vortex_ep-stamp/vortex_ep-source_dirinfo.txt)"
FLATC=<path to flatc> CARGO_TARGET_DIR=<repo>/build-clang/vortex_ep-cargo \
  cargo test --locked --release -p vortex-ffi callback_io
```

Two constraints apply when editing this file:

- **No new cargo dependencies.** The build runs `cargo rustc --locked`, so touching `Cargo.lock`
  breaks it. Only what `vortex-ffi` already depends on is available.
- **Upstream lints apply.** `vortex-ffi` sets `#![deny(missing_docs)]` and the Vortex workspace
  denies several rustc lints (`unsafe_op_in_unsafe_fn`, `unused_qualifications`,
  `let_underscore_drop`, ...), so every public item needs documentation and every `unsafe`
  operation needs its own `unsafe` block.

## Threading contract

`vx_input_callbacks::read_at_fn` must be safe to call concurrently for the same context: Vortex
issues parallel positional reads, each on a blocking-pool thread. paimon's positional
`InputStream::Read(buffer, size, offset)` satisfies this (the local implementation uses `pread`),
which is the same assumption the mosaic format's callbacks already rely on.

`vx_output_callbacks::write_fn` has the opposite contract: writes for one sink are sequential and
never concurrent, so no locking is required. They do run on a Vortex runtime thread rather than the
caller's, which is why the paimon-side context guards its error slot and byte counter with a mutex.
The host keeps ownership of its stream: `shutdown` only flushes, and closing is left to the caller.
