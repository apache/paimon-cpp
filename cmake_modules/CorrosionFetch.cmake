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
#
# Pull Corrosion-rs via FetchContent so we can import crates/lance_ffi as
# CMake targets for the Lance file format.
#
# Pinned to v0.5.2 (stable release). Requires CMake >= 3.22.

include(FetchContent)

# Corrosion does heavy cargo/rustc work at configure+build time; pin tag for
# reproducibility and allow override via env var for offline builds.
set(PAIMON_CORROSION_TAG
    "v0.5.2"
    CACHE STRING "Git tag of corrosion-rs to fetch; change only when upgrading. v0.5.1+
    is required for rustup >= 1.28 whose `rustup toolchain list --verbose`
    output format broke v0.5.0's FindRust.cmake regex.")

set(PAIMON_CORROSION_REPO
    "https://github.com/corrosion-rs/corrosion.git"
    CACHE STRING "Override to a private mirror for offline / firewalled builds.")

# Help Corrosion find rustc/cargo when CMake is invoked without a login shell
# or when rustup is installed to a non-default location. We try, in order:
#   1. Existing Rust_COMPILER cache variable (user override)
#   2. rustc/cargo on PATH
#   3. $CARGO_HOME/bin or $HOME/.cargo/bin for non-login shells
if(NOT DEFINED Rust_COMPILER OR Rust_COMPILER STREQUAL "")
    find_program(_rustc_path rustc PATHS "$ENV{CARGO_HOME}/bin" "$ENV{HOME}/.cargo/bin"
                                         NO_CACHE)
    if(_rustc_path)
        set(Rust_COMPILER
            "${_rustc_path}"
            CACHE FILEPATH "rustc")
    endif()
endif()
if(NOT DEFINED Rust_CARGO OR Rust_CARGO STREQUAL "")
    find_program(_cargo_path cargo PATHS "$ENV{CARGO_HOME}/bin" "$ENV{HOME}/.cargo/bin"
                                         NO_CACHE)
    if(_cargo_path)
        set(Rust_CARGO
            "${_cargo_path}"
            CACHE FILEPATH "cargo")
    endif()
endif()
# Corrosion reads `rustup which rustc` to resolve the real toolchain binary.
# If CMake is invoked from a non-login shell, $PATH may miss ~/.cargo/bin and
# `rustup` can't be found. Prepend rustup's bin dir so child processes see it.
if(DEFINED Rust_COMPILER)
    get_filename_component(_rustup_bin_dir "${Rust_COMPILER}" DIRECTORY)
    if(_rustup_bin_dir AND NOT "$ENV{PATH}" MATCHES "${_rustup_bin_dir}")
        set(ENV{PATH} "${_rustup_bin_dir}:$ENV{PATH}")
    endif()
endif()
message(STATUS "Corrosion: Rust_COMPILER=${Rust_COMPILER}")
message(STATUS "Corrosion: Rust_CARGO=${Rust_CARGO}")

fetchcontent_declare(Corrosion
                     GIT_REPOSITORY "${PAIMON_CORROSION_REPO}"
                     GIT_TAG "${PAIMON_CORROSION_TAG}"
                     GIT_SHALLOW TRUE)
fetchcontent_makeavailable(Corrosion)
