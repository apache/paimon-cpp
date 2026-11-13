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

# Contributing to Apache Paimon C++

Thank you for your interest in contributing to Paimon C++! This document explains how to get started.

---

## Reporting Issues

If you find a bug or want to request a feature, please open an [issue](https://github.com/apache/paimon-cpp/issues/new). Include as much detail as possible — steps to reproduce, expected vs. actual behavior, environment info, etc.

Use the appropriate [issue template](.github/ISSUE_TEMPLATE/) and complete all required fields.

---

## Commit Messages and Pull Request Titles

Use the [Conventional Commits](https://www.conventionalcommits.org/) format for commit messages and pull request titles:

```text
<type>(<optional-scope>): <description>
```

Use one of these lowercase types: `build`, `chore`, `ci`, `docs`, `feat`, `fix`, `perf`,
`refactor`, `revert`, `style`, or `test`. The scope is optional. For a breaking change,
add `!` immediately before the colon, for example `feat(api)!: remove a deprecated interface`.

Examples:

```text
feat(parquet): support page-level bitmap filtering
fix: handle non-contiguous row ranges
test(executor): add shutdown coverage
docs: update the build instructions
```

Choose a type and optional scope that accurately describe the change. Keep the description concise and write it in the imperative mood. A pull request title should summarize the complete change and use the same format.

---

## Submitting Pull Requests

1. **Fork** the repository and create a feature branch from `main`.
2. Make your changes following the [coding and development guidelines](#coding-guidelines).
3. Add or update tests for the functionality you changed.
4. Ensure all checks pass.
5. Open a pull request against `main`. Fill in the [PR template](.github/PULL_REQUEST_TEMPLATE.md).

When addressing review feedback or adding follow-up changes to an open pull request, prefer a separate commit instead of amending and force-pushing existing commits. This makes the incremental diff easier for reviewers to inspect. Rewrite existing commits only when a maintainer explicitly requests it.

### PR Checklist

Before submitting, please verify:

- [ ] Code compiles without warnings under `-Wall` (enabled by default in the build system).
- [ ] `pre-commit run --all-files` passes.
- [ ] New / modified public APIs are marked with `PAIMON_EXPORT`.
- [ ] Every new file has the Apache 2.0 license header.
- [ ] Error handling uses `Status` / `Result<T>` — no exceptions.
- [ ] New utility code checks for existing helpers before reinventing (see the [utility reference](docs/code-style.md#reuse-existing-utilities)).
- [ ] Tests are added or updated for the changed functionality.
- [ ] Design and regression coverage follow the [Development Guidelines](docs/development-guidelines.md).
- [ ] Copied or adapted external code follows the [attribution requirements](#external-code-attribution).
- [ ] PR description follows the [template](.github/PULL_REQUEST_TEMPLATE.md).

---

## Development Setup

### Prerequisites

- **C++17** compatible compiler (GCC recommended)
- **CMake** ≥ 3.22
- **Python 3** (for linting scripts and pre-commit)

### Pre-commit Hooks (Recommended)

Install [pre-commit](https://pre-commit.com/) and the Git hooks:

```bash
pip install pre-commit
pre-commit install
```

The hooks automatically run on every commit:

| Hook | What it checks |
|------|----------------|
| `clang-format` | C++ formatting (`.clang-format`) |
| `cpplint` | Google C++ lint rules |
| `cmake-format` | CMake file formatting (`.cmake-format.py`) |
| `codespell` | Spelling errors |
| `trailing-whitespace` | Trailing whitespace |
| `end-of-file-fixer` | Missing newline at end of file |
| `check-added-large-files` | Files > 5 MB |
| `check-yaml` | YAML syntax |
| `sphinx-lint` (alias `docs`) | reStructuredText syntax under `docs/source/` |

Run hooks for changed files during development, or for the entire repository before submitting:

```bash
pre-commit run --files <changed-files>
pre-commit run --all-files
```

### Building and Testing

Run the following commands from the project root. Reuse an existing configured build directory
when it is compatible with the change. Otherwise, configure a debug build with tests:

```bash
cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Debug \
    -DPAIMON_BUILD_TESTS=ON
```

Start with the relevant test target and suite. For example, for `CoreOptionsTest`:

```bash
cmake --build build --target paimon-core-test -j "$(nproc)"
./build/debug/paimon-core-test --gtest_filter='CoreOptionsTest.*'
```

Broaden validation for public API, shared infrastructure, build configuration, or cross-module
changes. To build and run all unit tests:

```bash
cmake --build build --target unittest -j "$(nproc)"
```

The CI build entry point is `ci/scripts/build_paimon.sh`; it may rebuild dependencies and take
substantially longer than a focused target. For complete build options and dependency setup,
see [Building Paimon C++](docs/source/build_system.rst).

Before handing off changes, review the diff and check whitespace:

```bash
git diff --check
```

Report which validation ran and any checks that could not run, including the reason. Use the
[PR template](.github/PULL_REQUEST_TEMPLATE.md) to describe test coverage in the pull request.

---

## Coding Guidelines

Read both guides before writing code. Each guide owns a different set of rules:

| Guide | Scope |
|-------|-------|
| [Code Style Guide](docs/code-style.md) | Code expression: declarations, naming, ownership types, initialization, formatting, comments, and test macros |
| [Development Guidelines](docs/development-guidelines.md) | Design and testing decisions: defaults and overloads, function responsibilities, reuse, dependencies, return semantics, validation boundaries, and regression coverage |

This contributing guide is the entry point for setup, validation, contribution workflow, and
external code attribution. Link to the detailed rules rather than maintaining parallel copies.

---

## Dev Containers

We provide Dev Container configuration file templates for VS Code:

```bash
cd .devcontainer
cp Dockerfile.template Dockerfile
cp devcontainer.json.template devcontainer.json
```

Then select **Dev Containers: Reopen in Container** from VS Code's Command Palette.

If you make improvements that could benefit all developers, please update the template files and submit a pull request.

---

## License

By contributing to Paimon C++, you agree that your contributions will be licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

### External Code Attribution

When copying or adapting external code, including code from another Apache project:

- Record the upstream project, source file, and version or commit in an attribution comment near
  the derived code. Explain local modifications where required by the upstream license.
- Preserve required copyright, license, and attribution notices. Follow the
  [ASF source header policy](https://www.apache.org/legal/src-headers.html) when handling
  third-party files; do not replace their headers with the standard ASF header.
- Update [LICENSE](LICENSE) with the affected files and applicable license information, including
  license texts under `licenses/` when needed. Add required third-party notices to [NOTICE](NOTICE)
  according to the [ASF third-party notice policy](https://www.apache.org/legal/resolved.html#required-third-party-notices).
- Include attribution changes in the same pull request as the code they describe, and identify
  the source in the PR description so reviewers can check the adaptation.
