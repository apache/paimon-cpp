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

# Apache Paimon C++ Development Guidelines

This document covers design and testing decisions that depend on the problem being solved.
Use the [Code Style Guide](code-style.md) for coding conventions and the
[Contributing Guide](../CONTRIBUTING.md) for setup, validation commands, and pull request workflow.

## Default Arguments and Overloads

- Give a parameter a default value only when omission has a clear meaning for production callers.
  A sentinel such as `-1` or `nullptr` needs a documented meaning and handling path.
- Do not introduce a default argument or constructor overload solely to simplify a test or avoid
  updating callers when adding a required dependency. Supply the dependency explicitly and use
  existing test helpers to construct it.
- Add an overload when it represents a useful operation for callers. Keep initialization and
  validation consistent across overloads by delegating to a common implementation where useful.

For example, if a reader requires a context in production, tests should provide a valid context
rather than motivate a constructor that silently accepts a missing one.

## Function Responsibilities and Reuse

- Split a function when it mixes separate responsibilities or its control flow is hard to follow.
  Extract a coherent operation with clear inputs, outputs, and error handling. There is no fixed
  line count that requires a split.
- Check the [existing utility reference](code-style.md#reuse-existing-utilities) before adding a
  helper. Reuse logic when its behavior and responsibility match the caller's needs.
- A small amount of local repetition or a short wrapper does not by itself justify a new
  abstraction. Consider whether the helper gives a meaningful name to an operation and whether
  callers share the same contract, rather than merely similar syntax.
- Keep a helper near its callers until there is a reason to share it more widely. Avoid exposing
  an implementation detail through a new public API just to make testing easier.

For example, extracting schema validation from a function that also opens files can clarify both
operations. Wrapping a single expression used once may only add another place for readers to look.

## Module Organization and Dependencies

- Keep generic `common` and `core` code independent of format plugin implementation headers.
  When both layers need a contract, put the shared interface in an appropriate common layer and
  let the plugin implement it.
- Keep a feature's readers, writers, and related utilities together in the module that owns the
  behavior. Put a helper in a shared utility directory only if it has a suitable shared purpose.
- Define a shared type alias once at the narrowest layer that all users can depend on. Avoid
  repeating aliases across headers or moving plugin-specific types into a generic layer.

For example, core code should consume a reader interface instead of including a particular
format's reader implementation to access a shared type.

## Return Semantics and Validation Boundaries

Choose and document return values so callers can distinguish these outcomes:

| Outcome | Contract |
|---------|----------|
| Operation succeeded with no data | A valid empty result, or a documented end-of-input representation |
| Operation was not performed | An explicit state or documented return value that callers handle |
| Operation is unsupported | A documented unsupported outcome or an appropriate error `Status` |
| Operation failed | An error `Status` or `Result<T>` that preserves the failure reason |

The representation depends on the interface. A documented `nullptr` sentinel can be appropriate;
do not replace it with an empty object without checking how callers distinguish the states.

- Validate required dependencies and input at the boundary that owns them, such as a builder,
  `Create()`, or an entry point consuming external data. Reject invalid state before starting
  work that relies on it.
- Internal code can rely on established invariants. Add another runtime check only when that
  layer can encounter a new invalid state, rather than repeating the same check at every call.
- Keep defensive code proportionate to reachable failure paths and interface contracts. Avoid
  speculative checks and fallback branches for states excluded by the input domain or established
  invariants. Add overflow checks where arithmetic can exceed its range for inputs that can
  reach the operation, including external input.
- Files, schemas, and imported data still need runtime validation. A debug assertion or
  [checked class-pointer cast](code-style.md#checked-class-pointer-casts) does not validate
  recoverable external input.
- Propagate failures without turning them into success, empty data, or an unsupported outcome.

For example, validate a required executor when constructing an asynchronous reader. Its internal
methods can then use that invariant, while each external byte range still needs the validation
appropriate to that read.

## Effective Regression Tests

- Exercise the affected production entry point and assert behavior that would expose the target
  defect. When practical, confirm that the test fails with the fix removed. A test that reproduces
  an algorithm locally without calling the changed production code does not cover that code.
- Use mocks at the relevant dependency boundary to make failures deterministic while keeping the
  behavior under test in production code. Test failures and boundary cases affected by the change.
  Configure only the mock outcomes needed for the scenario, and assert production behavior
  rather than merely rechecking configured mock responses.
- Keep boundary tests focused on meaningful input ranges and concrete defects. Add overflow and
  extreme-value cases when they exercise a reachable failure, an API or format requirement, or a
  known regression. Do not expand test matrices solely to cover speculative states introduced by
  unnecessary defensive checks.
- Wait for asynchronous work and callbacks to finish before checking their results or destroying
  their dependencies. Use synchronization primitives or existing test helpers instead of sleeps
  that assume a particular execution speed.
- Make sanitizer findings fail the check. A regression test that exits successfully after the
  sanitizer reports the target defect cannot guard against that defect. Configure the sanitizer
  or runner to return failure instead of merely printing diagnostics.
- In unit and integration tests, check observable results such as returned rows, statuses, and
  committed state. Assert mock interactions, call counts, or call order only when needed to verify
  a contract or a specific resource or performance requirement, such as avoiding redundant I/O.
  Avoid numerous variations of these assertions that lock tests to incidental implementation
  details and fail after a refactor that preserves the contract.

For example, a fix to read-ahead behavior needs a test that reads through the affected reader or
cache entry point. A separate byte-copy exercise cannot establish that the reader uses the fix.

See the [testing conventions](code-style.md#testing) for assertion macros and test names, and
[Building and Testing](../CONTRIBUTING.md#building-and-testing) for validation commands.
