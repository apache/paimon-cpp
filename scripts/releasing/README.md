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
PMC, stage a release candidate in the ASF distribution repository, and publish
an approved candidate. Run release operations from a clean checkout of
`apache/paimon-cpp`, not from a fork.

The source archive is the official Apache release. Git tags, GitHub Releases,
and binary packages are supplementary.

## Prerequisites

Before starting a release:

- obtain an ASF code-signing key, publish it through the ASF account system,
  and make sure it is present in
  [Paimon KEYS](https://downloads.apache.org/paimon/KEYS);
- install `git`, `gpg`, `svn`, `gh`, `python3`, `curl` or `wget`, Java, CMake,
  Ninja, and the toolchain needed by `ci/scripts/build_paimon.sh` (Java is
  required by Apache RAT);
- authenticate `gh` with access to read GitHub Actions runs in
  `apache/paimon-cpp`;
- make sure the Apache Git remote points directly to
  `apache/paimon-cpp`;
- prepare and merge a release-preparation PR that updates the release notes and
  all version metadata, and passes the normal and release-candidate workflows.

For example, update all version locations and review the diff:

```bash
scripts/releasing/bump_version.py 0.2.0 0.3.0
scripts/releasing/bump_version.py --check 0.3.0
```

### Signing key setup and security

Complete signing-key setup well before creating the first release candidate:

- new signing keys must use RSA with at least 4096 bits;
- publish the public key to the global public keyserver network;
- append the public key to `dist/release/paimon/KEYS`. Never remove historical
  keys because they are required to verify archived releases;
- wait until the updated key is visible from
  `https://downloads.apache.org/paimon/KEYS` before creating an RC; and
- never store the private key or create release signatures on ASF machines.
  Sign only on a secure machine controlled by the release manager.

By default, updating `dist/release/paimon/KEYS` requires PMC membership. A
non-PMC release manager should ask a PMC member to add the key.

See the ASF
[release-signing](https://infra.apache.org/release-signing.html) and
[release-distribution](https://infra.apache.org/release-distribution.html)
policies for the complete requirements.

The release scripts use `vVERSION-rcRC` for release-candidate tags and
`vVERSION` for the final release tag. For example, the first 0.3.0 candidate is
`v0.3.0-rc1`.

## Create a release candidate

Start from the exact clean commit approved for the candidate. Before publishing,
the wrapper fetches the release branch and requires `HEAD` to be contained in
its current history. It then creates and verifies a signed RC tag, creates the
source archive and its checksum/signature, performs the full source-release
verification, pushes the tag, waits for the tag-triggered release-candidate
workflow to succeed, and imports the artifacts into ASF `dist/dev`:

```bash
scripts/releasing/release_rc.sh \
  --version 0.3.0 \
  --rc 1 \
  --signing-key ASF_GPG_KEY_ID \
  --remote upstream
```

The release branch defaults to `main`; use `--release-branch NAME` for a
maintenance release from another Apache branch.

Use `--prepare-only` to create and verify artifacts without pushing the tag or
uploading to ASF infrastructure. This local-only mode does not require `HEAD`
to match the remote release branch. Use `--dry-run` to print identifiers
without making changes. A resumed run reuses an existing local tag or complete
artifact set only after validating it. A prepare-only run does not print a vote
email and must not be used to start a vote.

The candidate directory contains:

```text
apache-paimon-cpp-0.3.0-src.tgz
apache-paimon-cpp-0.3.0-src.tgz.asc
apache-paimon-cpp-0.3.0-src.tgz.sha512
```

The wrapper prints a vote-email template. Send it to `dev@paimon.apache.org`.
Keep the vote open for at least 72 hours. An Apache release vote requires at
least three binding `+1` votes and more binding `+1` than binding `-1` votes.
If the vote has not met these requirements after 72 hours, do not publish the
release; either extend the vote or close it as unsuccessful.

Before casting a binding `+1`, a PMC member must download the signed source
artifact onto hardware they control, verify its signature and ASF policy
compliance, compile it as provided, and test it on their platform. CI results
do not replace this voter responsibility.

After closing the vote, send a result email as a reply to the vote thread. Use
the subject
`[RESULT][VOTE][C++] Release Apache Paimon C++ VERSION RCNUMBER`, state whether
the vote passed, list binding and non-binding votes and voters separately, and
include the archived vote-thread link.

If a candidate changes for any reason, cancel or close its vote, fix the
release-preparation branch, increment the RC number, create a new signed tag
and artifacts, and start a new vote. Never overwrite an existing candidate.
After its vote is closed, a failed or superseded candidate may be removed from
`dist/dev`.

## Verify a release candidate

Voters can download and verify an ASF-staged candidate in one command:

```bash
scripts/releasing/verify_release_candidate.sh --version 0.3.0 --rc 1
```

To verify files that were downloaded separately, use an explicitly downloaded
KEYS file so signature verification runs in an isolated GPG home:

```bash
scripts/releasing/verify_release_candidate.sh \
  --keys-file /path/to/paimon-KEYS \
  apache-paimon-cpp-0.3.0-src.tgz
```

The verifier checks:

- the SHA-512 checksum and detached OpenPGP signature;
- archive path safety, portable filename collisions, file types, permissions,
  and the single `paimon-cpp-0.3.0/` root directory;
- required `LICENSE`, `NOTICE`, build, and documentation files;
- the CMake and documentation versions;
- absence of compiled artifacts by filename and file magic;
- Apache RAT results;
- a release build and the test suite from the extracted source archive; and
- installation plus compilation and execution of an external CMake consumer.

Pass `--git-ref v0.3.0-rc1` when the Git repository is available to regenerate
the archive from the signed tag and compare it byte-for-byte.

`--allow-unsigned`, `--skip-rat`, `--skip-build`, and `--skip-install` exist for
CI or local development of the release process. They are not a substitute for
the corresponding checks when voting. The release-candidate workflow creates
an unsigned archive for deterministic CI validation; official artifacts must
always be signed by the release manager.

## Publish an approved release

After closing the vote and confirming that it passed, publish the exact
approved candidate:

```bash
scripts/releasing/publish_release.sh \
  --version 0.3.0 \
  --rc 1 \
  --signing-key ASF_GPG_KEY_ID \
  --remote upstream \
  --confirm-vote-passed
```

By default, only PMC members can publish to `dist/release`. A non-PMC release
manager must ask a PMC member to perform this step unless Infra has configured
the project, following project consensus, to allow all committers to publish.

The script verifies the signed RC tag, creates a signed final tag pointing to
the same commit, moves the candidate from ASF `dist/dev` to `dist/release`, and
creates a GitHub Release containing byte-identical copies of the ASF source
artifacts. It then prints the remaining ASF reporting, old-release cleanup,
documentation, mirror-propagation, and announcement steps.

Download-page and release-note PRs may be prepared before publication, but do
not merge them while they point users at an unapproved RC. After publication,
verify the artifacts on `downloads.apache.org`, then wait at least 24 hours
before merging public download/documentation changes and sending the release
announcement. This follows the Paimon project convention, which is stricter
than ASF's general one-hour minimum.

## Individual tools

- `bump_version.py`: consistently check or update CMake and documentation
  version metadata.
- `create_source_release.sh`: deterministically create an archive, SHA-512
  checksum, and optional detached signature from an immutable Git ref.
- `validate_source_archive.py`: reject unsafe or non-portable tar members and
  compiled files.
- `verify_release_candidate.sh`: perform voter-facing integrity, license,
  build, test, and install checks.
- `release_rc.sh`: orchestrate release-candidate tagging, verification, and ASF
  staging.
- `publish_release.sh`: publish an approved candidate.

Run the release-tool regression tests with:

```bash
python3 -m unittest discover -s scripts/releasing/tests -v
```
