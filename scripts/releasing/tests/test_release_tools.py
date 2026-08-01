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

import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from typing import Dict, List, Optional, Tuple


RELEASING_DIR = Path(__file__).resolve().parents[1]
ARCHIVE_VALIDATOR = RELEASING_DIR / "validate_source_archive.py"
VERSION_TOOL = RELEASING_DIR / "bump_version.py"
RELEASE_VERIFIER = RELEASING_DIR / "verify_release_candidate.sh"


class ReleaseToolTest(unittest.TestCase):
    def run_tool(
        self, tool: Path, *args: str, expected_returncode: int = 0
    ) -> subprocess.CompletedProcess:
        result = subprocess.run(
            [sys.executable, str(tool), *args],
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(
            result.returncode,
            expected_returncode,
            msg=f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
        )
        return result

    def create_archive(
        self,
        path: Path,
        *,
        extra_members: Optional[List[Tuple[tarfile.TarInfo, bytes]]] = None,
    ) -> None:
        with tarfile.open(path, mode="w:gz") as archive:
            root = tarfile.TarInfo("paimon-cpp-1.2.3/")
            root.type = tarfile.DIRTYPE
            root.mode = 0o755
            archive.addfile(root)

            license_info = tarfile.TarInfo("paimon-cpp-1.2.3/LICENSE")
            license_info.size = len(b"Apache License\n")
            license_info.mode = 0o644
            archive.addfile(license_info, io.BytesIO(b"Apache License\n"))

            for member, content in extra_members or []:
                member.size = len(content) if member.isfile() else 0
                archive.addfile(member, io.BytesIO(content) if member.isfile() else None)

    def create_verifier_archive(self, directory: Path) -> Path:
        artifact = directory / "apache-paimon-cpp-1.2.3-src.tgz"
        files = {
            "LICENSE": b"Apache License\n",
            "NOTICE": b"Apache Paimon\n",
            "CMakeLists.txt": (
                b"project(paimon\n"
                b"        VERSION 1.2.3\n"
                b'        DESCRIPTION "Paimon C++ Project")\n'
            ),
            "docs/source/conf.py": b'version = "1.2.3"\n',
            "docs/source/_static/versions.json": (
                b'[{"name": "1.2.3", "version": "1.2.3", '
                b'"url": "https://paimon.apache.org/docs/cpp/"}]\n'
            ),
            ".github/.rat-excludes": b"",
            "scripts/releasing/create_source_release.sh": b"#!/usr/bin/env bash\n",
            "ci/scripts/build_paimon.sh": b"""#!/usr/bin/env bash
set -euo pipefail
source_root=$(cd "$(dirname "$0")/../.." && pwd)
[[ $# == 5 ]]
[[ $1 == --source_dir ]]
[[ $2 == "${source_root}" ]]
[[ $3 == --build_type ]]
[[ $4 == Release ]]
[[ $5 == --install_smoke ]]
[[ ${PAIMON_BUILD_JOBS} == 7 ]]
""",
        }
        with tarfile.open(artifact, mode="w:gz") as archive:
            root = tarfile.TarInfo("paimon-cpp-1.2.3/")
            root.type = tarfile.DIRTYPE
            root.mode = 0o755
            archive.addfile(root)
            for name, content in files.items():
                member = tarfile.TarInfo(f"paimon-cpp-1.2.3/{name}")
                member.size = len(content)
                member.mode = 0o755 if name.endswith(".sh") else 0o644
                archive.addfile(member, io.BytesIO(content))

        digest = hashlib.sha512(artifact.read_bytes()).hexdigest()
        artifact.with_suffix(artifact.suffix + ".sha512").write_text(
            f"{digest}  {artifact.name}\n", encoding="utf-8"
        )
        return artifact

    def run_verifier(
        self,
        artifact: Path,
        *,
        options: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
        expected_returncode: int = 0,
    ) -> subprocess.CompletedProcess:
        if options is None:
            options = ["--skip-rat", "--skip-build"]
        result = subprocess.run(
            [
                "bash",
                str(RELEASE_VERIFIER),
                "--allow-unsigned",
                *options,
                str(artifact),
            ],
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            check=False,
        )
        self.assertEqual(
            result.returncode,
            expected_returncode,
            msg=f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
        )
        return result

    def create_fake_rat(self, directory: Path) -> Tuple[Path, Dict[str, str]]:
        rat_jar = directory / "apache-rat.jar"
        rat_jar.touch()
        bin_dir = directory / "bin"
        bin_dir.mkdir()
        java = bin_dir / "java"
        java.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "printf '%s Unknown Licenses\\n' \"${FAKE_RAT_UNKNOWN_COUNT:?}\"\n"
            "printf '%s\\n' 'Files with unapproved licenses:'\n",
            encoding="utf-8",
        )
        java.chmod(0o755)
        env = os.environ.copy()
        env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
        return rat_jar, env

    def test_archive_validator_accepts_regular_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            artifact = Path(temp) / "valid.tgz"
            self.create_archive(artifact)
            self.run_tool(
                ARCHIVE_VALIDATOR,
                "--expected-root",
                "paimon-cpp-1.2.3",
                str(artifact),
            )

    def test_archive_validator_rejects_path_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            artifact = Path(temp) / "traversal.tgz"
            member = tarfile.TarInfo("../outside")
            member.mode = 0o644
            self.create_archive(artifact, extra_members=[(member, b"bad")])
            self.run_tool(
                ARCHIVE_VALIDATOR,
                "--expected-root",
                "paimon-cpp-1.2.3",
                str(artifact),
                expected_returncode=1,
            )

    def test_archive_validator_rejects_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            artifact = Path(temp) / "symlink.tgz"
            member = tarfile.TarInfo("paimon-cpp-1.2.3/link")
            member.type = tarfile.SYMTYPE
            member.linkname = "../../outside"
            member.mode = 0o777
            self.create_archive(artifact, extra_members=[(member, b"")])
            self.run_tool(
                ARCHIVE_VALIDATOR,
                "--expected-root",
                "paimon-cpp-1.2.3",
                str(artifact),
                expected_returncode=1,
            )

    def test_archive_validator_rejects_compiled_magic(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            artifact = Path(temp) / "binary.tgz"
            member = tarfile.TarInfo("paimon-cpp-1.2.3/generated")
            member.mode = 0o755
            self.create_archive(
                artifact, extra_members=[(member, b"\x7fELFcompiled")]
            )
            self.run_tool(
                ARCHIVE_VALIDATOR,
                "--expected-root",
                "paimon-cpp-1.2.3",
                str(artifact),
                expected_returncode=1,
            )

    def test_archive_validator_rejects_portable_collision(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            artifact = Path(temp) / "collision.tgz"
            first = tarfile.TarInfo("paimon-cpp-1.2.3/README")
            first.mode = 0o644
            second = tarfile.TarInfo("paimon-cpp-1.2.3/readme")
            second.mode = 0o644
            self.create_archive(
                artifact,
                extra_members=[(first, b"one"), (second, b"two")],
            )
            self.run_tool(
                ARCHIVE_VALIDATOR,
                "--expected-root",
                "paimon-cpp-1.2.3",
                str(artifact),
                expected_returncode=1,
            )

    def test_verifier_accepts_valid_checksum_and_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            artifact = self.create_verifier_archive(Path(temp))
            result = self.run_verifier(artifact)
            self.assertIn("Release candidate verification completed", result.stdout)

    def test_verifier_rejects_checksum_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            artifact = self.create_verifier_archive(Path(temp))
            checksum = artifact.with_suffix(artifact.suffix + ".sha512")
            checksum.write_text(f"{'0' * 128}  {artifact.name}\n", encoding="utf-8")
            result = self.run_verifier(artifact, expected_returncode=1)
            self.assertIn("SHA-512 checksum does not match", result.stderr)

    def test_verifier_rejects_multiple_checksum_lines(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            artifact = self.create_verifier_archive(Path(temp))
            checksum = artifact.with_suffix(artifact.suffix + ".sha512")
            checksum.write_text(
                checksum.read_text(encoding="utf-8")
                + f"{'0' * 128}  attacker-controlled-file\n",
                encoding="utf-8",
            )
            result = self.run_verifier(artifact, expected_returncode=1)
            self.assertIn(
                "checksum file must contain exactly one non-empty line",
                result.stderr,
            )

    def test_verifier_invokes_build_script_with_named_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            artifact = self.create_verifier_archive(Path(temp))
            result = self.run_verifier(
                artifact,
                options=["--skip-rat", "--jobs", "7"],
            )
            self.assertIn("Release build and tests: valid", result.stdout)
            self.assertIn("Install and consumer smoke test: valid", result.stdout)

    def test_verifier_accepts_zero_unknown_licenses(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            artifact = self.create_verifier_archive(directory)
            rat_jar, env = self.create_fake_rat(directory)
            env["FAKE_RAT_UNKNOWN_COUNT"] = "0"
            result = self.run_verifier(
                artifact,
                options=["--rat-jar", str(rat_jar), "--skip-build"],
                env=env,
            )
            self.assertIn("Apache RAT: valid", result.stdout)

    def test_verifier_rejects_unknown_licenses(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            artifact = self.create_verifier_archive(directory)
            rat_jar, env = self.create_fake_rat(directory)
            env["FAKE_RAT_UNKNOWN_COUNT"] = "1"
            result = self.run_verifier(
                artifact,
                options=["--rat-jar", str(rat_jar), "--skip-build"],
                env=env,
                expected_returncode=1,
            )
            self.assertIn(
                "Apache RAT found 1 files with unknown licenses",
                result.stderr,
            )

    def create_version_tree(self, root: Path) -> None:
        (root / "docs/source/_static").mkdir(parents=True)
        (root / "CMakeLists.txt").write_text(
            "project(paimon\n        VERSION 1.2.3\n"
            '        DESCRIPTION "Paimon C++ Project")\n',
            encoding="utf-8",
        )
        (root / "docs/source/conf.py").write_text(
            'version = "1.2.3"\n', encoding="utf-8"
        )
        (root / "docs/source/_static/versions.json").write_text(
            json.dumps(
                [
                    {
                        "name": "1.2.3",
                        "version": "1.2.3",
                        "url": "https://paimon.apache.org/docs/cpp/",
                    }
                ],
                indent=4,
            )
            + "\n",
            encoding="utf-8",
        )

    def test_version_tool_checks_and_updates_all_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.create_version_tree(root)
            self.run_tool(VERSION_TOOL, "--root", str(root), "--check", "1.2.3")
            self.run_tool(VERSION_TOOL, "--root", str(root), "1.2.3", "1.2.4")
            self.run_tool(VERSION_TOOL, "--root", str(root), "--check", "1.2.4")
            self.assertIn("VERSION 1.2.4", (root / "CMakeLists.txt").read_text())
            self.assertIn(
                'version = "1.2.4"', (root / "docs/source/conf.py").read_text()
            )

    def test_version_tool_rejects_inconsistent_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.create_version_tree(root)
            (root / "docs/source/conf.py").write_text(
                'version = "9.9.9"\n', encoding="utf-8"
            )
            self.run_tool(
                VERSION_TOOL,
                "--root",
                str(root),
                "--check",
                "1.2.3",
                expected_returncode=1,
            )


if __name__ == "__main__":
    unittest.main()
