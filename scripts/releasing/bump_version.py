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

"""Check or update the Apache Paimon C++ project and documentation version."""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, List


VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")
SERIES_PATTERN = re.compile(r"^\d+\.\d+$")

DOCS_URL = "https://paimon.apache.org/docs/cpp/"
MAIN_ENTRY_VERSION = "main"


def series(version: str) -> str:
    """The major.minor documentation series a version belongs to."""
    return ".".join(version.split(".")[:2])


def stable_entry(series_name: str) -> Dict[str, object]:
    """Dropdown entry for the latest release published at the site root."""
    return {
        "name": f"{series_name} (stable)",
        "version": "",
        "url": DOCS_URL,
        "preferred": True,
    }


def series_entry(series_name: str) -> Dict[str, object]:
    """Dropdown entry for an archived release series under <series>/."""
    return {
        "name": series_name,
        "version": series_name,
        "url": f"{DOCS_URL}{series_name}/",
    }


class VersionError(RuntimeError):
    """Version metadata is missing or inconsistent."""


def require_version(value: str) -> None:
    if VERSION_PATTERN.fullmatch(value) is None:
        raise VersionError(f"invalid version {value!r}; expected MAJOR.MINOR.PATCH")


def replace_once(text: str, pattern: str, replacement: str, description: str) -> str:
    result, count = re.subn(pattern, replacement, text, flags=re.MULTILINE)
    if count != 1:
        raise VersionError(f"expected one {description}, found {count}")
    return result


def load_versions(path: Path) -> List[Dict[str, object]]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise VersionError(f"cannot read {path}: {error}") from error
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise VersionError(f"{path} must contain a JSON array of objects")
    return value


def check_version_list(versions: List[Dict[str, object]]) -> None:
    """Validate the dev/stable/archived layout of versions.json."""
    if len(versions) < 2:
        raise VersionError(
            "versions.json must contain a dev entry followed by a stable entry"
        )
    dev = versions[0]
    if (
        dev.get("version") != MAIN_ENTRY_VERSION
        or dev.get("name") != "main (development)"
        or dev.get("url") != f"{DOCS_URL}main/"
    ):
        raise VersionError(
            "the first versions.json entry must be the main-branch docs: "
            f"name 'main (development)', version {MAIN_ENTRY_VERSION!r}, "
            f"url {DOCS_URL}main/"
        )
    stable = versions[1]
    name = stable.get("name")
    match = (
        re.fullmatch(r"(\d+\.\d+) \(stable\)", name)
        if isinstance(name, str)
        else None
    )
    if (
        match is None
        or stable.get("version") != ""
        or stable.get("url") != DOCS_URL
        or stable.get("preferred") is not True
    ):
        raise VersionError(
            "the second versions.json entry must be the stable docs: name "
            "'X.Y (stable)', version '', url the site root, preferred true"
        )
    for item in versions[2:]:
        item_name = item.get("name")
        if (
            not isinstance(item_name, str)
            or SERIES_PATTERN.fullmatch(item_name) is None
            or item.get("version") != item_name
            or item.get("url") != f"{DOCS_URL}{item_name}/"
        ):
            raise VersionError(
                "archived versions.json entries must use name == version == "
                f"X.Y with url {DOCS_URL}X.Y/, found {item!r}"
            )


def checked_files(root: Path, expected: str) -> Dict[Path, str]:
    cmake_path = root / "CMakeLists.txt"
    docs_path = root / "docs/source/conf.py"
    versions_path = root / "docs/source/_static/versions.json"

    cmake = cmake_path.read_text(encoding="utf-8")
    docs = docs_path.read_text(encoding="utf-8")
    versions = load_versions(versions_path)

    cmake_matches = re.findall(
        r"^[ \t]*VERSION[ \t]+(\d+\.\d+\.\d+)[ \t]*$", cmake, re.MULTILINE
    )
    docs_matches = re.findall(
        r'^version = "(\d+\.\d+\.\d+)"$', docs, re.MULTILINE
    )
    if cmake_matches != [expected]:
        raise VersionError(f"CMake version is {cmake_matches}, expected [{expected!r}]")
    if docs_matches != [expected]:
        raise VersionError(
            f"documentation version is {docs_matches}, expected [{expected!r}]"
        )
    check_version_list(versions)
    return {
        cmake_path: cmake,
        docs_path: docs,
        versions_path: json.dumps(versions, indent=4) + "\n",
    }


def updated_files(root: Path, current: str, new: str) -> Dict[Path, str]:
    files = checked_files(root, current)
    cmake_path = root / "CMakeLists.txt"
    docs_path = root / "docs/source/conf.py"
    versions_path = root / "docs/source/_static/versions.json"

    files[cmake_path] = replace_once(
        files[cmake_path],
        rf"^([ \t]*VERSION[ \t]+){re.escape(current)}([ \t]*)$",
        rf"\g<1>{new}\g<2>",
        f"CMake VERSION {current}",
    )
    files[docs_path] = replace_once(
        files[docs_path],
        rf'^version = "{re.escape(current)}"$',
        f'version = "{new}"',
        f"documentation version {current}",
    )

    versions = load_versions(versions_path)
    if series(current) != series(new):
        # A new documentation series ships with this release: the released
        # version becomes the stable docs at docs/cpp/ and the previous
        # stable docs move to their series directory. The main entry is
        # fixed. Patch release cycles keep the version list unchanged.
        previous_series = versions[1]["name"].split(" ")[0]
        versions[1] = stable_entry(series(current))
        versions.insert(2, series_entry(previous_series))
    files[versions_path] = json.dumps(versions, indent=4) + "\n"
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("current_version", nargs="?")
    parser.add_argument("new_version", nargs="?")
    parser.add_argument("--check", metavar="VERSION")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[2]
    )
    args = parser.parse_args()

    try:
        if args.check:
            if args.current_version or args.new_version:
                raise VersionError("--check cannot be combined with version arguments")
            require_version(args.check)
            checked_files(args.root.resolve(), args.check)
            print(f"Release version metadata is consistent: {args.check}")
            return 0

        if not args.current_version or not args.new_version:
            raise VersionError("CURRENT_VERSION and NEW_VERSION are required")
        require_version(args.current_version)
        require_version(args.new_version)
        if args.current_version == args.new_version:
            raise VersionError("current and new versions must differ")

        files = updated_files(
            args.root.resolve(), args.current_version, args.new_version
        )
        for path, content in files.items():
            if args.dry_run:
                print(f"Would update {path}")
            else:
                path.write_text(content, encoding="utf-8")
                print(f"Updated {path}")
        return 0
    except (OSError, VersionError) as error:
        print(f"Version update failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
