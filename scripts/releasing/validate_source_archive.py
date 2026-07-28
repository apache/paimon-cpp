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

"""Validate the portable and security-sensitive layout of a source archive."""

import argparse
import stat
import sys
import tarfile
import unicodedata
from pathlib import PurePosixPath
from typing import Optional, Set


COMPILED_MAGICS = {
    b"\x00asm": "WebAssembly module",
    b"\x7fELF": "ELF binary",
    b"BC\xc0\xde": "LLVM bitcode",
    b"\xca\xfe\xba\xbe": "Java class or Mach-O universal binary",
    b"\xce\xfa\xed\xfe": "Mach-O binary",
    b"\xcf\xfa\xed\xfe": "Mach-O binary",
    b"\xfe\xed\xfa\xce": "Mach-O binary",
    b"\xfe\xed\xfa\xcf": "Mach-O binary",
}


class ValidationError(RuntimeError):
    """The archive does not satisfy release safety requirements."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def normalized_name(member: tarfile.TarInfo) -> str:
    value = member.name
    require(bool(value), "archive contains an empty path")
    require("\\" not in value, f"archive path contains a backslash: {value!r}")
    candidate = value[:-1] if member.isdir() and value.endswith("/") else value
    path = PurePosixPath(candidate)
    require(not path.is_absolute(), f"archive contains an absolute path: {value!r}")
    require(".." not in path.parts, f"archive contains path traversal: {value!r}")
    require(path.as_posix() == candidate, f"archive path is not canonical: {value!r}")
    return path.as_posix()


def portable_key(name: str) -> str:
    return unicodedata.normalize("NFC", name).casefold()


def compiled_description(
    archive: tarfile.TarFile, member: tarfile.TarInfo
) -> Optional[str]:
    extracted = archive.extractfile(member)
    if extracted is None:
        raise ValidationError(f"cannot read archive member: {member.name}")
    header = extracted.read(4096)
    for magic, description in COMPILED_MAGICS.items():
        if header.startswith(magic):
            return description
    if header.startswith(b"!<arch>\n"):
        return "Unix archive"
    if header.startswith(b"MZ") and len(header) >= 64:
        pe_offset = int.from_bytes(header[60:64], "little")
        if pe_offset + 4 <= len(header) and header[pe_offset : pe_offset + 4] == b"PE\0\0":
            return "Windows PE binary"
    return None


def validate(artifact: str, expected_root: str) -> None:
    expected_root = expected_root.rstrip("/")
    require(bool(expected_root), "expected root must not be empty")
    require("/" not in expected_root, "expected root must be one path component")

    try:
        archive = tarfile.open(artifact, mode="r:gz")
    except (OSError, tarfile.TarError) as error:
        raise ValidationError(f"cannot open source archive: {error}") from error

    with archive:
        members = archive.getmembers()
        require(bool(members), "source archive is empty")
        names: Set[str] = set()
        portable_names: Set[str] = set()
        found_root = False

        for member in members:
            require(
                member.isfile() or member.isdir(),
                f"archive contains a link or special member: {member.name!r}",
            )
            name = normalized_name(member)
            parts = PurePosixPath(name).parts
            require(
                bool(parts) and parts[0] == expected_root,
                f"archive entry is outside {expected_root}/: {member.name!r}",
            )
            if name == expected_root and member.isdir():
                found_root = True

            require(name not in names, f"archive contains duplicate path: {name!r}")
            names.add(name)
            key = portable_key(name)
            require(
                key not in portable_names,
                f"archive contains a portable path collision: {name!r}",
            )
            portable_names.add(key)

            require(
                member.mode & (stat.S_IWGRP | stat.S_IWOTH) == 0,
                f"archive contains a group- or world-writable member: {name!r}",
            )
            require(
                member.mode & (stat.S_ISUID | stat.S_ISGID | stat.S_ISVTX) == 0,
                f"archive contains a member with special permission bits: {name!r}",
            )

            if member.isfile():
                description = compiled_description(archive, member)
                require(
                    description is None,
                    f"source archive contains a compiled file ({description}): {name}",
                )

        require(found_root, f"archive root directory is missing: {expected_root}/")
        print(f"Validated {len(members)} archive members under {expected_root}/")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", help="source .tar.gz or .tgz archive")
    parser.add_argument("--expected-root", required=True)
    args = parser.parse_args()
    try:
        validate(args.artifact, args.expected_root)
    except ValidationError as error:
        print(f"Archive validation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
