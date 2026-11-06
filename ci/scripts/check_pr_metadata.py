#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Validate PR metadata from the GitHub Actions event without invoking a shell."""

import argparse
import json
import os
import re
import sys
from typing import Dict, List, Tuple


SECTIONS = ("Purpose", "Tests", "API and Format", "Documentation", "Generative AI tooling")
ALLOWED_TYPES = (
    "build", "chore", "ci", "docs", "feat", "fix", "perf", "refactor", "revert", "style", "test"
)
TITLE_PATTERN = re.compile(
    r"(?P<type>[^\W\d_][\w-]*)(?:\((?P<scope>[^()\r\n]+)\))?!?: \S[^\r\n]*"
)
HEADING_PATTERN = re.compile(r"^ {0,3}(#{1,6})[ \t]+(.+?)[ \t]*$")
FENCE_PATTERN = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")


def check_title(title: str) -> List[str]:
    match = TITLE_PATTERN.fullmatch(title)
    if match is None or (match.group("scope") is not None and not match.group("scope").strip()):
        return [
            "PR title must follow '<type>[optional scope][!]: <description>', "
            "for example 'fix(parquet): handle empty input'."
        ]
    if match.group("type") not in ALLOWED_TYPES:
        return ["PR title type must be one of these lowercase types: " + ", ".join(ALLOWED_TYPES) + "."]
    return []


def strip_comment(line: str, in_comment: bool) -> Tuple[str, bool]:
    visible = ""
    while line:
        marker = "-->" if in_comment else "<!--"
        index = line.find(marker)
        if index < 0:
            return visible + ("" if in_comment else line), in_comment
        if not in_comment:
            visible += line[:index]
        line = line[index + len(marker) :]
        in_comment = not in_comment
    return visible, in_comment


def parse_sections(
    body: str, section_names: Tuple[str, ...] = SECTIONS, include_fenced_code: bool = True
) -> Tuple[Dict[str, List[str]], List[str]]:
    """Read level-three template sections, ignoring headings in comments or fences."""
    sections: Dict[str, List[str]] = {}
    headings: List[str] = []
    current = None
    fence = ""
    in_comment = False
    for line in body.splitlines():
        if not fence:
            line, in_comment = strip_comment(line, in_comment)
        marker = FENCE_PATTERN.match(line)
        if fence:
            if (
                marker
                and marker.group(1)[0] == fence[0]
                and len(marker.group(1)) >= len(fence)
                and not marker.group(2).strip()
            ):
                fence = ""
            elif current is not None and include_fenced_code:
                sections[current].append(line)
            continue
        if marker:
            fence = marker.group(1)
            continue
        heading = HEADING_PATTERN.match(line)
        if heading and len(heading.group(1)) <= 3:
            current = None
            name = re.sub(r"[ \t]+#+$", "", heading.group(2))
            if len(heading.group(1)) == 3 and name in section_names:
                headings.append(name)
                sections.setdefault(name, [])
                current = name
        elif current is not None:
            sections[current].append(line)
    return sections, headings


def check_description(body: str) -> List[str]:
    sections, headings = parse_sections(body)
    errors = []
    if tuple(headings) != SECTIONS:
        errors.append(
            "PR description must contain each template heading exactly once, in this order: "
            + ", ".join("### " + name for name in SECTIONS)
            + "."
        )
    for name in SECTIONS:
        if name not in sections:
            continue
        lines = sections[name]
        if name == "Purpose":
            # An issue reference alone does not explain the purpose of the change.
            lines = [
                line for line in lines if not re.match(
                    r"^\s*(?:Linked issue:|(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s+#\d+\s*$)",
                    line, re.IGNORECASE
                )
            ]
        content = "\n".join(lines).strip()
        if not re.search(r"\w", content):
            errors.append("Fill in '" + name + "'; explain why if it is not applicable.")
        elif re.fullmatch(r"(?:TODO|TBD|N/?A)[.!]?", content, re.IGNORECASE):
            errors.append("Replace the placeholder in '" + name + "' with an explanation.")
        if any(
            re.fullmatch(r"\s*(?:Linked issue:\s*)?close\s+#xxx\s*", line, re.IGNORECASE)
            for line in sections[name]
        ):
            errors.append("Remove or replace the template's 'Linked issue: close #xxx' placeholder.")
    disclosure = "\n".join(sections.get("Generative AI tooling", [])).strip()
    if disclosure and not re.fullmatch(r"No\.?", disclosure, re.IGNORECASE):
        if not re.search(
            r"^(?:[-*][ \t]+)?Generated-by:",
            disclosure, re.MULTILINE | re.IGNORECASE
        ):
            errors.append(
                "In 'Generative AI tooling', write 'No' or include 'Generated-by:'."
            )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("check", choices=("title", "description"))
    args = parser.parse_args()
    try:
        with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as source:
            event = json.load(source)
        pr = event["pull_request"]
        value = pr.get("title" if args.check == "title" else "body") or ""
        if not isinstance(value, str):
            raise ValueError("PR metadata must be text")
    except (OSError, KeyError, TypeError, ValueError) as error:
        print("Cannot read PR metadata: " + str(error), file=sys.stderr)
        return 1
    errors = check_title(value) if args.check == "title" else check_description(value)
    for error in errors:
        print("::error::" + error)
    if not errors:
        print("PR " + args.check + " is valid.")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
