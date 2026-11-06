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

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from check_pr_metadata import check_description, check_title


SCRIPT = Path(__file__).with_name("check_pr_metadata.py")
TEMPLATE = Path(__file__).resolve().parents[2] / ".github" / "PULL_REQUEST_TEMPLATE.md"
VALID_BODY = """### Purpose

Handle empty input before decoding a batch.

### Tests

Added EmptyInputTest to cover empty and missing batches.

### API and Format

No API or storage format changes.

### Documentation

Not applicable: this fixes existing behavior.

### Generative AI tooling

No
"""


class PrMetadataTest(unittest.TestCase):
    def test_conventional_titles(self):
        for title in (
            "fix: handle empty input", "feat(parquet): support bitmap filtering",
            "feat(api)!: remove deprecated interface", "fix!: change input handling",
            "fix(API)!: change input handling",
            "docs: 更新文档",
        ):
            with self.subTest(title=title):
                self.assertEqual(check_title(title), [])

    def test_all_allowed_types_with_optional_scope_and_breaking_marker(self):
        for kind in (
            "build", "chore", "ci", "docs", "feat", "fix", "perf", "refactor",
            "revert", "style", "test",
        ):
            for suffix in ("", "(reader)", "!", "(reader)!"):
                title = kind + suffix + ": describe the change"
                with self.subTest(title=title):
                    self.assertEqual(check_title(title), [])

    def test_unknown_types(self):
        for kind in ("index", "custom-type", "feature", "hotfix", "deps", "release"):
            with self.subTest(kind=kind):
                errors = check_title(kind + ": describe the change")
                self.assertTrue(errors)
                self.assertIn("lowercase types", errors[0])
                self.assertIn("feat", errors[0])

    def test_types_must_be_lowercase(self):
        for kind in ("Fix", "FIX", "fIx", "Feat", "CI", "Docs", "Revert"):
            with self.subTest(kind=kind):
                self.assertTrue(check_title(kind + "(reader)!: describe the change"))

    def test_invalid_titles(self):
        for title in (
            "", "Fix empty input", "fix: ", "fix:   ", "fix:missing space",
            "feat(): empty scope", "feat( ): blank scope", "fix(api: unclosed scope",
            "feat(api)(reader): nested scopes", "fix: first line\nsecond line",
        ):
            with self.subTest(title=title):
                self.assertTrue(check_title(title))

    def test_complete_description_and_optional_issue(self):
        self.assertEqual(check_description(VALID_BODY), [])
        self.assertEqual(check_description(VALID_BODY.replace(
            "### Purpose", "### Purpose\n\nLinked issue: close #123"
        )), [])
        self.assertEqual(check_description(VALID_BODY.replace(
            "Added EmptyInputTest to cover empty and missing batches.",
            "Not applicable: documentation-only change."
        )), [])

    def test_issue_placeholder_with_otherwise_complete_description(self):
        body = VALID_BODY.replace("### Purpose", "### Purpose\n\nLinked issue: close #xxx")
        self.assertTrue(any("#xxx" in error for error in check_description(body)))

    def test_unfilled_template_and_empty_body(self):
        self.assertTrue(check_description(""))
        errors = check_description(TEMPLATE.read_text(encoding="utf-8"))
        for section in ("Purpose", "Tests", "API and Format", "Documentation",
                        "Generative AI tooling"):
            self.assertTrue(any("'" + section + "'" in error for error in errors))
        self.assertTrue(any("#xxx" in error for error in errors))

    def test_missing_duplicate_out_of_order_or_wrong_level_headings(self):
        for body in (
            VALID_BODY.replace("### Tests", "### Test cases"),
            VALID_BODY + "\n### Tests\nMore coverage.\n",
            VALID_BODY.replace("### Tests", "### Documentation", 1),
            VALID_BODY.replace("### Tests", "### TEMP")
            .replace("### Documentation", "### Tests").replace("### TEMP", "### Documentation"),
            VALID_BODY.replace("### Tests", "## Tests"),
        ):
            with self.subTest(body=body):
                self.assertTrue(check_description(body))

    def test_headings_in_comments_and_fences_do_not_fill_missing_sections(self):
        for opening, closing in (("<!--", "-->"), ("```markdown", "```"), ("~~~", "~~~")):
            body = VALID_BODY.replace("### Tests", "### Test cases")
            body += "\n" + opening + "\n### Tests\nFake coverage.\n" + closing
            with self.subTest(opening=opening):
                self.assertTrue(check_description(body))

    def test_comments_and_code_examples_in_filled_sections(self):
        body = VALID_BODY.replace(
            "Handle empty input before decoding a batch.",
            "<!-- description hint --> Handle empty input.\n"
            "````markdown\n### Tests\n<!-- an example, not an HTML comment\n```\n````"
        )
        body = body.replace("### Tests\n\nAdded", "### Tests ###\n\nAdded")
        self.assertEqual(check_description(body), [])

    def test_empty_sections_comments_and_placeholders(self):
        for replacement in ("", "<!-- List UT and IT cases -->", "<!-- hint\nmore hint -->",
                            "TODO", "TBD", "N/A", "---"):
            body = VALID_BODY.replace(
                "Added EmptyInputTest to cover empty and missing batches.", replacement
            )
            with self.subTest(replacement=replacement):
                self.assertTrue(any("'Tests'" in error for error in check_description(body)))

    def test_issue_link_alone_is_not_a_purpose(self):
        for reference in ("Linked issue: close #123", "Closes #123", "Fixes #123"):
            body = VALID_BODY.replace("Handle empty input before decoding a batch.", reference)
            with self.subTest(reference=reference):
                self.assertTrue(any("'Purpose'" in error for error in check_description(body)))

    def test_ai_tool_disclosure(self):
        for disclosure in ("No.", "Generated-by: Codex GPT-6", "- Generated-by: Some Tool 1.0"):
            with self.subTest(disclosure=disclosure):
                self.assertEqual(check_description(VALID_BODY.replace("\nNo\n", "\n" + disclosure)), [])
        for disclosure in ("Yes", "Generated-by", "Generated by: Codex"):
            with self.subTest(disclosure=disclosure):
                self.assertTrue(check_description(VALID_BODY.replace("\nNo\n", "\n" + disclosure)))

    def test_generated_by_content_is_not_validated(self):
        for disclosure in (
            "Generated-by:", "Generated-by:   ", "Generated-by: Codex",
            "Generated-by:any content", "- Generated-by:",
            "Generated-by:\nAdditional details without a tool version.",
        ):
            with self.subTest(disclosure=disclosure):
                body = VALID_BODY.replace("\nNo\n", "\n" + disclosure)
                self.assertEqual(check_description(body), [])

    def run_check(self, directory, check, event):
        event_file = Path(directory) / "event.json"
        event_file.write_text(json.dumps(event), encoding="utf-8")
        env = dict(os.environ, GITHUB_EVENT_PATH=str(event_file))
        return subprocess.run(
            [sys.executable, str(SCRIPT), check], env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
            check=False,
        )

    def test_workflow_entrypoint_modes_and_null_body(self):
        with tempfile.TemporaryDirectory() as directory:
            event = {"pull_request": {"title": "invalid", "body": VALID_BODY}}
            result = self.run_check(directory, "description", event)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            result = self.run_check(directory, "title", event)
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertIn("::error::", result.stdout)
            result = self.run_check(directory, "description", {"pull_request": {"body": None}})
            self.assertEqual(result.returncode, 1)
            result = self.run_check(directory, "title", {})
            self.assertEqual(result.returncode, 1)
            self.assertIn("Cannot read PR metadata", result.stderr)

    def test_metadata_is_data_and_not_shell_code(self):
        with tempfile.TemporaryDirectory() as directory:
            sentinel = Path(directory) / "must-not-exist"
            title = "fix: $(touch " + str(sentinel) + ")"
            event = {"pull_request": {"title": title, "body": VALID_BODY}}
            result = self.run_check(directory, "title", event)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertFalse(sentinel.exists())


if __name__ == "__main__":
    unittest.main()
