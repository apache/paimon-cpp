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

import io
import json
import os
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock
from urllib.parse import parse_qs, urlsplit

from check_issue_template import (
    NOTICE_MARKER, check_issue_description, github_request, main, update_notice,
)


SEARCH = """### Search before asking

- [x] I searched in the [issues](https://github.com/apache/paimon-cpp/issues) and found nothing similar.
"""
BUG_BODY = SEARCH + """
### Paimon-cpp version

0.4.0

### Minimal reproduce step

Read an empty table with the example reader.

### What doesn't meet your expectations?

The reader should return an empty result but reports an error.

### Anything else?

_No response_

### Are you willing to submit a PR?

- [ ] I'm willing to submit a PR!
"""
FEATURE_BODY = SEARCH + """
### Motivation

Support reading the new storage format.

### Solution

_No response_
"""
REPOSITORY = "apache/paimon-cpp"
ISSUE_PATH = "/repos/" + REPOSITORY + "/issues/7"


def bot_comment(body, comment_id=1):
    return {
        "id": comment_id, "body": body,
        "user": {"login": "github-actions[bot]", "type": "Bot"},
    }


class FakeGitHub:
    def __init__(self, body, comments=None):
        self.issue = {"state": "open", "body": body}
        self.comments = list(comments or [])
        self.calls = []

    def __call__(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        if method == "GET" and path == ISSUE_PATH:
            return self.issue
        if method == "GET" and path.startswith(ISSUE_PATH + "/comments?"):
            page = int(parse_qs(urlsplit(path).query)["page"][0])
            return self.comments[(page - 1) * 100 : page * 100]
        if method == "POST" and path == ISSUE_PATH + "/comments":
            comment = bot_comment(payload["body"], len(self.comments) + 1)
            self.comments.append(comment)
            return comment
        if method == "PATCH" and path.startswith("/repos/" + REPOSITORY + "/issues/comments/"):
            comment_id = int(path.rsplit("/", 1)[1])
            comment = next(comment for comment in self.comments if comment["id"] == comment_id)
            comment["body"] = payload["body"]
            return comment
        raise AssertionError("Unexpected API request: " + method + " " + path)

    def writes(self):
        return [call for call in self.calls if call[0] != "GET"]


class IssueTemplateTest(unittest.TestCase):
    def test_bug_and_feature_forms_allow_empty_optional_fields(self):
        self.assertEqual(check_issue_description(BUG_BODY), [])
        self.assertEqual(check_issue_description(FEATURE_BODY), [])
        self.assertEqual(check_issue_description(FEATURE_BODY.split("### Solution")[0]), [])

    def test_blank_and_unstructured_issues(self):
        for body in ("", "Please fix this bug.", SEARCH):
            with self.subTest(body=body):
                self.assertTrue(check_issue_description(body))

    def test_required_bug_information_and_motivation(self):
        for value, field in (
            ("0.4.0", "Paimon-cpp version"),
            ("Read an empty table with the example reader.", "Minimal reproduce step"),
            ("The reader should return an empty result but reports an error.",
             "What doesn't meet your expectations?"),
        ):
            for placeholder in ("", "_No response_", "<!-- hint -->", "TODO", "N/A"):
                with self.subTest(field=field, placeholder=placeholder):
                    errors = check_issue_description(BUG_BODY.replace(value, placeholder))
                    self.assertTrue(any(field in error for error in errors))
        self.assertTrue(check_issue_description(FEATURE_BODY.replace(
            "Support reading the new storage format.", "_No response_"
        )))

    def test_missing_and_duplicate_sections(self):
        for body in (
            BUG_BODY.replace("### Paimon-cpp version", "### Version"),
            BUG_BODY + "\n### Paimon-cpp version\n0.3.0\n",
            FEATURE_BODY + SEARCH,
            FEATURE_BODY.replace("### Motivation", "## Motivation"),
        ):
            with self.subTest(body=body):
                self.assertTrue(check_issue_description(body))

    def test_required_search_checkbox(self):
        self.assertEqual(check_issue_description(FEATURE_BODY.replace("[x]", "[X]")), [])
        for search in (
            "", SEARCH.replace("[x]", "[ ]"),
            "### Search before asking\n- [x] A different checkbox.\n",
            "### Search before asking\n```markdown\n" + SEARCH.splitlines()[2] + "\n```\n",
        ):
            with self.subTest(search=search):
                self.assertTrue(check_issue_description(FEATURE_BODY.replace(SEARCH, search)))

    def test_comment_and_code_headings_do_not_supply_required_sections(self):
        for begin, end in (("<!--", "-->"), ("```markdown", "```"), ("~~~", "~~~")):
            body = FEATURE_BODY.replace("### Motivation", "### Other information")
            body += "\n" + begin + "\n### Motivation\nA reason.\n" + end
            with self.subTest(begin=begin):
                self.assertTrue(check_issue_description(body))

    def test_code_only_reproduction_steps_are_valid(self):
        body = BUG_BODY.replace(
            "Read an empty table with the example reader.",
            "```cpp\nReadTable(\"empty-table\");\n```"
        )
        self.assertEqual(check_issue_description(body), [])

    def test_template_field_names_match_checked_in_forms(self):
        template_dir = Path(__file__).resolve().parents[2] / ".github" / "ISSUE_TEMPLATE"
        for name, fields in (
            ("bug_report.yaml", ("Search before asking", "Paimon-cpp version",
                                 "Minimal reproduce step", "What doesn't meet your expectations?")),
            ("feature.yaml", ("Search before asking", "Motivation")),
        ):
            template = (template_dir / name).read_text(encoding="utf-8")
            for field in fields:
                with self.subTest(template=name, field=field):
                    self.assertIn("label: " + field, template)


class IssueReminderTest(unittest.TestCase):
    def test_valid_issue_has_no_comment(self):
        api = FakeGitHub(BUG_BODY)
        update_notice(REPOSITORY, 7, api)
        self.assertEqual(api.writes(), [])

    def test_invalid_issue_gets_one_reminder_with_template_links(self):
        api = FakeGitHub("Please fix this.")
        update_notice(REPOSITORY, 7, api)
        update_notice(REPOSITORY, 7, api)
        self.assertEqual(len(api.comments), 1)
        self.assertEqual(len(api.writes()), 1)
        body = api.comments[0]["body"]
        self.assertTrue(body.startswith(NOTICE_MARKER))
        self.assertIn("https://github.com/apache/paimon-cpp/issues/new?template=bug_report.yaml", body)
        self.assertIn("template=feature.yaml", body)
        self.assertIn("edit this issue", body)

    def test_changed_errors_update_the_same_comment(self):
        api = FakeGitHub("Please fix this.")
        update_notice(REPOSITORY, 7, api)
        api.issue["body"] = BUG_BODY.replace("0.4.0", "")
        update_notice(REPOSITORY, 7, api)
        self.assertEqual(len(api.comments), 1)
        self.assertEqual([call[0] for call in api.writes()], ["POST", "PATCH"])
        self.assertIn("Fill in 'Paimon-cpp version'", api.comments[0]["body"])

    def test_corrected_issue_updates_the_existing_reminder_once(self):
        api = FakeGitHub("Please fix this.")
        update_notice(REPOSITORY, 7, api)
        api.issue["body"] = FEATURE_BODY
        update_notice(REPOSITORY, 7, api)
        update_notice(REPOSITORY, 7, api)
        self.assertEqual(len(api.comments), 1)
        self.assertEqual([call[0] for call in api.writes()], ["POST", "PATCH"])
        self.assertIn("Thanks for updating", api.comments[0]["body"])

    def test_human_comment_cannot_spoof_bot_reminder(self):
        comment = bot_comment(NOTICE_MARKER)
        comment["user"] = {"login": "contributor", "type": "User"}
        api = FakeGitHub("Please fix this.", [comment])
        update_notice(REPOSITORY, 7, api)
        self.assertEqual(len(api.comments), 2)
        self.assertEqual(api.comments[0]["body"], NOTICE_MARKER)
        self.assertEqual(api.writes()[0][0], "POST")

    def test_comment_pagination_prevents_duplicate_reminders(self):
        comments = [{"id": number, "body": "Discussion", "user": {"type": "User"}}
                    for number in range(1, 101)]
        api = FakeGitHub("Please fix this.", comments)
        update_notice(REPOSITORY, 7, api)
        update_notice(REPOSITORY, 7, api)
        self.assertEqual(len(api.comments), 101)
        self.assertEqual(len(api.writes()), 1)
        self.assertTrue(any("page=2" in call[1] for call in api.calls))

    def test_closed_issues_and_prs_are_ignored(self):
        for extra in ({"state": "closed"}, {"pull_request": {}}):
            api = FakeGitHub("Please fix this.")
            api.issue.update(extra)
            update_notice(REPOSITORY, 7, api)
            self.assertEqual(len(api.calls), 1)
            self.assertEqual(api.writes(), [])

    def test_workflow_entrypoint_reads_latest_issue_instead_of_stale_event_body(self):
        api = FakeGitHub(FEATURE_BODY)
        with tempfile.TemporaryDirectory() as directory:
            event_path = Path(directory) / "event.json"
            event_path.write_text(json.dumps({"issue": {"number": 7, "body": "stale"}}))
            with mock.patch.dict(os.environ, {
                "GITHUB_EVENT_PATH": str(event_path), "GITHUB_REPOSITORY": REPOSITORY,
            }), mock.patch("check_issue_template.github_request", side_effect=api):
                self.assertEqual(main(), 0)
        self.assertEqual(api.writes(), [])

    def test_api_failure_is_reported_without_posting_a_comment(self):
        failure = urllib.error.HTTPError("https://api.github.com", 403, "Forbidden", {}, None)
        with tempfile.TemporaryDirectory() as directory:
            event_path = Path(directory) / "event.json"
            event_path.write_text(json.dumps({"issue": {"number": 7}}))
            with mock.patch.dict(os.environ, {
                "GITHUB_EVENT_PATH": str(event_path), "GITHUB_REPOSITORY": REPOSITORY,
            }), mock.patch("check_issue_template.github_request", side_effect=failure), \
                    mock.patch("sys.stderr", new_callable=io.StringIO) as stderr:
                self.assertEqual(main(), 1)
                self.assertIn("HTTP 403", stderr.getvalue())

    def test_api_request_sends_structured_json(self):
        response = io.BytesIO(b'{"id": 7}')
        with mock.patch.dict(os.environ, {
            "GITHUB_TOKEN": "test-token", "GITHUB_API_URL": "https://api.github.com",
        }), mock.patch("urllib.request.urlopen", return_value=response) as opener:
            self.assertEqual(github_request("POST", ISSUE_PATH + "/comments", {
                "body": "Please use the template.\n\nMore information."
            }), {"id": 7})
        request = opener.call_args[0][0]
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.full_url, "https://api.github.com" + ISSUE_PATH + "/comments")
        self.assertEqual(json.loads(request.data), {
            "body": "Please use the template.\n\nMore information."
        })


if __name__ == "__main__":
    unittest.main()
