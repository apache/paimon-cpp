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

"""Check issue forms and maintain a single bot reminder for missing information."""

import json
import os
import re
import sys
import urllib.error
import urllib.request
from typing import List

from check_pr_metadata import parse_sections


SEARCH_SECTION = "Search before asking"
SEARCH_CONFIRMATION = (
    "I searched in the [issues](https://github.com/apache/paimon-cpp/issues) "
    "and found nothing similar."
)
BUG_FIELDS = (
    "Paimon-cpp version", "Minimal reproduce step", "What doesn't meet your expectations?"
)
FEATURE_FIELDS = ("Motivation",)
OPTIONAL_FIELDS = ("Solution", "Anything else?", "Are you willing to submit a PR?")
ISSUE_SECTIONS = (SEARCH_SECTION,) + BUG_FIELDS + FEATURE_FIELDS + OPTIONAL_FIELDS
NOTICE_MARKER = "<!-- paimon-cpp-issue-template-check -->"
BOT_LOGIN = "github-actions[bot]"


def check_issue_description(body: str) -> List[str]:
    sections, headings = parse_sections(body, ISSUE_SECTIONS)
    search_sections, _ = parse_sections(body, (SEARCH_SECTION,), include_fenced_code=False)
    search_checked = any(
        re.fullmatch(r"\s*[-*+]\s+\[[xX]\]\s+" + re.escape(SEARCH_CONFIRMATION) + r"\s*", line)
        for line in search_sections.get(SEARCH_SECTION, [])
    )
    candidates = []
    for fields in (BUG_FIELDS, FEATURE_FIELDS):
        if not any(field in sections for field in fields):
            continue
        errors = []
        if headings.count(SEARCH_SECTION) != 1 or not search_checked:
            errors.append("Complete 'Search before asking' and check the search confirmation box.")
        for field in fields:
            if headings.count(field) != 1:
                errors.append("Include exactly one '### " + field + "' section.")
                continue
            content = "\n".join(sections[field]).strip()
            if not re.search(r"\w", content) or content.strip("*_ .!").lower() in (
                "no response", "todo", "tbd", "na", "n/a"
            ):
                errors.append("Fill in '" + field + "' with the requested information.")
        candidates.append(errors)
    if not candidates:
        return ["Use the Bug report or Feature template and fill in its required fields."]
    return min(candidates, key=len)


def github_request(method, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/") + path,
        data=data,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": "Bearer " + os.environ["GITHUB_TOKEN"],
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method=method,
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def find_notice(api, issue_path):
    page = 1
    while True:
        comments = api("GET", issue_path + "/comments?per_page=100&page=" + str(page))
        for comment in comments:
            user = comment.get("user") or {}
            if (
                user.get("login") == BOT_LOGIN
                and user.get("type") == "Bot"
                and (comment.get("body") or "").startswith(NOTICE_MARKER)
            ):
                return comment
        if len(comments) < 100:
            return None
        page += 1


def update_notice(repository, issue_number, api, server_url="https://github.com"):
    issue_path = "/repos/" + repository + "/issues/" + str(issue_number)
    # Read the current body so queued events and reruns cannot restore an outdated reminder.
    issue = api("GET", issue_path)
    if "pull_request" in issue or issue.get("state") != "open":
        return
    errors = check_issue_description(issue.get("body") or "")
    notice = find_notice(api, issue_path)
    if errors:
        new_issue_url = server_url.rstrip("/") + "/" + repository + "/issues/new?template="
        message = (
            NOTICE_MARKER + "\n\n"
            "Thanks for opening this issue. Please use one of our issue templates:\n\n"
            "- [Bug report](" + new_issue_url + "bug_report.yaml)\n"
            "- [Feature request](" + new_issue_url + "feature.yaml)\n\n"
            "You can edit this issue to include the template sections and required information. "
            "There is no need to open a duplicate issue.\n\n"
            "Please address the following:\n\n"
            + "\n".join("- " + error for error in errors)
        )
    elif notice:
        message = NOTICE_MARKER + "\n\nThanks for updating this issue to follow our issue template."
    else:
        return
    if notice:
        if notice["body"] != message:
            api("PATCH", "/repos/" + repository + "/issues/comments/" + str(notice["id"]),
                {"body": message})
            print("Updated the issue template reminder.")
    else:
        api("POST", issue_path + "/comments", {"body": message})
        print("Posted the issue template reminder.")


def main() -> int:
    try:
        with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as source:
            event = json.load(source)
        issue_number = event["issue"]["number"]
        if not isinstance(issue_number, int) or issue_number <= 0:
            raise ValueError("Invalid issue number")
        update_notice(
            os.environ["GITHUB_REPOSITORY"], issue_number, github_request,
            os.environ.get("GITHUB_SERVER_URL", "https://github.com"),
        )
    except urllib.error.HTTPError as error:
        print("GitHub API request failed with HTTP " + str(error.code) + ".", file=sys.stderr)
        return 1
    except (OSError, KeyError, TypeError, ValueError) as error:
        print("Cannot check issue template: " + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
