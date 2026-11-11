#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 19b00e8c-7119-438b-bf83-2e4abf62c406
#
# This source code is licensed under the Business Source License 1.1
"""Generate a release's notes from the range between two refs.

Features are the requirements whose status became complete between the refs, each the first
sentence of its description, grouped by category. Changes are the feat and user-visible fix
commits of the range, grouped by scope; a commit citing a requirement completed in this release
is listed under that feature instead. A commit reverted before the release is left out, with its
revert (git's "This reverts commit <sha>"), and so is a commit scripts/release_notes.yaml lists
under `exclude`, each with its reason. No requirement id appears in any output.

The first release shown is set in scripts/release_notes.yaml (`start`: a date or a release tag;
empty shows every release). A release before it is left out of every output, and the pages and
CHANGELOG sections of earlier releases already written are removed.

Writes, relative to --repo:
  provisa-ui/src/generated/releaseNotes.json  the structured notes the UI bundles (null when
                                              the release is before the start)
  docs/releases/<version>.md                  the notes, rendered
  mkdocs.yml                                  the page's entry under "Release Notes"
  CHANGELOG.md                                the release prepended; the old list under "History"

Usage:
  python scripts/gen_release_notes.py --from v0.1.0-alpha.478 --to v0.1.0-alpha.479 [--version NAME]
"""

# Requirements: REQ-1989
from __future__ import annotations

import argparse
import datetime
import json
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import yaml

REQUIREMENTS = "docs/arch/requirements.yaml"
UI_JSON = Path("provisa-ui/src/generated/releaseNotes.json")
DOCS_DIR = Path("docs/releases")
MKDOCS = Path("mkdocs.yml")
CHANGELOG = Path("CHANGELOG.md")
NAV_GROUP = "  - Release Notes:"
HISTORY = "## History"
SETTINGS = Path("scripts/release_notes.yaml")

# A conventional subject: type(scope)!: text
_SUBJECT = re.compile(r"^(?P<type>[a-z]+)(?:\((?P<scope>[^)]+)\))?!?:\s*(?P<text>.+)$")
# Fixes to the project's own machinery, not to what a user runs.
_INTERNAL_FIX_SCOPES = {"tests", "test", "ci", "e2e", "ui-e2e"}
_CANARY = re.compile(r"\bcanar(?:y|ies)\b", re.IGNORECASE)
# REQ-12, REQ-12..14, REQ-12-14
_CITE = re.compile(r"REQ-(\d+)(?:\s*(?:\.\.|–)\s*(?:REQ-)?(\d+))?")
# A parenthesis holding only requirement ids, or a bare id with the punctuation that joins it.
_REQ_PAREN = re.compile(r"\s*\((?:see\s+)?REQ-\d+[^()]*\)")
_REQ_BARE = re.compile(r"\s*\bREQ-\d+(?:\s*(?:\.\.|–|-)\s*\d+)?(?:\s*[,:;])?")
_ANY_REQ = re.compile(r"REQ-?\d", re.IGNORECASE)
# A work item's id (R508g3, J1185) used as a commit scope names no part of the product.
_WORK_ITEM_SCOPE = re.compile(r"[RJ]\d+[a-z0-9]*", re.IGNORECASE)
_SENTENCE_END = re.compile(r"(?<!\be\.g)(?<!\bi\.e)(?<!\betc)(?<!\bvs)[.!?](?=\s+[A-Z(\"'`]|$)")


class ReleaseNotesError(Exception):
    """The notes cannot be generated as asked."""


def _git(repo: Path, *args: str) -> str:
    done = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True)
    if done.returncode != 0:
        raise ReleaseNotesError(f"git {' '.join(args)}: {done.stderr.strip()}")
    return done.stdout


def excluded_of(repo: Path) -> list[str]:
    """The commits the settings leave out of every release's notes, as full shas."""
    entries = (yaml.safe_load((repo / SETTINGS).read_text()) or {}).get("exclude") or []
    shas = []
    for entry in entries:
        if not isinstance(entry, dict) or not entry.get("sha") or not entry.get("reason"):
            raise ReleaseNotesError(
                f"{SETTINGS}: each exclude entry needs a sha and a reason: {entry!r}"
            )
        shas.append(_git(repo, "rev-parse", "--verify", f"{entry['sha']}^{{commit}}").strip())
    return shas


def start_of(repo: Path) -> datetime.date | str | None:
    """The first release the notes show: a date, a tag, or None for every release."""
    start = (yaml.safe_load((repo / SETTINGS).read_text()) or {}).get("start")
    if start is None or isinstance(start, datetime.date):
        return start
    if not isinstance(start, str) or not start:
        raise ReleaseNotesError(
            f"{SETTINGS}: start must be a date (YYYY-MM-DD) or a tag, not {start!r}"
        )
    _git(repo, "rev-parse", "--verify", f"{start}^{{commit}}")
    return start


def released_before(repo: Path, ref: str, start: datetime.date | str | None) -> bool:
    if start is None:
        return False
    if isinstance(start, datetime.date):
        released = _git(repo, "log", "-1", "--format=%cs", ref).strip()
        return datetime.date.fromisoformat(released) < start
    done = subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", start, ref])
    if done.returncode not in (0, 1):
        raise ReleaseNotesError(f"git merge-base --is-ancestor {start} {ref} failed")
    return done.returncode == 1


def strip_req_ids(text: str) -> str:
    text = _REQ_PAREN.sub("", text)
    text = _REQ_BARE.sub("", text)
    text = re.sub(r"\s{2,}", " ", text).strip()
    text = re.sub(r"\s+([,.;:])", r"\1", text)
    return text.lstrip(",;: ").strip()


def first_sentence(description: str) -> str:
    text = " ".join(strip_req_ids(description).split())
    end = _SENTENCE_END.search(text)
    return text[: end.end()] if end else text


def _requirements(repo: Path, ref: str) -> dict[str, dict]:
    entries = yaml.load(_git(repo, "show", f"{ref}:{REQUIREMENTS}"), Loader=yaml.CSafeLoader)
    return {entry["id"]: entry for entry in entries}


def completed_between(repo: Path, prev: str, new: str) -> dict[str, dict]:
    before, after = _requirements(repo, prev), _requirements(repo, new)
    return {
        req_id: entry
        for req_id, entry in after.items()
        if entry["status"] == "complete" and before.get(req_id, {}).get("status") != "complete"
    }


def cited(message: str) -> set[str]:
    ids: set[str] = set()
    for match in _CITE.finditer(message):
        start, end = int(match[1]), int(match[2] or match[1])
        width = len(match[1])
        ids.update(f"REQ-{n:0{width}d}" for n in range(start, end + 1))
    return ids


_REVERTS = re.compile(r"This reverts commit ([0-9a-f]{7,40})")


def _commits(repo: Path, prev: str, new: str) -> list[tuple[str, str]]:
    """The range's commits, oldest first, less any reverted within it, the reverts themselves,
    and those the settings exclude."""
    log = _git(
        repo, "log", "--reverse", "--no-merges", "--format=%H%x1f%s%x1f%b%x1e", f"{prev}..{new}"
    )
    records = []
    for record in log.split("\x1e"):
        if record.strip():
            sha, subject, body = record.strip("\n").split("\x1f", 2)
            records.append((sha, subject, body))
    reverted = [m for _, _, body in records for m in _REVERTS.findall(body)]
    reverted += excluded_of(repo)
    return [
        (subject, body)
        for sha, subject, body in records
        if not _REVERTS.search(body) and not any(sha.startswith(r) for r in reverted)
    ]


def _change(subject: str) -> tuple[str, str] | None:
    """The scope and text of a commit that belongs in the notes, or None."""
    match = _SUBJECT.match(subject)
    if not match or _CANARY.search(subject):
        return None
    # A scope of requirement ids is no scope: no id reaches the notes (REQ-1989).
    kind, scope = match["type"], strip_req_ids(match["scope"] or "")
    if _WORK_ITEM_SCOPE.fullmatch(scope):
        scope = ""
    if kind == "feat" or (kind == "fix" and scope not in _INTERNAL_FIX_SCOPES):
        text = strip_req_ids(match["text"])
        # A sentence starts with a capital; a name or identifier keeps its own spelling.
        first = text.split(" ", 1)[0]
        return scope, text[:1].upper() + text[1:] if first.isalpha() and first.islower() else text
    return None


def build(repo: Path, prev: str, new: str, version: str) -> dict:
    completed = completed_between(repo, prev, new)
    folded: dict[str, list[str]] = defaultdict(list)
    by_scope: dict[str, list[str]] = defaultdict(list)
    for subject, body in _commits(repo, prev, new):
        change = _change(subject)
        if change is None:
            continue
        scope, text = change
        home = sorted(cited(f"{subject}\n{body}") & completed.keys())
        if home:
            folded[home[0]].append(text)
        else:
            by_scope[scope].append(text)

    by_category: dict[str, list[dict]] = defaultdict(list)
    for req_id in sorted(completed, key=lambda r: int(r.split("-")[1])):
        entry = completed[req_id]
        by_category[entry["category"]].append(
            {"text": first_sentence(entry["description"]), "changes": folded[req_id]}
        )
    notes = {
        "version": version,
        "previous": prev,
        "date": _git(repo, "log", "-1", "--format=%cs", new).strip(),
        "features": [
            {"category": category, "entries": by_category[category]}
            for category in sorted(by_category)
        ],
        "changes": [
            {"scope": scope, "entries": by_scope[scope]}
            for scope in sorted(by_scope, key=lambda s: (s == "", s))
        ],
    }
    leaked = _ANY_REQ.search(json.dumps(notes, ensure_ascii=False))
    if leaked:
        raise ReleaseNotesError(f"a requirement id is left in the notes: {leaked.group()}")
    return notes


def render(notes: dict, level: int = 1) -> str:
    h = "#" * level
    lines = [f"{h} {notes['version']}", "", f"Released {notes['date']}.", ""]
    if notes["features"]:
        lines += [f"{h}# Features", ""]
        for group in notes["features"]:
            lines += [f"{h}## {group['category']}", ""]
            for entry in group["entries"]:
                lines.append(f"- {entry['text']}")
                lines += [f"    - {change}" for change in entry["changes"]]
            lines.append("")
    if notes["changes"]:
        lines += [f"{h}# Changes", ""]
        for group in notes["changes"]:
            lines += [f"{h}## {group['scope'] or 'General'}", ""]
            lines += [f"- {text}" for text in group["entries"]]
            lines.append("")
    if not notes["features"] and not notes["changes"]:
        lines += ["No user-visible changes.", ""]
    return "\n".join(lines)


def add_nav_entry(mkdocs: str, version: str) -> str:
    entry = f"      - {version}: releases/{version}.md"
    lines = mkdocs.split("\n")
    if entry in lines:
        return mkdocs
    if NAV_GROUP not in lines:
        start = lines.index("nav:")
        end = start + 1
        while end < len(lines) and lines[end].startswith("  "):
            end += 1
        lines.insert(end, NAV_GROUP)
    lines.insert(lines.index(NAV_GROUP) + 1, entry)
    return "\n".join(lines)


def _without_section(changelog: str, version: str) -> str:
    heading = f"## {version}\n"
    if heading not in changelog:
        return changelog
    start = changelog.index(heading)
    end = changelog.index("\n## ", start + len(heading)) + 1
    return changelog[:start] + changelog[end:]


def prepend_changelog(changelog: str, notes: dict) -> str:
    section = render(notes, level=2)
    if HISTORY not in changelog:
        changelog = changelog.replace("## [Unreleased]", HISTORY, 1)
        if HISTORY not in changelog:
            raise ReleaseNotesError(f"{CHANGELOG} has neither {HISTORY!r} nor '## [Unreleased]'")
    changelog = _without_section(changelog, notes["version"])
    at = changelog.index("\n## ") + 1
    return changelog[:at] + section + "\n" + changelog[at:]


def _changelog_versions(changelog: str) -> list[str]:
    head = changelog.split(f"\n{HISTORY}", 1)[0]
    return re.findall(r"^## (\S+)$", head, re.MULTILINE)


def drop_release(repo: Path, version: str) -> None:
    """Remove a written release from the docs pages, the nav and CHANGELOG.md."""
    page = repo / DOCS_DIR / f"{version}.md"
    if page.exists():
        page.unlink()
    entry = f"      - {version}: releases/{version}.md"
    lines = [line for line in (repo / MKDOCS).read_text().split("\n") if line != entry]
    (repo / MKDOCS).write_text("\n".join(lines))
    changelog = (repo / CHANGELOG).read_text()
    (repo / CHANGELOG).write_text(_without_section(changelog, version))


def write(repo: Path, notes: dict | None, start: datetime.date | str | None) -> None:
    (repo / UI_JSON).parent.mkdir(parents=True, exist_ok=True)
    (repo / UI_JSON).write_text(json.dumps(notes, indent=2, ensure_ascii=False) + "\n")
    if notes is not None:
        version = notes["version"]
        (repo / DOCS_DIR).mkdir(parents=True, exist_ok=True)
        (repo / DOCS_DIR / f"{version}.md").write_text(render(notes))
        (repo / MKDOCS).write_text(add_nav_entry((repo / MKDOCS).read_text(), version))
        (repo / CHANGELOG).write_text(prepend_changelog((repo / CHANGELOG).read_text(), notes))
    if start is None:
        return
    written = (
        {p.stem for p in (repo / DOCS_DIR).glob("*.md")} if (repo / DOCS_DIR).exists() else set()
    )
    written |= set(_changelog_versions((repo / CHANGELOG).read_text()))
    for version in sorted(written):
        if released_before(repo, version, start):
            drop_release(repo, version)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate a release's notes from two refs.")
    parser.add_argument("--from", dest="prev", required=True, help="the previous release's tag")
    parser.add_argument("--to", dest="new", required=True, help="the new release's tag or ref")
    parser.add_argument("--version", help="the release's name (default: --to)")
    parser.add_argument("--repo", type=Path, default=Path("."), help="the repository (default: .)")
    args = parser.parse_args(argv)
    version = args.version or args.new
    try:
        start = start_of(args.repo)
        before = released_before(args.repo, args.new, start)
        notes = None if before else build(args.repo, args.prev, args.new, version)
        write(args.repo, notes, start)
    except (ReleaseNotesError, KeyError, ValueError, OSError, yaml.YAMLError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    if notes is None:
        print(f"{version}: before the first release shown ({start}); no notes written")
        return 0
    features = sum(len(g["entries"]) for g in notes["features"])
    changes = sum(len(g["entries"]) for g in notes["changes"])
    print(f"{DOCS_DIR / (version + '.md')}: {features} features, {changes} changes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
