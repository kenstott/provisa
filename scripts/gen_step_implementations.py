#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: d7e3f1a2-9b4c-4d8e-a562-3f0b7c1e5d9a
#
# This source code is licensed under the Business Source License 1.1
"""Generate real BDD step implementations from requirements.yaml using Claude.

For each behavioral requirement with a scenario, calls claude-sonnet-4-6 to
generate step definitions that call real Provisa APIs/code with real assertions.
Organises output into domain step files: tests/steps/steps_{category_slug}.py

Usage:
  python scripts/gen_step_implementations.py --req REQ-001
  python scripts/gen_step_implementations.py --all
  python scripts/gen_step_implementations.py --all --skip-existing
  python scripts/gen_step_implementations.py --all --skip-existing --concurrency 10
"""

from __future__ import annotations

import argparse
import asyncio
import re
import sys
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).parent.parent))

import anthropic
import yaml

YAML_PATH = Path("docs/arch/requirements.yaml")
FEATURES_DIR = Path("tests/features")
STEPS_DIR = Path("tests/steps")
STUB_FILE = STEPS_DIR / "generated_stubs.py"

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def category_slug(category: str) -> str:
    return _SLUG_RE.sub("_", category.lower()).strip("_")


def steps_file_for(category: str) -> Path:
    return STEPS_DIR / f"steps_{category_slug(category)}.py"


def has_real_implementation(steps_file: Path, step_text: str) -> bool:
    """Return True if steps_file contains a non-stub implementation for step_text."""
    if not steps_file.exists():
        return False
    content = steps_file.read_text()
    if step_text not in content:
        return False
    # Find the function body after the step decorator; if it only contains
    # pytest.skip it is still a stub.
    idx = content.find(step_text)
    snippet = content[idx : idx + 400]
    return "pytest.skip" not in snippet


def collect_existing_tests(test_paths: list[str] | None) -> str:
    """Return concatenated content of up to 3 test files for context."""
    if not test_paths:
        return ""
    parts: list[str] = []
    for p in test_paths[:3]:
        path = Path(p)
        if path.exists():
            parts.append(f"# {p}\n{path.read_text()[:4000]}")
    return "\n\n".join(parts)


def collect_code_refs(code_paths: list[str] | None) -> str:
    """Return first ~200 lines of each code reference for context."""
    if not code_paths:
        return ""
    parts: list[str] = []
    for cp in code_paths[:4]:
        path = Path(cp)
        if path.is_file():
            lines = path.read_text().splitlines()[:200]
            parts.append(f"# {cp}\n" + "\n".join(lines))
        elif path.is_dir():
            for py in sorted(path.glob("*.py"))[:3]:
                lines = py.read_text().splitlines()[:100]
                parts.append(f"# {py}\n" + "\n".join(lines))
    return "\n\n".join(parts)


SYSTEM_PROMPT = """\
You are an expert Python/pytest-bdd developer generating BDD step implementations
for the Provisa data governance platform. You write real, working step definitions —
not stubs. Steps must call real Provisa APIs, classes, or functions and make real
assertions. No pytest.skip. No TODO comments. No pass.

Output ONLY valid Python source — no markdown fences, no prose. The output will be
written directly to a .py file.

File structure rules:
1. Include the standard copyright header (first 4 lines below, replace YEAR with 2026).
2. Import from provisa.* for server-side logic; use httpx.AsyncClient for HTTP calls.
3. Use pytest_asyncio fixtures and pytest_bdd (given, when, then, parsers).
4. Group all new steps without duplicating any step already in the existing file content
   provided to you (if any).
5. If a step requires live infrastructure (Docker, Trino, Kafka) that is unavailable
   in unit test context, add `@pytest.mark.integration` and guard with:
   `if not os.getenv("PROVISA_INTEGRATION"): pytest.skip("integration only")`.
6. Every step function must do real work or make a real assertion.
7. Use `shared_data` fixture (a plain dict) to pass state between Given/When/Then steps
   within the same scenario.

Standard copyright header:
# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""


def build_prompt(
    req: dict,
    feature_text: str,
    existing_file: str,
    test_context: str,
    code_context: str,
    append_mode: bool,
) -> str:
    if append_mode:
        # Append-only: emit ONLY the new steps for this requirement. Re-emitting
        # the whole (potentially thousands-of-lines) file is what overran the
        # token budget and truncated files mid-statement.
        output_instructions = f"""\
The steps file for this category ALREADY EXISTS (its full content is shown below).
Generate ONLY the NEW definitions this requirement needs, to be APPENDED to that file.
Do NOT re-emit the file header, existing imports, or any existing step definition.

Output, in this order, Python source only (no markdown, no prose):
1. Any import statements the new steps require that are NOT already present in the
   existing file content (omit entirely if none are needed).
2. A single line `scenarios("../features/{req["id"]}.feature")` — but OMIT it if that
   exact line already appears in the existing file content below.
3. The new step functions (given/when/then) for the scenario above. Do NOT duplicate
   any step already implemented in the existing content."""
    else:
        output_instructions = """\
The steps file for this category does not exist yet. Generate a COMPLETE new file:
the standard copyright header, imports, the scenarios(...) registration, and all step
functions for the scenario above. Output Python source only (no markdown, no prose)."""

    return f"""Generate pytest-bdd step implementations for the following requirement.

## Requirement
ID: {req["id"]}
Category: {req.get("category", "")}
Group: {req.get("group", "")}
Description: {req.get("description", "")}
Use case: {req.get("use_case", "")}

## Feature file (scenario to implement)
{feature_text}

## Code references (implement steps using these modules)
{code_context or "(none provided)"}

## Existing tests for this category (use as implementation guide)
{test_context or "(none provided)"}

## Existing steps file content (DO NOT duplicate anything already here)
{existing_file or "(file does not exist yet)"}

## Output
{output_instructions}
"""


# Unicode punctuation the model occasionally emits in code position (not inside a
# string), which is illegal Python. Mapping to ASCII is safe: inside a string it only
# changes text; in code position it turns an illegal token into a legal one. Quote
# characters are deliberately excluded — rewriting them could re-delimit a string, so
# any residual quote breakage is left to the model repair loop below.
_UNICODE_FIXUPS = {
    "—": "-",  # em dash
    "–": "-",  # en dash
    "→": "->",  # rightwards arrow
    "←": "<-",  # leftwards arrow
    "✓": "[check]",  # check mark
    "✗": "[x]",  # ballot x
    "…": "...",  # ellipsis
    "×": "x",  # multiplication sign
    "≥": ">=",  # greater-than-or-equal
    "≤": "<=",  # less-than-or-equal
    "≠": "!=",  # not equal
    " ": " ",  # non-breaking space
    "‘": "'",  # left single quote
    "’": "'",  # right single quote
    "“": '"',  # left double quote
    "”": '"',  # right double quote
}


def normalize_unicode(text: str) -> str:
    for bad, good in _UNICODE_FIXUPS.items():
        text = text.replace(bad, good)
    return text


def strip_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    return text.strip()


_FUTURE_RE = re.compile(r"^\s*from __future__ import .*$")
# Any string literal naming a feature file, in either the `scenarios("...")` or the
# `@scenario("...", "name")` form. The model routinely gets the relative depth and the
# filename casing wrong; the canonical location is always tests/features/REQ-NNN.feature.
_FEATURE_LIT_RE = re.compile(r"""["'][^"']*?[Rr][Ee][Qq][_-]?\d+\.feature["']""")
_PYTEST_BDD_IMPORT_RE = re.compile(r"^(from pytest_bdd import )(.+)$", re.MULTILINE)


def sanitize_fragment(text: str, req_id: str, append_mode: bool) -> str:
    """Fix the structural mistakes the model makes: a `from __future__` import in an
    appended fragment (illegal anywhere but a file's top), a wrong relative path or
    casing in the feature registration, and a `scenarios(` call whose import was
    omitted."""

    # Normalise every feature-path literal to the canonical location regardless of the
    # registration form the model chose.
    canonical_path = f'"../features/{req_id}.feature"'
    text = _FEATURE_LIT_RE.sub(canonical_path, text)

    if append_mode:
        text = "\n".join(line for line in text.splitlines() if not _FUTURE_RE.match(line))
    elif "scenarios(" in text and not re.search(r"\bscenarios\b", text.split("scenarios(")[0]):
        # New-file mode: the model called scenarios() but forgot to import it. Add it
        # to the existing pytest_bdd import line rather than inventing a new import.
        m = _PYTEST_BDD_IMPORT_RE.search(text)
        if m and "scenarios" not in m.group(2):
            text = _PYTEST_BDD_IMPORT_RE.sub(
                lambda mm: f"{mm.group(1)}{mm.group(2).rstrip()}, scenarios", text, count=1
            )
    return text


def is_noop_fragment(text: str) -> bool:
    """True if the fragment has no executable Python — only comments/blank lines.

    In append mode the model returns a comment-only note ("No new steps required...")
    plus a re-emitted copyright/canary header whenever a requirement's steps already
    exist. Such a fragment parses cleanly, so without this guard every regeneration run
    appends another dead comment block, growing the file unbounded (the observed bug).
    """
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            return False
    return True


def parse_error(source: str) -> SyntaxError | None:
    """Return the SyntaxError from compiling source, or None if it is valid.
    Uses compile() rather than ast.parse() so misplaced __future__ imports and
    other compile-only errors are caught before the file is written."""
    try:
        compile(source, "<generated>", "exec")
    except SyntaxError as exc:
        return exc
    return None


async def stream_text(
    client: anthropic.AsyncAnthropic,
    system: str,
    messages: list[anthropic.types.MessageParam],
) -> tuple[str, str | None]:
    """Stream a completion; return (text, stop_reason)."""
    content_parts: list[str] = []
    stop_reason: str | None = None
    async with client.messages.stream(
        model="claude-sonnet-4-6",
        max_tokens=8000,
        system=system,
        messages=messages,
    ) as stream:
        async for event in stream:
            if getattr(event, "type", None) == "content_block_delta":
                delta = getattr(event, "delta", None)
                if delta and getattr(delta, "type", None) == "text_delta":
                    content_parts.append(delta.text)
        final = await stream.get_final_message()
        stop_reason = final.stop_reason
    return "".join(content_parts), stop_reason


# Number of times to ask the model to repair unparseable output before giving up.
_MAX_REPAIR_ATTEMPTS = 2


async def repair_syntax(
    client: anthropic.AsyncAnthropic,
    system: str,
    prompt: str,
    broken: str,
    exc: SyntaxError,
) -> str | None:
    """Feed a SyntaxError back to the model until the output parses (or attempts run out)."""
    current = broken
    error = exc
    for _ in range(_MAX_REPAIR_ATTEMPTS):
        repair_prompt = (
            f"The Python you produced does not parse. Fix it and return the COMPLETE "
            f"corrected source only — no markdown, no prose. Preserve all real logic; "
            f"only make it valid Python (ASCII operators/punctuation, terminated string "
            f"literals, no leading-zero integer literals, no bare prose lines).\n\n"
            f"SyntaxError: {error} (line {error.lineno})\n\n"
            f"Source:\n{current}"
        )
        messages: list[anthropic.types.MessageParam] = [
            {"role": "user", "content": prompt},
            {"role": "assistant", "content": current},
            {"role": "user", "content": repair_prompt},
        ]
        text, stop_reason = await stream_text(client, system, messages)
        if stop_reason == "max_tokens":
            return None
        candidate = normalize_unicode(strip_fences(text))
        exc2 = parse_error(candidate)
        if exc2 is None:
            return candidate
        current, error = candidate, exc2
    return None


async def generate_for_req(
    client: anthropic.AsyncAnthropic,
    req: dict,
    skip_existing: bool,
    semaphore: asyncio.Semaphore,
    file_locks: dict[str, asyncio.Lock],
) -> Path | None:
    req_id = req["id"]
    category = req.get("category", "misc")
    scenario = req.get("scenario", "")
    if not scenario:
        return None

    steps_file = steps_file_for(category)

    feature_path = FEATURES_DIR / f"{req_id}.feature"
    if not feature_path.exists():
        print(f"  skip {req_id}: no feature file", file=sys.stderr)
        return None

    if skip_existing and steps_file.exists():
        all_real = True
        for line in scenario.splitlines():
            m = re.match(r"^\s*(Given|When|Then|And|But)\s+(.+)$", line)
            if m:
                step_text = m.group(2).strip()
                if not has_real_implementation(steps_file, step_text):
                    all_real = False
                    break
        if all_real:
            print(f"  skip {req_id}: all steps already implemented", flush=True)
            return None

    feature_text = feature_path.read_text()
    test_context = collect_existing_tests(req.get("tests"))
    code_context = collect_code_refs(req.get("code"))

    file_key = str(steps_file)
    if file_key not in file_locks:
        file_locks[file_key] = asyncio.Lock()

    async with semaphore:
        print(f"  generating {req_id} → {steps_file}", flush=True)

        async with file_locks[file_key]:
            existing_file = steps_file.read_text() if steps_file.exists() else ""

        append_mode = bool(existing_file.strip())
        prompt = build_prompt(
            req, feature_text, existing_file, test_context, code_context, append_mode
        )

        # Substitute a real per-file canary; the SYSTEM_PROMPT ships the literal
        # placeholder "{canary}", which otherwise lands verbatim in the output.
        system = SYSTEM_PROMPT.replace("{canary}", str(uuid4()))
        raw, stop_reason = await stream_text(client, system, [{"role": "user", "content": prompt}])

        # A truncated (max_tokens) generation is the root cause of mid-statement
        # file corruption — never write it over a good file.
        if stop_reason == "max_tokens":
            print(
                f"  ERROR {req_id}: generation hit max_tokens (truncated) — not writing",
                file=sys.stderr,
                flush=True,
            )
            return None

        generated = sanitize_fragment(normalize_unicode(strip_fences(raw)), req_id, append_mode)

        # The fragment must parse standalone (imports + scenarios + step functions).
        # If it does not, ask the model to repair its own output before giving up.
        exc = parse_error(generated)
        if exc is not None:
            print(
                f"  repair {req_id}: {exc} (line {exc.lineno}) — asking model to fix",
                file=sys.stderr,
                flush=True,
            )
            repaired = await repair_syntax(client, system, prompt, generated, exc)
            if repaired is None:
                print(
                    f"  ERROR {req_id}: generated content does not parse after "
                    f"{_MAX_REPAIR_ATTEMPTS} repair attempts — not writing",
                    file=sys.stderr,
                    flush=True,
                )
                return None
            # Re-apply structural fixes: repair may reintroduce a __future__ line
            # or an off path.
            generated = sanitize_fragment(repaired, req_id, append_mode)

        # In append mode a comment-only fragment means the model had no new steps to
        # add. Appending it accumulates dead comment blocks on every run, so drop it.
        if append_mode and is_noop_fragment(generated):
            print(f"  skip {req_id}: no new steps to append", flush=True)
            return None

        STEPS_DIR.mkdir(parents=True, exist_ok=True)
        async with file_locks[file_key]:
            # Re-read under the lock so we append to the freshest content.
            current = steps_file.read_text() if steps_file.exists() else ""
            if append_mode and current.strip():
                new_content = current.rstrip() + "\n\n\n" + generated + "\n"
            else:
                new_content = generated + "\n"

            # Final safety net: never overwrite a valid file with unparseable output.
            exc = parse_error(new_content)
            if exc is not None:
                print(
                    f"  ERROR {req_id}: assembled file does not parse ({exc}) — not writing",
                    file=sys.stderr,
                    flush=True,
                )
                return None

            steps_file.write_text(new_content)

        print(f"  done {req_id}", flush=True)
        return steps_file


def update_conftest(generated_files: list[Path]) -> None:
    """Add imports for new domain step files into conftest.py."""
    conftest = Path("tests/features/conftest.py")
    if not conftest.exists():
        return

    content = conftest.read_text()
    changed = False
    for sf in generated_files:
        module = f"tests.steps.{sf.stem}"
        import_line = f"import {module}  # type: ignore[import]  # noqa: F401"
        if import_line not in content:
            # Insert before the scenarios(".") call
            content = content.replace(
                'scenarios(".")',
                f'{import_line}\n\nscenarios(".")',
            )
            changed = True

    if changed:
        conftest.write_text(content)
        print(f"  updated {conftest}")


async def async_main() -> int:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--req", metavar="REQ-NNN", help="Generate for a single requirement")
    group.add_argument(
        "--all", action="store_true", help="Generate for all behavioral requirements"
    )
    parser.add_argument(
        "--skip-existing", action="store_true", help="Skip reqs whose steps are already implemented"
    )
    parser.add_argument(
        "--concurrency", type=int, default=10, help="Max parallel API calls (default: 10)"
    )
    args = parser.parse_args()

    with open(YAML_PATH) as f:
        all_reqs: list[dict] = yaml.safe_load(f)

    behavioral = [r for r in all_reqs if r.get("type") == "behavioral" and r.get("scenario")]

    if args.req:
        targets = [r for r in behavioral if r["id"] == args.req]
        if not targets:
            print(f"No behavioral requirement with scenario found for {args.req}", file=sys.stderr)
            return 1
    else:
        targets = behavioral

    client = anthropic.AsyncAnthropic()
    semaphore = asyncio.Semaphore(args.concurrency)
    file_locks: dict[str, asyncio.Lock] = {}

    async def run_one(req: dict) -> Path | None:
        try:
            return await generate_for_req(
                client,
                req,
                skip_existing=args.skip_existing,
                semaphore=semaphore,
                file_locks=file_locks,
            )
        except Exception as exc:
            print(f"  ERROR {req['id']}: {exc}", file=sys.stderr, flush=True)
            return None

    # Group by output file so reqs sharing a file run sequentially (each reads
    # the file after the previous one has written it). Different files run concurrently.
    from collections import defaultdict

    by_file: dict[str, list[dict]] = defaultdict(list)
    for r in targets:
        by_file[str(steps_file_for(r.get("category", "misc")))].append(r)

    async def run_group(reqs: list[dict]) -> list[Path | None]:
        return [await run_one(r) for r in reqs]

    group_results = await asyncio.gather(*[run_group(g) for g in by_file.values()])
    results = [p for group in group_results for p in group]
    generated = [p for p in results if p]

    unique = list({str(p): p for p in generated}.values())
    if unique:
        update_conftest(unique)
        print(f"\nWrote {len(unique)} domain step file(s).")
    else:
        print("Nothing generated.")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(async_main()))
