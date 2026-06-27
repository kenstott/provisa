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
    req: dict, feature_text: str, existing_file: str, test_context: str, code_context: str
) -> str:
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

## Existing steps file content (DO NOT duplicate steps already here)
{existing_file or "(file does not exist yet — generate a complete new file)"}

Generate the complete updated steps file. Include all existing step definitions plus
the new implementations for the scenario above. Output Python source only.
"""


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

        prompt = build_prompt(req, feature_text, existing_file, test_context, code_context)

        content_parts: list[str] = []
        async with client.messages.stream(
            model="claude-sonnet-4-6",
            max_tokens=8000,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        ) as stream:
            async for event in stream:
                if hasattr(event, "type"):
                    if event.type == "content_block_delta":
                        delta = getattr(event, "delta", None)
                        if delta and getattr(delta, "type", None) == "text_delta":
                            content_parts.append(delta.text)

        generated = "".join(content_parts).strip()

        if generated.startswith("```"):
            lines = generated.splitlines()
            generated = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

        STEPS_DIR.mkdir(parents=True, exist_ok=True)
        async with file_locks[file_key]:
            steps_file.write_text(generated + "\n")

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

    results = await asyncio.gather(*[run_one(r) for r in targets])
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
