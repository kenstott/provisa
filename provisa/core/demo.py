# Copyright (c) 2026 Kenneth Stott
# Canary: 1cbbf391-7348-4ad8-869a-6d3a78c79fa9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Demo-mode signal (REQ-1220). PROVISA_DEMO is exported by every demo launch path:
``provisa run --demo``, scripts/provisa, packaging first-launch, start-ui*.sh."""

from __future__ import annotations

import os
from pathlib import Path


def is_demo() -> bool:
    """True when the deployment was launched in demo mode (PROVISA_DEMO=1/true/yes)."""
    return os.environ.get("PROVISA_DEMO", "").lower() in ("1", "true", "yes")


NAMED_DEMO_DIR_ENV = "PROVISA_NAMED_DEMO_DIR"


def started_demo_directory() -> Path | None:  # REQ-2010
    """The directory of the named demo this deployment was started as (start-ui-install.sh --demo
    <name> names it), or None when none was. The sandbox tier, which serves many demos, names
    each request's own instead (setup_router)."""
    named = os.environ.get(NAMED_DEMO_DIR_ENV)
    return Path(named) if named else None


def named_demo(directory: Path | None) -> str | None:  # REQ-2010
    """The name of the named demo in ``directory``, or None when there is none: the UI keys the
    demo's own track and its words by it. A bundle's directory is its version; the demo is the
    name its bundle.json gives (REQ-2049)."""
    if directory is None:
        return None
    bundle = directory / "bundle.json"
    if bundle.is_file():
        import json

        return json.loads(bundle.read_text(encoding="utf-8"))["demo"]
    return directory.name


def example_queries(directory: Path | None) -> list[dict[str, str]]:  # REQ-2009
    """The example GraphQL queries the named demo in ``directory`` ships (``queries/*.graphql``),
    in file-name order: each its file's name without the extension, and its text. The GraphQL
    explorer opens them as tabs the first time the demo is opened. None when there is no named
    demo or it ships none."""
    if directory is None:
        return []  # no named demo: nothing ships examples
    return [
        {"name": path.stem, "query": path.read_text(encoding="utf-8")}
        for path in sorted((directory / "queries").glob("*.graphql"))
    ]


def tour_answers_path(named: str) -> Path:  # REQ-2012
    """Where the demo's bundle build stores Polly's answers to its tour's questions
    (scripts/demo_bundle.py):
    ``{"<step key>": {"question": ..., "answer": ...}}``."""
    return Path(named) / "tour_answers.json"


def tour_image(directory: Path, name: str) -> str:
    """The picture ``name`` (a PNG beside the demo's tour.yaml) as a data URL. Refused by name when
    it is not a PNG file there."""
    import base64

    path = directory / name
    if path.parent != directory or path.suffix != ".png" or not path.is_file():
        raise ValueError(f"{directory.name}: tour image {name!r} is not a PNG beside tour.yaml")
    return "data:image/png;base64," + base64.b64encode(path.read_bytes()).decode()


def demo_landing(directory: Path | None) -> dict[str, str] | None:  # REQ-2010
    """Where "Look around on my own" leaves a visitor at the end of the demo's tour: the page its
    tour.yaml names as ``landing`` (``route`` and the ``capability`` its gate needs). None when no
    named demo was started or it has no tour."""
    import yaml

    if directory is None:
        return None  # no named demo: no tour to end
    path = directory / "tour.yaml"
    if not path.is_file():
        return None  # a named demo without a tour of its own
    return yaml.safe_load(path.read_text(encoding="utf-8"))["landing"]


def demo_tour(directory: Path | None) -> list[dict[str, str]]:  # REQ-2010
    """The steps of the named demo's own tour (``demo/named/<name>/tour.yaml``), as written: the
    UI runs them as the demo's track, before the product tour. None when no named demo was
    started or it has no tour.

    REQ-2012: a step asking Polly a question (``polly``) carries the answer stored for it when the
    demo's bundle was built, as ``answer``, if one was stored for that same question; the UI
    leaves out a step with none. Nothing is asked when a visitor walks the tour.

    A step showing a picture (``image``: a PNG beside tour.yaml) carries it as ``image_src``, a
    data URL: the tour that names it is all the visitor's browser needs, on any tier."""
    import json

    import yaml

    if directory is None:
        return []  # no named demo: there is no demo tour
    named = str(directory)
    path = directory / "tour.yaml"
    if not path.is_file():
        return []  # a named demo without a tour of its own
    steps = yaml.safe_load(path.read_text(encoding="utf-8"))["steps"]
    for step in steps:
        if "image" in step:
            step["image_src"] = tour_image(directory, step["image"])
    answers_path = tour_answers_path(named)
    if not answers_path.is_file():
        return steps  # a demo not built as a bundle: no stored answers
    stored = json.loads(answers_path.read_text(encoding="utf-8"))
    for step in steps:
        entry = stored.get(step["key"]) if "polly" in step else None
        # An answer stored for another question is not this step's answer.
        if entry is not None and entry["question"] == step["polly"]:
            step["answer"] = entry["answer"]
    return steps
