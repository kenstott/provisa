# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Write the bank secrecy demo's vectors that the graph retrieval acceptance case reads offline.

``tests/unit/test_reference_library_graph.py`` (REQ-2185, GRAPH RETRIEVAL) embeds with the demo's
model, sentence-transformers/all-MiniLM-L6-v2, without loading it: every text the case embeds is
looked up in ``tests/fixtures/ecfr/banksecrecy-minilm.json``. This writes that file with the real
model: each passage of the 31 CFR sections in ``tests/fixtures/ecfr`` as the library cuts them,
each glossary term of the demo's config.yaml as a term is embedded (name, then definition), and the
case's questions (:data:`QUESTIONS`, kept as the test's). Run it again after changing a section,
the demo's glossary or a question.

Needs the vector extra (sentence-transformers) and the model, downloaded on first use.

Usage: .venv/bin/python3 scripts/gen_ecfr_minilm_vectors.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import yaml  # noqa: E402
from sentence_transformers import SentenceTransformer  # noqa: E402

from provisa.reference_library import extract  # noqa: E402
from provisa.reference_library.passages import cut  # noqa: E402
from provisa.reference_library.terms import Term, embedded_text  # noqa: E402

MODEL = "sentence-transformers/all-MiniLM-L6-v2"
ECFR = _REPO / "tests" / "fixtures" / "ecfr"
DEMO = _REPO / "demo" / "named" / "banksecrecy" / "config.yaml"
OUT = ECFR / "banksecrecy-minilm.json"

#: The acceptance case's questions (tests/unit/test_reference_library_graph.py).
QUESTIONS = (
    "which cash deposits need a currency transaction report",
    "a customer keeps depositing cash just under $10,000; what must the bank do",
)


def _passages() -> list[str]:
    texts: list[str] = []
    for path in sorted(ECFR.glob("31-cfr-*.xml")):
        document = extract.read(path.read_bytes(), "application/xml", path.name)
        texts += [p.text for p in cut(document)]
    return texts


def _terms() -> list[str]:
    declared = yaml.safe_load(DEMO.read_text())["glossary_terms"]
    return [
        embedded_text(Term(id=0, name=t["name"], definition=t.get("definition"), status=""))
        for t in declared
    ]


def main() -> None:
    texts = list(dict.fromkeys([*_passages(), *_terms(), *QUESTIONS]))
    vectors = SentenceTransformer(MODEL).encode(texts)
    OUT.write_text(json.dumps({t: [float(x) for x in v] for t, v in zip(texts, vectors)}))
    print(f"{OUT.relative_to(_REPO)}: {len(texts)} texts")


if __name__ == "__main__":
    main()
