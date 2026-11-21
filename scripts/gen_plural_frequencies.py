# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e2a91-5d4b-4f86-b1e0-8a6c2d9f4e17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""General-English frequencies of the tied noun plurals, for scripts/gen_noun_plurals.py (REQ-194).

Source: the Google Books Ngram Viewer English 1-grams, version 20200217
(http://storage.googleapis.com/books/ngrams/books/datasetsv3.html), licensed CC BY 3.0; the
attribution is kept in ``provisa/core/data/THIRD_PARTY_NOTICES``.

Usage, in two steps (the first streams about 6.4 GB and keeps only the listed words):

    python scripts/gen_plural_frequencies.py fetch <scowl-pre.txt> <counts.tsv>
    python scripts/gen_plural_frequencies.py table <scowl-pre.txt> <counts.tsv>

A word's count is the match count over 2000-2019 (current usage) of its lower-case 1-gram tagged
as a noun, ``schemas_NOUN``: the untagged 1-gram also counts every other word of the same spelling
(``does``, ``dies`` as verbs). ``table`` writes
``provisa/core/data/plural_frequencies.tsv``: every form gen_noun_plurals.tsv's ``tied_forms``
lists, with 0 for a form the corpus does not carry (Google Books keeps only 1-grams seen 40 times
or more), sorted by word.
"""

from __future__ import annotations

import gzip
import importlib.util
import sys
import urllib.request
from pathlib import Path

_SHARDS = 24
_URL = "http://storage.googleapis.com/books/ngrams/books/20200217/eng/1-{:05d}-of-{:05d}.gz"
_FIRST_YEAR, _LAST_YEAR = 2000, 2019
_NOUN_TAG = "_NOUN"
_HERE = Path(__file__).resolve().parent


def _generator():
    """scripts/gen_noun_plurals.py as a module (scripts/ is not a package)."""
    spec = importlib.util.spec_from_file_location("gen_noun_plurals", _HERE / "gen_noun_plurals.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def fetch(words: set[str], out: Path) -> None:
    """Stream every shard and write ``word<TAB>count`` for each listed word the corpus carries as a
    noun."""
    tagged = {f"{word}{_NOUN_TAG}": word for word in words}
    with out.open("w", encoding="utf-8") as sink:
        for shard in range(_SHARDS):
            with (
                urllib.request.urlopen(_URL.format(shard, _SHARDS)) as response,
                gzip.open(response, "rt", encoding="utf-8") as lines,
            ):
                for line in lines:
                    token, _, years = line.partition("\t")
                    word = tagged.get(token)
                    if word is None:
                        continue
                    count = 0
                    for record in years.rstrip("\n").split("\t"):
                        year, matches, _ = record.split(",")
                        if _FIRST_YEAR <= int(year) <= _LAST_YEAR:
                            count += int(matches)
                    sink.write(f"{word}\t{count}\n")
            print(f"shard {shard}", file=sys.stderr, flush=True)


def table(forms: list[str], counts: Path) -> Path:
    """Write the bundled table of ``forms``' counts from a ``fetch`` output."""
    seen: dict[str, int] = {}
    for line in counts.read_text(encoding="utf-8").splitlines():
        word, count = line.split("\t")
        seen[word] = int(count)
    generator = _generator()
    target: Path = generator._FREQUENCIES
    # A form the corpus does not carry was seen fewer than 40 times: its count is 0, not unknown.
    target.write_text("".join(f"{w}\t{seen.get(w, 0)}\n" for w in forms), encoding="utf-8")
    return target


def main(argv: list[str]) -> int:
    mode, scowl_pre, counts = argv[0], Path(argv[1]), Path(argv[2])
    forms = _generator().tied_forms(scowl_pre)
    if mode == "fetch":
        fetch(set(forms), counts)
    elif mode == "table":
        print(table(forms, counts))
    else:
        raise SystemExit(f"unknown mode {mode!r}: fetch or table")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
