#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Build the reference lists the profiler's reference classification kinds test against (REQ-2147).

Reads the GeoNames dump files from DIR and writes provisa/profiler/reference/*.txt.gz, one
normalised value a line (``classify.normalise``), sorted and without repeats:

* countries.txt.gz -- country names (countryInfo.txt, its Country column);
* country_codes.txt.gz -- ISO 3166 alpha-2 and alpha-3 codes (countryInfo.txt);
* currency_codes.txt.gz -- ISO 4217 codes of the countries' currencies (countryInfo.txt);
* language_codes.txt.gz -- ISO 639-1 codes and both ISO 639-2 forms (iso-languagecodes.txt);
* cities.txt.gz -- the names of the cities of 15,000 people or more (cities15000.txt, its name
  and asciiname columns).

DIR holds countryInfo.txt, iso-languagecodes.txt and cities15000.txt, from
https://download.geonames.org/export/dump/ (CC BY 4.0; the attribution is in
provisa/profiler/reference/THIRD_PARTY_NOTICES and THIRD_PARTY_NOTICES).

Usage: .venv/bin/python3 scripts/build_classification_reference.py DIR
"""

from __future__ import annotations

import gzip
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from provisa.profiler.classify import REFERENCE_DIR, normalise  # noqa: E402


def _rows(path: Path) -> list[list[str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [line.split("\t") for line in lines if line and not line.startswith("#")]


def _write(name: str, values: set[str]) -> None:
    kept = sorted(v for v in (normalise(x) for x in values) if v)
    path = REFERENCE_DIR / f"{name}.txt.gz"
    # mtime 0: the file is the same bytes for the same list, so a rebuild changes nothing.
    with gzip.GzipFile(path, "wb", mtime=0) as out:
        out.write(("\n".join(kept) + "\n").encode("utf-8"))
    print(f"{path.relative_to(ROOT)}: {len(kept)} values")


def main(source: Path) -> None:
    countries = _rows(source / "countryInfo.txt")
    _write("countries", {r[4] for r in countries})
    _write("country_codes", {r[0] for r in countries} | {r[1] for r in countries})
    _write("currency_codes", {r[10] for r in countries if r[10]})
    languages = _rows(source / "iso-languagecodes.txt")[1:]  # the first row names the columns
    # ISO 639-2 is written "fra / fre*" where a language has a terminology and a bibliographic code.
    part2 = {c.strip().rstrip("*") for r in languages for c in r[1].split("/")}
    _write("language_codes", {r[2] for r in languages if r[2]} | part2)
    cities = _rows(source / "cities15000.txt")
    _write("cities", {r[1] for r in cities} | {r[2] for r in cities})


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    main(Path(sys.argv[1]))
