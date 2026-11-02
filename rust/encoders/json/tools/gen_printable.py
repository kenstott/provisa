# Copyright (c) 2026 Kenneth Stott
# Canary: 3b0f7c52-8e1d-4d55-9a57-6f0f6f3c1e21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Writes src/printable.rs: the code points Python's ``repr`` escapes (not ``str.isprintable``),
from the running interpreter's Unicode database. Run with the product's interpreter:
``.venv/bin/python -I tools/gen_printable.py > src/printable.rs``."""

import sys
import unicodedata

ranges = []
start = None
for cp in range(sys.maxunicode + 1):
    np_ = not chr(cp).isprintable()
    if np_ and start is None:
        start = cp
    if not np_ and start is not None:
        ranges.append((start, cp - 1))
        start = None
if start is not None:
    ranges.append((start, sys.maxunicode))
print(
    "//! Generated from Python %s's `str.isprintable` (Unicode %s): the code points `repr` escapes."
    % (sys.version.split()[0], unicodedata.unidata_version)
)
print("//! Regenerate with tools/gen_printable.py.")
print()
print("const NOT_PRINTABLE: &[(u32, u32)] = &[")
for a, b in ranges:
    print(f"    (0x{a:X}, 0x{b:X}),")
print("];")
print()
print("pub fn is_printable(cp: u32) -> bool {")
print(
    "    NOT_PRINTABLE.binary_search_by(|&(a, b)| if cp < a { std::cmp::Ordering::Greater }"
    " else if cp > b { std::cmp::Ordering::Less } else { std::cmp::Ordering::Equal }).is_err()"
)
print("}")
