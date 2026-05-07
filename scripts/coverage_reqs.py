#!/usr/bin/env python3
"""Report which REQ-NNN IDs in docs/arch/requirements.md have no test coverage."""
import re, sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
REQ_FILE = ROOT / "docs/arch/requirements.md"
TEST_DIRS = [ROOT / "tests/unit", ROOT / "tests/integration", ROOT / "tests/e2e"]

req_ids = re.findall(r'##\s+(REQ-\d+)', REQ_FILE.read_text())
covered = set()
for d in TEST_DIRS:
    if not d.exists():
        continue
    for f in d.rglob("*.py"):
        if f.name.startswith("._"):
            continue
        try:
            text = f.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        covered.update(re.findall(r'REQ-\d+', text))

uncovered = [r for r in req_ids if r not in covered]
print(f"Total REQs: {len(req_ids)}, Covered: {len(req_ids)-len(uncovered)}, Uncovered: {len(uncovered)}")
for r in uncovered:
    print(f"  UNCOVERED: {r}")
sys.exit(1 if uncovered else 0)
