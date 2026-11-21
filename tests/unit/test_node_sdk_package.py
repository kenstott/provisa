# Copyright (c) 2026 Kenneth Stott
# Canary: 09ebc021-114b-4b02-95a6-0cd0faab4cf2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The optional TypeScript SDK package (provisa-sdk/typescript) and the server's copy of its
declarations (provisa/node/sdk.d.ts) say the same thing (REQ-1984): the package differs only in
taking the services types from its own copy, which is the server's services.d.ts; its JavaScript
offers exactly the wrappers its declarations declare, each returning its function; and it depends
on nothing."""

# Requirements: REQ-1984
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "provisa-sdk" / "typescript"
SERVER_SDK = ROOT / "provisa" / "node" / "sdk.d.ts"
SERVER_SERVICES = ROOT / "provisa" / "node" / "services.d.ts"


def _text(path: Path) -> str:
    """The file without its canary stamp, which differs per file by design."""
    return "".join(
        line for line in path.read_text().splitlines(keepends=True) if "Canary:" not in line
    )


def test_the_package_declarations_are_the_servers_with_their_own_services_types():
    server = _text(SERVER_SDK)
    assert '"@provisa/services"' in server
    assert _text(PACKAGE / "index.d.ts") == server.replace('"@provisa/services"', '"./services.js"')


def test_the_package_services_types_are_the_servers():
    assert _text(PACKAGE / "services.d.ts") == _text(SERVER_SERVICES)


def test_the_javascript_offers_exactly_the_declared_wrappers_and_spelling():
    declarations = (PACKAGE / "index.d.ts").read_text()
    script = (PACKAGE / "index.js").read_text()
    declared = set(re.findall(r"^export declare function (\w+)", declarations, re.M))
    declared |= set(re.findall(r"^export declare const (\w+)", declarations, re.M))
    offered = set(re.findall(r"^export const (\w+)", script, re.M))
    assert declared == offered == {
        "SPELLING", "provisa", "table", "stream", "command", "query", "columns", "onStart",
        "onStop", "health",
    }  # fmt: skip
    spelling = re.search(r"^export declare const SPELLING: (\d+);", declarations, re.M)
    assert spelling is not None
    assert f"export const SPELLING = {spelling.group(1)};" in script
    assert f"export type Spelling = {spelling.group(1)};" in declarations


def test_the_package_depends_on_nothing_and_ships_its_declarations():
    package = json.loads((PACKAGE / "package.json").read_text())
    assert package["name"] == "@provisa/sdk"
    assert not {"dependencies", "peerDependencies", "optionalDependencies"} & set(package)
    assert package["exports"]["."] == {"types": "./index.d.ts", "default": "./index.js"}
    assert set(package["files"]) == {"index.js", "index.d.ts", "services.d.ts"}
