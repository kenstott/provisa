# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e9b41-58d3-4a6f-b0e1-93d4f2a6c815
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pinned Trino plugin jars the test stacks mount: their pin, their sha256, fetch and check.

Standard library only, so a workflow step runs it with the runner's own Python before any
environment exists:

    python3 tests/trino_plugins.py fetch    # download each pinned jar, each checked by sha256
    python3 tests/trino_plugins.py verify   # every pinned jar present with its recorded sha256

The integration workflows fetch once per pin in a job of their own and cache the jars
(.github/actions/trino-plugins); every lane restores that cache and verifies it before pytest, so
no lane asks Maven Central for anything at collection time (release 510: 18 lanes missing the
cache at once were refused with HTTP 429). tests/conftest.py checks the same sums.

The pin is plain assignments of literals: .github/actions/trino-plugins reads it with ast for its
cache key.
"""

from __future__ import annotations

import hashlib
import os
import sys
import urllib.error
import urllib.request

# The released Calcite Trino plugins (io.simpleishard on Maven Central); each jar unpacked
# into trino/plugins/<name>/ is a complete plugin directory.
_TRINO_PLUGIN_MAVEN = "https://repo1.maven.org/maven2/io/simpleishard"
_TRINO_PLUGIN_VERSION = "0.106.3"
_TRINO_PLUGINS = (
    "trino-sharepoint",
    "trino-splunk",
    "trino-file",
    "trino-salesforce",
    "trino-cloudops",
)
# Plugins fetched at a version other than _TRINO_PLUGIN_VERSION; a plugin absent here is at
# _TRINO_PLUGIN_VERSION. The same table as provisa/runtime_deps/pgwire_bundles.py
# CONNECTOR_RELEASE, for the same reason, and each entry goes when _TRINO_PLUGIN_VERSION moves.
#   trino-salesforce: 0.108.0 is the first release that has it (REQ-1946).
#   trino-cloudops: 0.108.0 is the first whose plugin takes the `schema` property (REQ-1947).
#   trino-file: 0.109.0 is the first whose plugin takes the `crawl` property (REQ-1960).
_TRINO_PLUGIN_VERSIONS = {
    "trino-salesforce": "0.108.0",
    "trino-cloudops": "0.108.0",
    "trino-file": "0.109.0",
}
# The sha256 of each pinned jar, taken from the Maven Central artifact (its published .sha1
# matched). A repin changes the version and the sum together.
_TRINO_PLUGIN_SHA256 = {
    "trino-sharepoint": "8c33f0b22a99baceb4b57a6121cc149d82f310c4589b63a516339dd2a2b66974",
    "trino-splunk": "074f0e2818dcb5eff03eca1ad19b02a03312bebb4bf1693122453fbd4b232adf",
    "trino-file": "20cfe75f88223c698e8c7eeb2cd521c0ceed810d0c8571cb3ddb43479dfcb3a6",
    "trino-salesforce": "f75bbd16ce56c6ff766a34582aa745b99ed44cb150d91bf485cd2d145eb65d65",
    "trino-cloudops": "e4b421d0bacdfbb48cf4c5fc12cac514d1b8fe890f03c14ddc2a58c8190f77d2",
}

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TrinoPluginRefused(RuntimeError):
    """A pinned plugin jar is missing, or its bytes are not the pinned build's."""


def plugin_version(name: str) -> str:
    return _TRINO_PLUGIN_VERSIONS.get(name, _TRINO_PLUGIN_VERSION)


def jar_name(name: str) -> str:
    return f"{name}-{plugin_version(name)}.jar"


def plugin_url(name: str) -> str:
    version = plugin_version(name)
    return f"{_TRINO_PLUGIN_MAVEN}/{name}/{version}/{jar_name(name)}"


def sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def jar_problem(path: str, name: str) -> str | None:
    """Why the jar at ``path`` is not the pinned ``name`` build, or None when it is."""
    if not os.path.isfile(path):
        return f"{path} is missing (the pinned {jar_name(name)})"
    actual = sha256(path)
    if actual != _TRINO_PLUGIN_SHA256[name]:
        return (
            f"{path} has sha256 {actual}, not the pinned {jar_name(name)}'s "
            f"{_TRINO_PLUGIN_SHA256[name]}"
        )
    return None


def fetch(target: str, name: str) -> str:
    """Download the pinned ``name`` jar into the directory ``target``; return its path.

    The bytes are checked against the recorded sha256 before the jar takes its name, so a refused,
    truncated or substituted download is refused by name and leaves no jar behind.
    """
    os.makedirs(target, exist_ok=True)
    path = os.path.join(target, jar_name(name))
    part = path + ".part"
    url = plugin_url(name)
    print(f"[trino-plugins] downloading {name} from {url}", flush=True)
    try:
        urllib.request.urlretrieve(url, part)  # noqa: S310 — pinned https Maven Central URL
    except urllib.error.URLError as exc:
        if os.path.exists(part):
            os.unlink(part)
        raise TrinoPluginRefused(f"Trino plugin {name}: {url} was refused: {exc}") from exc
    problem = jar_problem(part, name)
    if problem is not None:
        if os.path.exists(part):
            os.unlink(part)
        raise TrinoPluginRefused(f"Trino plugin {name} from {url}: {problem}")
    os.replace(part, path)
    return path


def plugin_paths(root: str) -> dict[str, str]:
    plugins = os.path.join(root, "trino", "plugins")
    return {name: os.path.join(plugins, name, jar_name(name)) for name in _TRINO_PLUGINS}


def verify(root: str) -> None:
    """Every pinned jar under ``root``/trino/plugins is present with its recorded sha256; the
    refusal names each one that is not."""
    problems = [
        f"Trino plugin {name}: {problem}"
        for name, path in plugin_paths(root).items()
        if (problem := jar_problem(path, name)) is not None
    ]
    if problems:
        raise TrinoPluginRefused("\n".join(problems))


def fetch_all(root: str) -> None:
    for name, path in plugin_paths(root).items():
        fetch(os.path.dirname(path), name)
    verify(root)


def main(argv: list[str]) -> int:
    if argv not in (["fetch"], ["verify"]):
        print("usage: trino_plugins.py fetch|verify", file=sys.stderr)
        return 2
    try:
        if argv == ["fetch"]:
            fetch_all(_REPO_ROOT)
        else:
            verify(_REPO_ROOT)
    except TrinoPluginRefused as exc:
        print(f"::error::{exc}".replace("\n", "%0A"), file=sys.stderr)
        return 1
    for name, path in plugin_paths(_REPO_ROOT).items():
        print(f"[trino-plugins] {name}: {os.path.relpath(path, _REPO_ROOT)} sha256 ok")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
