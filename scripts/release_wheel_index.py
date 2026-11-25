# Copyright (c) 2026 Kenneth Stott
# Canary: c0a5a796-b35a-4f6a-a6d6-b0bbd4d9419a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The provisa package index: a PEP 503 simple index of the wheels on GitHub releases.

Nothing is published to PyPI (maintainer 2026-11-21). The server, client, notebook, extension and Spark data source wheels are
installed from the release assets through this index, served on GitHub Pages (``pip install provisa --extra-index-url
<pages>/simple/``). "Release: attach and publish" (.github/workflows/release-publish.yml) runs it
after each release is published, over every release of the repository:

    gh api repos/OWNER/REPO/releases --paginate --slurp > releases.json
    python scripts/release_wheel_index.py releases.json site

It writes ``site/simple/index.html`` and ``site/simple/<project>/index.html``. Each link is the
asset's release download URL with its ``#sha256=`` fragment from the asset's GitHub digest. Only
published releases count: a draft's assets are not served at their download URL.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
from collections.abc import Iterable
from pathlib import Path

PROJECTS = (
    "provisa",
    "provisa-client",
    "provisa-notebook",
    "provisa-duckdb-ext",
    "provisa-pg-ext",
    "provisa-sdk",
    "provisa-pyspark",
)


def normalize(name: str) -> str:
    """The PEP 503 normalized form of a project name."""
    return re.sub(r"[-_.]+", "-", name).lower()


def wheel_project(filename: str) -> str | None:
    """The normalized project of a wheel file name, or None when the asset is not a wheel."""
    if not filename.endswith(".whl"):
        return None
    parts = filename[: -len(".whl")].split("-")
    # {distribution}-{version}(-{build})?-{python}-{abi}-{platform}.whl
    if len(parts) not in (5, 6):
        raise ValueError(f"not a wheel file name: {filename}")
    return normalize(parts[0])


def _sha256(asset: dict) -> str:
    digest = asset.get("digest")
    if not digest or not digest.startswith("sha256:"):
        raise ValueError(f"release asset {asset['name']} has no sha256 digest: {digest!r}")
    return digest[len("sha256:") :]


def project_wheels(releases: Iterable[dict], project: str) -> list[tuple[str, str, str]]:
    """(file name, download URL, sha256) of every wheel of ``project`` on a published release."""
    want = normalize(project)
    wheels: dict[str, tuple[str, str, str]] = {}
    for release in releases:
        if release["draft"]:
            continue
        for asset in release["assets"]:
            name = asset["name"]
            if wheel_project(name) != want:
                continue
            if name in wheels:
                raise ValueError(f"wheel {name} is attached to more than one release")
            wheels[name] = (name, asset["browser_download_url"], _sha256(asset))
    return [wheels[name] for name in sorted(wheels)]


def _page(title: str, links: list[tuple[str, str]]) -> str:
    body = "\n".join(
        f'    <a href="{html.escape(href)}">{html.escape(text)}</a><br>' for href, text in links
    )
    return (
        "<!DOCTYPE html>\n<html>\n  <head>\n"
        '    <meta name="pypi:repository-version" content="1.0">\n'
        f"    <title>{html.escape(title)}</title>\n  </head>\n  <body>\n{body}\n  </body>\n</html>\n"
    )


def write_index(releases: list[dict], out: Path, projects: tuple[str, ...] = PROJECTS) -> list[str]:
    """Write the simple index for ``projects`` under ``out``; the wheel file names it lists."""
    simple = out / "simple"
    names: list[str] = []
    listed: list[str] = []
    for project in projects:
        wheels = project_wheels(releases, project)
        if not wheels:
            raise ValueError(f"no published release carries a {project} wheel")
        name = normalize(project)
        (simple / name).mkdir(parents=True, exist_ok=True)
        links = [(f"{url}#sha256={sha}", filename) for filename, url, sha in wheels]
        (simple / name / "index.html").write_text(_page(f"Links for {name}", links))
        names.append(name)
        listed.extend(wheel[0] for wheel in wheels)
    (simple / "index.html").write_text(_page("Simple index", [(f"{n}/", n) for n in names]))
    return listed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write the provisa wheel simple index.")
    parser.add_argument(
        "releases", type=Path, help="JSON of `gh api .../releases --paginate --slurp`"
    )
    parser.add_argument("out", type=Path, help="site directory to write simple/ into")
    args = parser.parse_args(argv)
    pages = json.loads(args.releases.read_text())
    # --slurp wraps each page of the paginated listing in an outer list.
    releases = [release for page in pages for release in page]
    listed = write_index(releases, args.out)
    print(f"{len(listed)} wheel(s) indexed")
    for filename in listed:
        print(f"  {filename}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
