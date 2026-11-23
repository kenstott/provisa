# Copyright (c) 2026 Kenneth Stott
# Canary: b5d3659d-4316-4479-8a46-95b6ec0fa32d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Code on the pip path names no GitHub host, except through the explicit list below.

A pip-installed Provisa must get everything it needs, at install time and at run time, from PyPI,
Maven Central and npm only: enterprise Artifactory proxies allow those and usually not GitHub
release assets or GitHub Pages (maintainer 2026-11-22; docs/arch/fetch-sources.md). The installers
under packaging/ may use GitHub and are not scanned.

The pip path is what the wheel ships and what pip resolves from it: the provisa package, the
vendored buenavista package, config/ (staged into provisa/_config), the UI sources and public
files (built into provisa/_ui), and pyproject.toml's declared dependencies. Python comments and
docstrings are prose, not code, and are not scanned; every other string literal is, for a GitHub
host with or without a scheme. Other files are scanned whole, for GitHub URLs.

Each entry of ALLOWED is one (file, matched text) pair present today, with the registry move it
waits for or why it is not a fetch. A new GitHub reference fails the test; an entry that no longer
matches fails it too, so the list only shrinks.
"""

# Requirements: REQ-294, REQ-1142, REQ-956

from __future__ import annotations

import ast
import re
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

#: A GitHub host (github.com and its subdomains such as api.github.com, *.github.io,
#: *.githubusercontent.com), with the URL scheme before it and the path after it.
_GITHUB = re.compile(
    r"(?:https?://)?(?<![\w.-])(?:[\w-]+\.)*(?:github\.com|github\.io|githubusercontent\.com)"
    r"[^\s\"'<>()\[\]\\`]*"
)
#: The same, with the scheme required: in non-Python files a bare ``github.com/...`` is a Go module or
#: protobuf go_package path (resolved through the GOPROXY, never fetched from github.com) or a
#: placeholder, so only a URL counts there.
_GITHUB_URL = re.compile(r"https?://" + _GITHUB.pattern.removeprefix("(?:https?://)?"))

#: The trees the wheel ships, relative to the repository root.
_PY_ROOTS = ("provisa", "vendor/buenavista")
_TEXT_ROOTS = ("provisa", "vendor/buenavista", "config", "provisa-ui/src", "provisa-ui/public")
_TEXT_FILES = ("provisa-ui/index.html",)
_TEXT_SUFFIXES = frozenset(
    {".mjs", ".js", ".cjs", ".ts", ".tsx", ".jsx", ".go", ".java", ".yaml", ".yml", ".json"}
    | {".css", ".html", ".toml", ".sql", ".proto", ".txt", ".mod", ".sum"}
)
#: UI test scaffolding is not built into provisa/_ui.
_UI_TEST_PARTS = frozenset({"__tests__", "__mocks__", "e2e"})

#: (file, matched text) -> the registry move it awaits, or why it is no fetch.
ALLOWED: dict[tuple[str, str], str] = {
    # --- Fetched at run time from a GitHub release: each awaits a registry home. ---
    (
        "provisa/runtime_deps/pgwire_bundles.py",
        "https://github.com/",
    ): "pgwire connector bundles (kenstott/calcite engine release, tarball + .sha256), fetched when "
    "a source of a pgwire connector type is first used. Move: the calcite engine release publishes "
    "each bundle to Maven Central as a per-platform classifier artifact; Provisa fetches it from "
    "Central (or the configured Maven mirror).",
    (
        "provisa/runtime_deps/code_runtimes.py",
        "https://github.com/oven-sh/bun/releases/download/bun-v{version}/{filename}",
    ): "Bun for JavaScript code sources. Move: npm, Oven's own per-platform packages "
    "(@oven/bun-darwin-aarch64, @oven/bun-linux-x64, @oven/bun-windows-x64, ...).",
    (
        "provisa/runtime_deps/code_runtimes.py",
        "https://github.com/denoland/deno/releases/download/v{version}/{filename}",
    ): "Deno for JavaScript code sources. Move: npm, the deno package's per-platform packages "
    "(@deno/<platform>).",
    (
        "provisa/runtime_deps/code_runtimes.py",
        "https://github.com/astral-sh/python-build-standalone/releases/download/20260924/{filename}",
    ): "CPython for the image of a Python code source built for a remote host's platform only; "
    "on the server a Python source runs on the interpreter Provisa runs on, with uv from PyPI "
    "(running_interpreter, uv_executable), and fetches nothing. Move: open question (no upstream "
    "registry home): republish the pinned build as a Provisa PyPI project once PyPI access "
    "returns.",
    (
        "provisa/runtime_deps/code_runtimes.py",
        "https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.12.1%2B1/{filename}",
    ): "Temurin JDK whose javac builds the sparql reasoner's jar (scripts/ci/reasoner_maven_bundle.py, "
    ".github/workflows/reasoner-maven-bundle.yml); the reasoner runs on jdk4py's Java from PyPI and "
    "its jar comes from Maven Central, so nothing on the pip path fetches it. Kept until "
    "io.simpleishard:provisa-sparql-reasoner is promoted: until then the jar cannot be fetched and "
    "the reasoner's tests build it. Move: the pin goes to the build script.",
    # --- Named, never fetched by Provisa. ---
    (
        "provisa/pg_extensions/catalog.py",
        "https://github.com/kenstott/provisa/releases/download",
    ): "provisa-pg-ext release URL, only quoted in ExtensionArtifactUnavailable's message (the "
    "runtime stages from the provisa-pg-ext wheel). Move: provisa-pg-ext on PyPI once access "
    "returns; then the message names the wheel instead.",
    (
        "provisa/openapi/brands.py",
        "https://raw.githubusercontent.com/stripe/openapi/master/openapi/spec3.json",
    ): "Stripe's published spec, read only at build time by scripts/bake_openapi_brand_spec.py; "
    "the wheel ships the baked copy (_known_specs). No move.",
    (
        "provisa/api/metadata_export/openlineage.py",
        "https://github.com/provisa-dev/provisa",
    ): "OpenLineage producer URI, an identifier in emitted events, never fetched. No move.",
    # --- Endpoints of GitHub services the user chooses to connect; not dependency fetches. ---
    (
        "provisa/core/env_remote.py",
        "github.com",
    ): "host key of the GitHub git-hosting adapter for a user's model remote. No move.",
    (
        "provisa/core/env_remote.py",
        "www.github.com",
    ): "host key of the GitHub git-hosting adapter for a user's model remote. No move.",
    (
        "provisa/core/env_remote.py",
        "https://api.github.com/user",
    ): "GitHub API of a user's model remote (REQ-125). No move.",
    (
        "provisa/core/env_remote.py",
        "https://api.github.com/user/repos",
    ): "GitHub API of a user's model remote (REQ-125). No move.",
    (
        "provisa/core/env_remote.py",
        "https://api.github.com/orgs/",
    ): "GitHub API of a user's model remote (REQ-125). No move.",
    (
        "provisa/core/env_remote.py",
        "https://api.github.com/repos/",
    ): "GitHub API of a user's model remote (REQ-125). No move.",
    (
        "provisa/graphql_remote/brands.py",
        "https://api.github.com/graphql",
    ): "the GitHub branded GraphQL source's endpoint (REQ-1923), a data source the user adds. "
    "No move.",
    # --- UI: links shown to the user and vendored-file provenance; the browser fetches none. ---
    (
        "provisa-ui/src/plugins/graphiql-explorer-fork.cjs",
        "https://github.com/graphql/graphiql/blob/master/packages/graphiql/css/app.css",
    ): "provenance comment in the vendored GraphiQL explorer. No move.",
    (
        "provisa-ui/src/plugins/graphiql-explorer-fork.cjs",
        "https://github.com/OneGraph/graphiql-explorer",
    ): "'Contribute on GitHub' link the explorer renders. No move.",
    (
        "provisa-ui/src/pages/sources/CodeSourceFields.tsx",
        "https://github.com/acme/rates.git",
    ): "example repository URL in a form field's placeholder. No move.",
    (
        "provisa-ui/public/voyager/voyager.css",
        "https://github.com/graphql-kit/graphql-voyager.git",
    ): "provenance comment in the vendored GraphQL Voyager stylesheet. No move.",
    (
        "provisa-ui/public/voyager/voyager.standalone.js",
        "https://github.com/graphql-kit/graphql-voyager",
    ): "link in the vendored GraphQL Voyager bundle. No move.",
}


def _prose_nodes(tree: ast.AST) -> set[int]:
    """ids of the string constants that are docstrings or other bare string statements."""
    return {
        id(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    }


def _python_literals(path: Path) -> list[str]:
    """Every str and bytes literal of ``path`` except docstrings and bare string statements."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    prose = _prose_nodes(tree)
    out: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or id(node) in prose:
            continue
        if isinstance(node.value, str):
            out.append(node.value)
        elif isinstance(node.value, bytes):
            out.append(node.value.decode("latin-1"))
    return out


def _text_files() -> list[Path]:
    files = [REPO / f for f in _TEXT_FILES if (REPO / f).is_file()]
    for root in _TEXT_ROOTS:
        for path in sorted((REPO / root).rglob("*")):
            if not path.is_file() or path.suffix not in _TEXT_SUFFIXES:
                continue
            if _UI_TEST_PARTS & set(path.relative_to(REPO).parts):
                continue
            files.append(path)
    return files


def _found() -> set[tuple[str, str]]:
    found: set[tuple[str, str]] = set()
    for root in _PY_ROOTS:
        for path in sorted((REPO / root).rglob("*.py")):
            if "github" not in path.read_text(encoding="utf-8"):
                continue
            rel = path.relative_to(REPO).as_posix()
            for literal in _python_literals(path):
                found |= {(rel, m.group(0)) for m in _GITHUB.finditer(literal)}
    for path in _text_files():
        text = path.read_text(encoding="utf-8", errors="replace")
        rel = path.relative_to(REPO).as_posix()
        found |= {(rel, m.group(0)) for m in _GITHUB_URL.finditer(text)}
    return found


def _declared_dependencies() -> list[str]:
    project = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    deps = list(project["dependencies"])
    for extra in project["optional-dependencies"].values():
        deps.extend(extra)
    return deps


def test_pip_path_names_no_github_host_outside_the_list() -> None:
    new = sorted(_found() - ALLOWED.keys())
    assert not new, (
        "code on the pip path names a GitHub host; a pip-installed Provisa must fetch only from "
        "PyPI, Maven Central and npm (docs/arch/fetch-sources.md). Give it a registry home, or add "
        f"it to ALLOWED with the move it awaits: {new}"
    )


def test_every_allowed_entry_is_still_present() -> None:
    stale = sorted(ALLOWED.keys() - _found())
    assert not stale, f"remove these ALLOWED entries, the code no longer names them: {stale}"


def test_declared_dependencies_name_no_github_url() -> None:
    hits = [d for d in _declared_dependencies() if _GITHUB.search(d)]
    assert not hits, f"pyproject.toml dependencies reference GitHub directly: {hits}"


def test_pattern_matches_each_github_host_form() -> None:
    for text, want in (
        ("x https://github.com/a/b y", "https://github.com/a/b"),
        ("https://objects.githubusercontent.com/x", "https://objects.githubusercontent.com/x"),
        (
            "https://kenstott.github.io/provisa/simple/",
            "https://kenstott.github.io/provisa/simple/",
        ),
        ('"api.github.com"', "api.github.com"),
    ):
        match = _GITHUB.search(text)
        assert match is not None and match.group(0) == want, (text, match)
    assert _GITHUB.search("notgithub.company.com") is None
    assert _GITHUB_URL.search("require github.com/apache/arrow-go/v18 v18.8.0") is None
    assert _GITHUB_URL.search("https://api.github.com/x").group(0) == "https://api.github.com/x"
