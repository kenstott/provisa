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

The same scan covers every other host. Each http(s) URL host on the pip path is one of
REGISTRY_HOSTS (PyPI, Maven Central, npm) or a key of HOSTS, which names its move or why it is no
dependency fetch. IMPLICIT lists the hosts a dependency reaches by its own default, which no URL in
our code names, each with the code that still leads there. A new host fails the test; a listed host
the code no longer names fails it too.
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
    ): "Temurin JDK for the sparql reasoner. Move: open question (no upstream JDK on PyPI, Maven "
    "Central or npm): precompile the reasoner to a jar on Maven Central and run it on a JRE from "
    "a registry, or republish the JDK as a Provisa package once PyPI access returns.",
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


#: An http(s) URL's host, ASCII only (a URL inside translated text runs on into the sentence).
_URL_HOST = re.compile(r"https?://([A-Za-z0-9{\[][A-Za-z0-9{}\[\]._-]*)")

#: The three registries a pip-installed Provisa may fetch from.
REGISTRY_HOSTS = frozenset(
    {
        "pypi.org",  # PyPI simple index: the Python code sources' default package registry
        "files.pythonhosted.org",  # PyPI files: pyarrow for the Python code-source host
        "registry.npmjs.org",  # npm: the JavaScript code sources' default registry, apache-arrow
        "repo1.maven.org",  # Maven Central: HermiT and OWL API for the sparql reasoner
    }
)

_PLACEHOLDER = "placeholder or example address in a form hint or docstring; never fetched. No move."
_LOCAL = (
    "loopback or in-cluster service name (this machine, a compose or Kubernetes service); "
    "never leaves the deployment. No move."
)
_IN_PROCESS = "base URL of an in-process ASGI client; no network. No move."
_NAMESPACE = "namespace, schema or scope identifier; never fetched. No move."
_VENDORED_LINK = (
    "link or provenance comment inside a vendored UI bundle or stylesheet; the browser fetches "
    "nothing from it. No move."
)
_UI_LINK = "link the UI shows the user to follow; not a dependency fetch. No move."
_USER_SERVICE = (
    "API of a service the user chooses to connect (a source, sign-in, mail, model, catalog or "
    "cloud the organization configures); the data or service itself, not a dependency. No move."
)
_DATASET = (
    "provenance of a government dataset in the govdata catalog, read when the user adds that "
    "source; data, not a dependency. No move."
)
_MIRRORABLE_REGISTRY = (
    "public default of a code-source package registry; the organization replaces it with its own "
    "mirror in the Package Registries setting. No move."
)

#: host -> its move, or why it is no dependency fetch. Hosts outside REGISTRY_HOSTS only.
HOSTS: dict[str, str] = {
    # --- Dependency fetches outside the three registries: each names its move. ---
    "github.com": "GitHub release assets and links; each reference is listed with its move in "
    "ALLOWED above.",
    "raw.githubusercontent.com": "Stripe spec read only at build time; see ALLOWED above. No move.",
    "nodejs.org": "Node for JavaScript code sources (provisa/runtime_deps/code_runtimes.py). Move "
    "(follow-up): resolve the runtime through the organization's configured npm registry from "
    "per-platform Node packages on npm.",
    "go.dev": "Go toolchain for Go code sources (code_runtimes.py). Move (follow-up): open "
    "question, no upstream Go toolchain on PyPI, Maven Central or npm; republish the pinned "
    "toolchain as a Provisa package resolved through the configured registries.",
    "static.rust-lang.org": "Rust toolchain components for Rust code sources (code_runtimes.py). "
    "Move (follow-up): open question, no upstream Rust toolchain on PyPI, Maven Central or npm; "
    "republish the pinned components as Provisa packages resolved through the configured "
    "registries.",
    "huggingface.co": "default embedding model all-MiniLM-L6-v2, fetched at first embedding "
    "(embedding.hub_endpoint, HF_ENDPOINT). Move (follow-up): the setting already points at an "
    "organization's Hugging Face mirror; the registry move is to publish the pinned model as a "
    "Provisa package on PyPI once PyPI access returns.",
    "duckdb-extensions.query.farm": "Query.Farm repository for the crypto and airport DuckDB "
    "extensions, reached only without the embedded extra (provisa/federation/duckdb_extensions.py)."
    " Move: install provisa[embedded]; provisa-duckdb-ext carries them, and goes to PyPI once "
    "access returns.",
    "ghcr.io": "published demo bundles (provisa/core/demo_bundles.py PUBLISHED_AT), an OCI "
    "registry; PROVISA_DEMO_REGISTRY or --registry points at a mirror. Move (follow-up): open "
    "question, none of the three registries hosts OCI artifacts; publish the demo bundles as a "
    "Provisa package on PyPI or npm.",
    "provisa.dev": "the project site: User-Agent and ontology namespace strings, the registration "
    "and online-docs links (offline docs ship in the wheel), and the tour's sample Parquet file "
    "(provisa-ui/src/tour/tourSteps.ts). Move (follow-up): ship the tour's sample file in the "
    "wheel.",
    "proxy.golang.org": _MIRRORABLE_REGISTRY,
    "index.crates.io": _MIRRORABLE_REGISTRY,
    # --- Services the user chooses to connect. ---
    "accounts.google.com": _USER_SERVICE,
    "acme.atlassian.net": _PLACEHOLDER,
    "acme.my.salesforce.com": _PLACEHOLDER,
    "acme.service-now.com": _PLACEHOLDER,
    "admin.googleapis.com": _USER_SERVICE,
    "api.anthropic.com": _USER_SERVICE,
    "api.askamerica.ai": _USER_SERVICE,
    "api.cohere.com": _USER_SERVICE,
    "api.deepseek.com": _USER_SERVICE,
    "api.fabric.microsoft.com": _USER_SERVICE,
    "api.fireworks.ai": _USER_SERVICE,
    "api.github.com": _USER_SERVICE,
    "api.groq.com": _USER_SERVICE,
    "api.inceptionlabs.ai": _USER_SERVICE,
    "api.mailgun.net": _USER_SERVICE,
    "api.mistral.ai": _USER_SERVICE,
    "api.openai.com": _USER_SERVICE,
    "api.postmarkapp.com": _USER_SERVICE,
    "api.resend.com": _USER_SERVICE,
    "api.sambanova.ai": _USER_SERVICE,
    "api.sendgrid.com": _USER_SERVICE,
    "api.studio.nebius.com": _USER_SERVICE,
    "api.together.xyz": _USER_SERVICE,
    "api.typesafe.ai": _USER_SERVICE,
    "biglake.googleapis.com": _USER_SERVICE,
    "api.x.ai": _USER_SERVICE,
    "appcenter.intuit.com": _USER_SERVICE,
    "cloudresourcemanager.googleapis.com": _USER_SERVICE,
    "compute.googleapis.com": _USER_SERVICE,
    "container.googleapis.com": _USER_SERVICE,
    "database.windows.net": _USER_SERVICE,
    "gitlab.com": _USER_SERVICE,
    "gmail.googleapis.com": _USER_SERVICE,
    "graph.microsoft.com": _USER_SERVICE,
    "login.microsoftonline.com": _USER_SERVICE,
    "management.azure.com": _USER_SERVICE,
    "oauth.platform.intuit.com": _USER_SERVICE,
    "oauth2.googleapis.com": _USER_SERVICE,
    "onelake.dfs.fabric.microsoft.com": _USER_SERVICE,
    "openidconnect.googleapis.com": _USER_SERVICE,
    "query.wikidata.org": _USER_SERVICE,
    "quickbooks.api.intuit.com": _USER_SERVICE,
    "sandbox-quickbooks.api.intuit.com": _USER_SERVICE,
    "sheets.googleapis.com": _USER_SERVICE,
    "storage.googleapis.com": _USER_SERVICE,
    "tasks.googleapis.com": _USER_SERVICE,
    "www.googleapis.com": _USER_SERVICE,
    "www.kaggle.com": _USER_SERVICE,
    "{store}.myshopify.com": _USER_SERVICE,
    "metadata.google.internal": "GCP instance metadata server, reached only on a GCP host; "
    "never leaves the cloud. No move.",
    "cloud.provisa.dev": "link to the hosted listing in outgoing mail; not fetched. No move.",
    # --- Government datasets (provisa/govdata/catalog_metadata.json). ---
    "coralreefwatch.noaa.gov": _DATASET,
    "doi.org": _DATASET,
    "download.bls.gov": _DATASET,
    "fd.house.gov": _DATASET,
    "mesonet.agron.iastate.edu": _DATASET,
    "satepsanone.nesdis.noaa.gov": _DATASET,
    "www.federalregister.gov": _DATASET,
    "www.ncei.noaa.gov": _DATASET,
    "www.sec.gov": _DATASET,
    "www.uscis.gov": _DATASET,
    "www2.census.gov": _DATASET,
    # --- Namespaces, schemas and scopes. ---
    "docs.oasis-open.org": _NAMESPACE,
    "openlineage.io": _NAMESPACE,
    "purview.azure.net": _NAMESPACE,
    "schemas.microsoft.com": _NAMESPACE,
    "shopify.dev": _NAMESPACE,
    "specs.apollo.dev": _NAMESPACE,
    "wikiba.se": _NAMESPACE,
    "www.apache.org": _NAMESPACE,
    "www.w3.org": _NAMESPACE,
    "www.wikidata.org": _NAMESPACE,
    # --- Links the UI shows, and vendored-bundle provenance. ---
    "askamerica.ai": _UI_LINK,
    "cloud.getdbt.com": _UI_LINK,
    "dbpedia.org": _UI_LINK,
    "en.wikipedia.org": _UI_LINK,
    "js.cytoscape.org": _UI_LINK,
    "vscode.dev": _UI_LINK,
    "js.stripe.com": "Stripe.js for the hosted billing page; Stripe requires it be loaded from "
    "js.stripe.com, never bundled (provisa-ui/src/api/billing.ts). A hosted-billing service the "
    "deployment opts into, not a dependency of a pip install. No move.",
    "commonmark.org": _VENDORED_LINK,
    "drafts.csswg.org": _VENDORED_LINK,
    "mui.com": _VENDORED_LINK,
    "reactjs.org": _VENDORED_LINK,
    "www.onegraph.com": _VENDORED_LINK,
    # --- Placeholders and examples. ---
    "amzn-s3-demo-bucket.s3.us-west-2.amazonaws.com": _PLACEHOLDER,
    "api.example.com": _PLACEHOLDER,
    "cargo.example.com": _PLACEHOLDER,
    "catalog.example.com": _PLACEHOLDER,
    "ci.example.com": _PLACEHOLDER,
    "contoso.sharepoint.com": _PLACEHOLDER,
    "data.example.com": _PLACEHOLDER,
    "datahub.example.com": _PLACEHOLDER,
    "dbc-xxxxx.cloud.databricks.com": _PLACEHOLDER,
    "example.com": _PLACEHOLDER,
    "example.org": _PLACEHOLDER,
    "goproxy.example.com": _PLACEHOLDER,
    "host": _PLACEHOLDER,
    "idp.example.com": _PLACEHOLDER,
    "issuer": _PLACEHOLDER,
    "keycloak.example.com": _PLACEHOLDER,
    "my-vault.vault.azure.net": _PLACEHOLDER,
    "myvault.vault.azure.net": _PLACEHOLDER,
    "npm.example.com": _PLACEHOLDER,
    "openmetadata.example.com": _PLACEHOLDER,
    "openrouter.ai": _PLACEHOLDER,
    "otel.provisa.io": _PLACEHOLDER,
    "provisa.example.com": _PLACEHOLDER,
    "pypi.example.com": _PLACEHOLDER,
    "s3": _PLACEHOLDER,
    "svc.internal": _PLACEHOLDER,
    "vault.internal": _PLACEHOLDER,
    "x.example": _PLACEHOLDER,
    "your-site.atlassian.net": _PLACEHOLDER,
    # --- This machine and in-cluster services. ---
    "0.0.0.0": _LOCAL,
    "127.0.0.1": _LOCAL,
    "docker": _LOCAL,
    "fuseki": _LOCAL,
    "graphql-demo": _LOCAL,
    "localhost": _LOCAL,
    "minio": _LOCAL,
    "otel-collector": _LOCAL,
    "petstore-mock": _LOCAL,
    "prometheus": _LOCAL,
    "provisa": _LOCAL,
    "schema-registry": _LOCAL,
    "trino": _LOCAL,
    "nl": _IN_PROCESS,
    "warmup": _IN_PROCESS,
}

#: host -> (file, text that still leads there, its move): hosts a dependency reaches by its own
#: default, named by no URL in our code.
IMPLICIT: dict[str, tuple[str, str, str]] = {
    "extensions.duckdb.org": (
        "provisa/federation/duckdb_extensions.py",
        'return [f"INSTALL {extension} FROM community" if from_community else f"INSTALL {extension}"]',
        "DuckDB's core extension repository, which a bare INSTALL reaches when the embedded extra "
        "is absent. Move: install provisa[embedded]; its duckdb-extension-* wheels are on PyPI.",
    ),
    "community-extensions.duckdb.org": (
        "provisa/federation/duckdb_extensions.py",
        'return [f"INSTALL {extension} FROM community" if from_community else f"INSTALL {extension}"]',
        "DuckDB's community extension repository, which INSTALL ... FROM community reaches when "
        "the embedded extra is absent. Move: install provisa[embedded]; provisa-duckdb-ext carries "
        "them, and goes to PyPI once access returns.",
    ),
    "cdn.jsdelivr.net": (
        "provisa/api/app.py",
        'docs_url="/data/openapi/docs",',
        "FastAPI's own Swagger UI (/data/openapi/docs) and ReDoc (/data/openapi/redoc) pages load "
        "their scripts from jsDelivr by default. Move (follow-up): serve both from npm packages in "
        "the UI build, Swagger UI from the swagger-ui-dist copy the REST docs page already uses and "
        "ReDoc from the redoc package.",
    ),
    "fastapi.tiangolo.com": (
        "provisa/api/app.py",
        'redoc_url="/data/openapi/redoc",',
        "the favicon FastAPI's Swagger UI and ReDoc pages load by default. Move (follow-up): with "
        "cdn.jsdelivr.net, point the pages at the UI's own favicon.",
    ),
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


def _found_hosts() -> dict[str, set[str]]:
    """Each http(s) URL host on the pip path -> the files naming it, scanned as ``_found`` scans."""
    found: dict[str, set[str]] = {}

    def add(text: str, rel: str) -> None:
        for match in _URL_HOST.finditer(text):
            found.setdefault(match.group(1).lower().rstrip("."), set()).add(rel)

    for root in _PY_ROOTS:
        for path in sorted((REPO / root).rglob("*.py")):
            if "http" not in path.read_text(encoding="utf-8"):
                continue
            rel = path.relative_to(REPO).as_posix()
            for literal in _python_literals(path):
                add(literal, rel)
    for path in _text_files():
        add(path.read_text(encoding="utf-8", errors="replace"), path.relative_to(REPO).as_posix())
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


def test_every_host_on_the_pip_path_is_a_registry_or_listed() -> None:
    known = REGISTRY_HOSTS | HOSTS.keys()
    new = {h: sorted(files) for h, files in _found_hosts().items() if h not in known}
    assert not new, (
        "code on the pip path names a host outside PyPI, Maven Central and npm "
        "(docs/arch/fetch-sources.md). Give it a registry home, or add it to HOSTS with its move: "
        f"{new}"
    )


def test_every_listed_host_is_still_named() -> None:
    stale = sorted((REGISTRY_HOSTS | HOSTS.keys()) - _found_hosts().keys())
    assert not stale, (
        f"remove these hosts from REGISTRY_HOSTS or HOSTS, no code names them: {stale}"
    )


def test_registry_listed_and_implicit_hosts_are_disjoint() -> None:
    assert not REGISTRY_HOSTS & HOSTS.keys()
    assert not IMPLICIT.keys() & (REGISTRY_HOSTS | HOSTS.keys())


def test_every_implicit_host_still_has_the_code_that_reaches_it() -> None:
    stale = sorted(
        host
        for host, (file, text, _move) in IMPLICIT.items()
        if text not in (REPO / file).read_text(encoding="utf-8")
    )
    assert not stale, f"the code leading to these IMPLICIT hosts is gone; remove them: {stale}"


def test_every_listed_host_names_its_move() -> None:
    reasons = {**HOSTS, **{h: move for h, (_f, _t, move) in IMPLICIT.items()}}
    missing = sorted(h for h, reason in reasons.items() if "move" not in reason.lower())
    assert not missing, f"these hosts name no move (or 'No move.' with the reason): {missing}"


def test_url_host_pattern_reads_ascii_hosts() -> None:
    for text, want in (
        ("https://api.example.com（任意の上書き）", "api.example.com"),
        ("sparse+https://index.crates.io/", "index.crates.io"),
        ("https://{store}.myshopify.com/admin", "{store}.myshopify.com"),
        ("http://localhost:8001/x", "localhost"),
    ):
        match = _URL_HOST.search(text)
        assert match is not None and match.group(1) == want, (text, match)
    assert _URL_HOST.search("ein https://-Adresse") is None
