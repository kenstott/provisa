# Copyright (c) 2026 Kenneth Stott
# Canary: 9d41c6b2-7e3a-4f15-8b0c-2a6e5f9d3c71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A built-in code source (REQ-2267, amended 2026-11-24): an OpenAPI or remote GraphQL source
whose code is Provisa's own.

Saving one writes the built-in's code into a server-owned code directory for the source -- under
its org's folder, ``code/builtin/<source id>`` -- records the code as the ``builtin`` kind
(``mapping["code"] = {"kind": "builtin"}``) with its content hash, and declares the built-in's
credential as the source's secret reference (``mapping["secrets"]``,
provisa/codesource/credentials.py). A credential given as a value is refused by name. The
code-source runtime then serves the source: :func:`prepare` reads the code with Provisa's Python
reader and runs it in Provisa's Python host, as every Python code source runs.

What the code is written from is the source as saved: an OpenAPI source's spec is the one its
``path`` names (read as ``load_source_spec`` reads it, with its mapping's exclusions), its address
the mapping's ``base_url`` or the spec's first server, its auth and headers the mapping's; a
remote GraphQL source's endpoint is its ``path``, its schema the one the endpoint answers
introspection with, its auth, namespace and field overrides the mapping's, and its mapping limits
the deployment's (``graphql_remote`` settings).
"""

# Requirements: REQ-2267
from __future__ import annotations

import importlib
import shutil
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.languages import BUILTIN_KIND, BUILTIN_TYPES

if TYPE_CHECKING:
    from provisa.codesource.manifest import Manifest
    from provisa.codesource.source import CodeConfig, Context, Prepared
    from provisa.codesource.worker import HostCommand

#: The environment the reader and the host get beside PATH (as provisa/python/source.py's).
_PYTHON_ENV = {"PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1"}


def is_builtin(source_type: str, mapping: dict | None) -> bool:
    """Whether a source of ``source_type`` with ``mapping`` is a built-in code source."""
    return (
        source_type in BUILTIN_TYPES
        and ((mapping or {}).get("code") or {}).get("kind") == BUILTIN_KIND
    )


def module_of(source_type: str) -> Any:  # the built-in's module
    """The module of the built-in ``source_type``: its ``ENTRY``, ``write_code`` and
    ``declared_secrets``."""
    return importlib.import_module(BUILTIN_TYPES[source_type])


def code_directory(org_id: str, source_id: str) -> Path:
    """The server-owned directory the source's code is written to: under its org's folder."""
    from provisa.core.org_files import org_path

    return org_path("code", BUILTIN_KIND, source_id, org_id=org_id)


def read_code(directory: Path, entry: str) -> Manifest:
    """The code's tables, queries and commands, read by Provisa's Python reader under Provisa's
    own Python (the code imports Provisa); refused with every problem by name."""
    from provisa.codesource.manifest import PYTHON_RULES, read_manifest
    from provisa.codesource.worker import private_environment
    from provisa.python.source import READER, run_reader

    argv = [sys.executable, "-I", str(READER), str(directory), entry]
    return read_manifest(run_reader(argv, private_environment(**_PYTHON_ENV)), PYTHON_RULES)


def host_command(directory: Path, entry: str, manifest: Manifest) -> HostCommand:
    """How Provisa's own Python runs the host on the code."""
    from provisa.codesource.manifest import discovered_handlers
    from provisa.codesource.worker import HostCommand, private_environment
    from provisa.python.source import HOST

    return HostCommand(
        argv=lambda address: [sys.executable, "-I", str(HOST), address, str(directory), entry],
        env=private_environment(**_PYTHON_ENV),
        cwd=directory,
        discovered=discovered_handlers(manifest),
    )


def prepare(source_id: str, config: CodeConfig, ctx: Context) -> Prepared:
    """The source made ready from the code its save wrote: read, and how its worker starts. Code
    whose content is not the hash its save recorded is refused by both hashes."""
    from provisa.codesource.bundle import content_hash
    from provisa.codesource.source import Prepared, granted

    directory = code_directory(ctx.org_id, source_id)
    found = content_hash(directory)
    if config.content_hash is not None and found != config.content_hash:
        raise BundleRefused(
            "content_changed",
            f"source {source_id}: its built-in code is {found}, not the {config.content_hash} "
            "its save recorded; save the source again to write it afresh",
            source=source_id,
            expected=config.content_hash,
            found=found,
        )
    entry = module_of(config.language).ENTRY
    manifest = read_code(directory, entry)
    command = granted(host_command(directory, entry, manifest), source_id, config, ctx)
    return Prepared(source_id, found, manifest, command, ctx.org_id, workers=config.workers)


# -- the save ---------------------------------------------------------------------------------------


def _refused(refused: Exception, source_id: str) -> BundleRefused:
    if isinstance(refused, BundleRefused):
        return refused
    return BundleRefused(
        "builtin_unwritable",
        f"source {source_id}: its built-in code could not be written: {refused}",
        source=source_id,
        reason=str(refused),
    )


def _openapi_code(directory: Path, source_id: str, path: str, mapping: dict) -> str:
    from provisa.api_source.openapi_endpoint import api_auth
    from provisa.core.secrets import resolve_secrets
    from provisa.openapi.code_source import write_code
    from provisa.openapi.exclusions import load_source_spec
    from provisa.openapi.loader import spec_server_url

    auth = mapping.get("auth")
    module_of("openapi").declared_secrets(source_id, auth)  # a literal refused before any read
    spec_path = resolve_secrets(path)
    spec = load_source_spec(spec_path, mapping, source_id=source_id, auth=api_auth(auth))
    base_url = (mapping.get("base_url") or "").strip() or spec_server_url(spec, spec_path)
    return write_code(
        directory,
        source_id=source_id,
        spec=spec,
        base_url=base_url,
        auth=auth,
        headers=mapping.get("headers") or {},
    )


async def _graphql_config(path: str, mapping: dict, limits: Any) -> dict:
    from provisa.core.secrets_store import resolved_credential
    from provisa.graphql_remote.discovered import AUTH_CREDENTIAL
    from provisa.graphql_remote.introspect import introspect_schema

    auth = mapping.get("auth")
    url = await resolved_credential(path)
    config = {
        "url": url,
        "auth": auth,
        "namespace": mapping.get("namespace", ""),
        "schema": None,
        "max_object_depth": limits.max_object_depth,
        "max_list_depth": limits.max_list_depth,
        "max_list_items": limits.max_list_items,
        "max_rows": limits.max_rows,
        # A loader batch is one page of the remote's list: the most items one request answers.
        "loader_batch": limits.max_list_items,
        "field_overrides": mapping.get("field_overrides"),
    }
    module_of("graphql_remote").declared_secrets(config)  # a literal refused before any call
    # Introspection is called with the credential the reference names, read now.
    called = auth
    field = None if not auth else AUTH_CREDENTIAL.get(auth.get("type", "none"))
    if auth and field is not None:
        called = {**auth, field: await resolved_credential(auth[field])}
    config["schema"] = await introspect_schema(url, called)
    return config


async def write(
    source_id: str, source_type: str, mapping: dict, path: str | None, ctx: Context, limits: Any
) -> None:
    """Write the built-in's code for the source being saved and record it in ``mapping`` (in
    place): its ``code`` the built-in kind, its ``secrets`` the built-in's declared credential and
    its ``content_hash`` the written code's. ``limits``: the deployment's ``graphql_remote``
    settings. Refused by name (:class:`BundleRefused`) when the credential is a value, the source
    names no ``path``, or the spec or schema cannot be read."""
    from provisa.codesource.bundle import content_hash

    if not path:
        raise BundleRefused(
            "builtin_path_missing",
            f"source {source_id}: a {source_type} source names where its "
            + ("spec is" if source_type == "openapi" else "endpoint is"),
            source=source_id,
        )
    directory = code_directory(ctx.org_id, source_id)
    shutil.rmtree(directory)  # the code as this save writes it, nothing an earlier save left
    try:
        if source_type == "openapi":
            secrets = module_of(source_type).declared_secrets(source_id, mapping.get("auth"))
            _openapi_code(directory, source_id, path, mapping)
        else:
            config = await _graphql_config(path, mapping, limits)
            secrets = module_of(source_type).declared_secrets(config)
            module_of(source_type).write_code(directory, config)
    except Exception as refused:  # noqa: BLE001 -- every cause is refused by name below
        raise _refused(refused, source_id) from refused
    mapping["code"] = {"kind": BUILTIN_KIND}
    mapping["secrets"] = secrets
    mapping["content_hash"] = content_hash(directory)
