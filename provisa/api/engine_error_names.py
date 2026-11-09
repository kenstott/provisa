# Copyright (c) 2026 Kenneth Stott
# Canary: 409889d1-ec96-4b47-9d50-bcddad1e7a5b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a client is told of an error: redacted, and in the model's names (REQ-2087, REQ-2152).

The engine runs a statement lowered to physical addresses -- a source's catalog, its schema and
its table -- and its error quotes them: DuckDB's Binder Error carries a ``LINE n:`` excerpt of the
physical SQL. A physical address is source configuration, and the caller wrote model names, so
:func:`client_text` -- the one text every transport answers a client's error with (the HTTP error
middleware, pgwire, Flight, MCP, gRPC) -- writes each as the model names it and drops the excerpt.
Where a source lives is source configuration too: httpx's own wording of a failed request, and
any internal address in a message, are written without it (:func:`no_internal_address`), whichever
path brought the text here. An org's own schemas are the platform's, and the engine's "Did you
mean" quotes them, a demo template's among them: a client is told the table, never the schema
(:func:`no_internal_schema`). Logs, traces and the audit record keep the physical names: they are
the operator's.
"""

# Requirements: REQ-2087, REQ-2152

from __future__ import annotations

import ipaddress
import re
from urllib.parse import urlsplit

from provisa.core.environments import SCHEMA_SUFFIXES
from provisa.core.org_ids import SANDBOX_PREFIX

#: An engine's excerpt of the statement it ran: ``LINE n: ...`` and the caret line under it, one
#: caret (DuckDB 1.x) or one for each character it marks (2.0).
_EXCERPT = re.compile(r"\n+LINE \d+:[^\n]*(?:\n[ \t]*\^+)?")
#: The same inside a JSON string: newlines escaped, and the string ending at an unescaped quote.
_EXCERPT_JSON = re.compile(r"(?:\\n)+LINE \d+:.*?(?:\\n[ \t]*\^+|(?=(?<!\\)\"))")
#: httpx.Response.raise_for_status's message, with the URL it failed on.
_HTTPX_STATUS = re.compile(
    r"(?:Informational response|Redirect response|Client error|Server error|Invalid status code)"
    r" '(\d{3}) ?([^']*)' for url '[^']*'"
    r"(?:(?:\n|\\n)Redirect location: '[^']*')?"
    r"(?:(?:\n|\\n)For more information check: [^\s\"']+)?"
)
_URL = re.compile(r"https?://[^\s'\"<>()\[\]]+", re.IGNORECASE)
_INTERNAL_ADDRESS = "[internal address]"
_INTERNAL_DOMAINS = (".internal", ".local", ".localdomain")
_INTERNAL_SCHEMA = "[internal schema]"
#: What environments.org_schema adds to an org's schema, and naming.py to its catalogs.
_SCHEMA_TAIL = (
    r"(?:_(?:env|rg)_\w+|__\w+|" + "|".join(re.escape(s) for s in SCHEMA_SUFFIXES if s) + r")?"
)


def client_text(text: str, org_id: str | None = None) -> str:
    """``text`` as a client is told it: in the model's names (:func:`model_named`), without an
    internal schema (:func:`no_internal_schema`), then redacted. ``org_id`` is the org the request
    acted in; when the transport does not say, the org bound now."""
    from provisa.core.request_context import current_org
    from provisa.security.redaction import redact

    org = org_id if org_id is not None else current_org.get()
    return redact(no_internal_schema(model_named(no_internal_address(text), org), org))


def no_internal_address(text: str) -> str:
    """``text`` without httpx's wording of a failed request and without any URL of a host inside
    the deployment's network. A public URL stays: an error's link to documentation."""
    said = _HTTPX_STATUS.sub(
        lambda m: f"a source answered {m.group(1)} {m.group(2)}".rstrip(), text
    )
    return _URL.sub(lambda m: _INTERNAL_ADDRESS if _is_internal(m.group(0)) else m.group(0), said)


def _is_internal(url: str) -> bool:
    """Whether ``url``'s host is one only the deployment's network resolves: a name with no dot
    (a container's, a service's), one in an internal domain, or a private address."""
    try:
        host = urlsplit(url).hostname
    except ValueError:
        return True  # not parsed: nothing shows it names a public host
    if host is None:
        return False
    try:
        return not ipaddress.ip_address(host).is_global
    except ValueError:
        return "." not in host or host.endswith(_INTERNAL_DOMAINS)


def no_internal_schema(text: str, org_id: str | None) -> str:
    """``text`` without the schemas of the sandbox tier's orgs, whose prefix no other org may take
    (a statement in a sandbox can be told of its template's), nor of ``org_id``, in any of their
    environments, regions and stores: a table's qualifier goes and the table stays. Another word
    beginning ``org_`` is a name in the model and stays."""
    if "org_" not in text:
        return text
    owners = [re.escape(SANDBOX_PREFIX) + "[a-z0-9]*"]
    if org_id is not None:
        owners.append(re.escape(org_id))
    schema = rf"org_(?:{'|'.join(owners)}){_SCHEMA_TAIL}(?!\w)"
    unqualified = re.sub(rf'(\\?"){schema}\1\.|(?<!\w){schema}\.', "", text)
    return re.sub(rf"(?<!\w){schema}", _INTERNAL_SCHEMA, unqualified)


def model_named(text: str, org_id: str | None) -> str:
    """``text`` with each physical address of a table (its source's catalog, schema and table, as
    a statement is lowered to it) written as the model names the table, quoted, unquoted or as a
    JSON body carries it, and with an engine's excerpt of the statement it ran dropped: truncated,
    the excerpt can begin inside a physical address, and its caret counts characters of SQL the
    caller never wrote.

    The addresses are ``org_id``'s tables, the org the request acted in; another org's names are
    never written into it. An error with no org (one raised before a request bound any) has
    nothing rewritten, its excerpt dropped all the same."""
    text = _EXCERPT.sub("", _EXCERPT_JSON.sub("", text))
    if org_id is None:
        return text
    named = _names(org_id)
    if not named:
        return text
    found = re.compile(
        r"(?<![\w.])(?:"
        + "|".join(re.escape(k) for k in sorted(named, key=len, reverse=True))
        + r")(?![\w])"
    )
    return found.sub(lambda m: named[m.group(0)], text)


def _names(org_id: str) -> dict[str, str]:
    """Each physical address of ``org_id``'s tables, in its three spellings, → the model's name
    for it; none for an org whose model is not built."""
    from provisa.api.app import state
    from provisa.compiler.sql_rewrite import semantic_ref

    runtime = state.org_registry.get(org_id)
    # The model-wide context holds every table the org serves: reading each role's would build
    # every role's surface (REQ-1882).
    ctx = None if runtime is None else runtime.view_context
    if ctx is None:
        return {}
    named: dict[str, str] = {}
    for meta in ctx.tables.values():
        model = semantic_ref(meta)  # "domain"."table"
        parts = (meta.catalog_name, meta.schema_name, meta.table_name)
        named[".".join(f'"{p}"' for p in parts)] = model
        # As a JSON error body carries it, its quotes escaped.
        named[".".join(f'\\"{p}\\"' for p in parts)] = model.replace('"', '\\"')
        named[".".join(parts)] = model
    return named
