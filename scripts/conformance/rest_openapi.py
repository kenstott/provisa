# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4e2c71-5b8d-4f3a-a6e9-1c7b3d5f2e84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's REST surface (/data/rest) against OpenAPI 3.1 and HTTP semantics (REQ-2167).

No public conformance suite for a REST server built from its own OpenAPI document exists, so the
scenario set is derived from the specifications, each case named for its section:

- the document Provisa serves at /data/rest/openapi.json, validated by openapi-spec-validator (the
  OpenAPI Initiative's 3.1 schema and its semantic rules; installed at a pinned version, never
  vendored), and the OpenAPI 3.1 rules a schema validation does not reach;
- request/response conformance: every GET operation the document declares with no required
  parameter is called, and its status, Content-Type and body are checked against what the
  operation declares (the body by JSON Schema 2020-12, the dialect OpenAPI 3.1 schemas use);
- the HTTP semantics (RFC 9110) the REST surface answers by.

Every case is passed, skipped by design or failed (REQ-2167, amended 2026-10-28); the run is
judged by baseline_gate.py against scripts/conformance/expected/rest.json.

Usage: rest_openapi.py <base url> <role> <authorization> <validator version> <out.json> <out.md>
"""

# Requirements: REQ-2167

from __future__ import annotations

import re
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from surface_cases import Case, expect, report, run, write

SUITE = "OpenAPI 3.1 and RFC 9110 scenarios, document validated by openapi-spec-validator"
PREFIX = "/data/rest"
DOCUMENT = f"{PREFIX}/openapi.json"
METHODS = ("get", "put", "post", "delete", "options", "head", "patch", "trace")
#: OpenAPI 3.1 §4.8.7.1 (Components Object): the keys of a component map.
COMPONENT_KEY = re.compile(r"^[a-zA-Z0-9.\-_]+$")
DOC_URI = "urn:provisa:rest-openapi"

Validate = Callable[[dict], None]


def _pointer(*parts: str) -> str:
    return "".join("/" + p.replace("~", "~0").replace("/", "~1") for p in parts)


def operations(doc: dict) -> Iterator[tuple[str, str, dict]]:
    """Every (path, method, operation) the document declares."""
    for path, item in (doc.get("paths") or {}).items():
        for method in METHODS:
            if method in item:
                yield path, method, item[method]


def _parameters(doc: dict, path: str, op: dict) -> list[dict]:
    """The operation's parameters with its path item's, references resolved."""
    params = [*(doc["paths"][path].get("parameters") or []), *(op.get("parameters") or [])]
    return [_resolve(doc, p) for p in params]


def _resolve(doc: dict, node: dict) -> dict:
    while isinstance(node, dict) and "$ref" in node:
        ref = node["$ref"]
        expect(ref.startswith("#/"), f"{ref} is not a reference within the document")
        node = doc
        for part in ref[2:].split("/"):
            node = node[part.replace("~1", "/").replace("~0", "~")]
    return node


def callable_reads(doc: dict) -> list[tuple[str, dict]]:
    """The GET operations a request can call as declared: no path template, no required
    parameter."""
    out = []
    for path, method, op in operations(doc):
        if method != "get" or "{" in path:
            continue
        if any(p.get("required") for p in _parameters(doc, path, op)):
            continue
        out.append((path, op))
    return out


class Rest:
    """The REST surface of one instance, as the job user acting as ``role``."""

    def __init__(self, base: str, role: str, authorization: str, validate: Validate) -> None:
        self.base = base.rstrip("/")
        self.headers = {"authorization": authorization, "x-provisa-role": role}
        self.validate_document = validate
        self.client = httpx.Client(timeout=120)
        self._doc: dict | None = None

    def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        headers = {**self.headers, **kwargs.pop("headers", {})}
        return self.client.request(method, self.base + path, headers=headers, **kwargs)

    def document(self) -> dict:
        if self._doc is None:
            got = self.request("GET", DOCUMENT)
            expect(got.status_code == 200, f"GET {DOCUMENT} answered {got.status_code}")
            self._doc = got.json()
        assert self._doc is not None
        return self._doc

    def reads(self) -> list[tuple[str, dict]]:
        found = callable_reads(self.document())
        expect(bool(found), "the document declares no GET operation callable without parameters")
        return found

    def url(self, path: str) -> str:
        """A document path under its server URL (the document's servers[0].url)."""
        servers = self.document().get("servers") or [{"url": "/"}]
        return servers[0]["url"].rstrip("/") + path

    # --- the document ----------------------------------------------------------------------------

    def served_as_json(self) -> None:
        got = self.request("GET", DOCUMENT)
        expect(got.status_code == 200, f"answered {got.status_code}")
        kind = got.headers.get("content-type", "")
        expect(kind.split(";")[0].strip() == "application/json", f"Content-Type {kind!r}")
        expect(isinstance(got.json(), dict), "the document is not a JSON object")

    def version_is_31(self) -> None:
        version = str(self.document().get("openapi", ""))
        expect(bool(re.fullmatch(r"3\.1\.\d+", version)), f"openapi is {version!r}, not 3.1.x")

    def validates(self) -> None:
        self.validate_document(self.document())

    def refs_resolve(self) -> None:
        doc = self.document()
        broken = []

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                ref = node.get("$ref")
                if isinstance(ref, str):
                    try:
                        _resolve(doc, {"$ref": ref})
                    except (KeyError, TypeError, AssertionError):
                        broken.append(ref)
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)

        walk(doc)
        expect(not broken, f"unresolved references: {sorted(set(broken))[:10]}")

    def operation_ids_unique(self) -> None:
        seen: dict[str, str] = {}
        twice = []
        for path, method, op in operations(self.document()):
            oid = op.get("operationId")
            if oid is None:
                continue
            if oid in seen:
                twice.append(f"{oid} ({seen[oid]} and {method.upper()} {path})")
            seen[oid] = f"{method.upper()} {path}"
        expect(not twice, f"operationId used twice: {twice[:10]}")

    def path_parameters_declared(self) -> None:
        doc = self.document()
        wrong = []
        for path, method, op in operations(doc):
            declared = {p["name"]: p for p in _parameters(doc, path, op) if p.get("in") == "path"}
            for name in re.findall(r"\{([^}]+)\}", path):
                if name not in declared or declared[name].get("required") is not True:
                    wrong.append(f"{method.upper()} {path} {{{name}}}")
            for name in set(declared) - set(re.findall(r"\{([^}]+)\}", path)):
                wrong.append(f"{method.upper()} {path} declares path parameter {name} it lacks")
        expect(not wrong, f"path templates without a required path parameter: {wrong[:10]}")

    def responses_declared(self) -> None:
        empty = [
            f"{method.upper()} {path}"
            for path, method, op in operations(self.document())
            if not op.get("responses")
        ]
        expect(not empty, f"operations declaring no response: {empty[:10]}")

    def component_keys(self) -> None:
        bad = [
            f"{kind}.{key}"
            for kind, entries in (self.document().get("components") or {}).items()
            if isinstance(entries, dict)
            for key in entries
            if not COMPONENT_KEY.match(key)
        ]
        expect(not bad, f"component keys outside ^[a-zA-Z0-9.\\-_]+$: {bad[:10]}")

    # --- request and response ----------------------------------------------------------------------

    def _declared(self, op: dict, status: int) -> dict | None:
        responses = op.get("responses") or {}
        for key in (str(status), f"{str(status)[0]}XX", "default"):
            if key in responses:
                return _resolve(self.document(), responses[key])
        return None

    def reads_answer_declared_status(self) -> None:
        wrong = []
        for path, op in self.reads():
            got = self.request("GET", self.url(path))
            if not (200 <= got.status_code < 300) or self._declared(op, got.status_code) is None:
                wrong.append(f"GET {path}: {got.status_code} {got.text[:200]}")
        expect(not wrong, f"not a declared 2xx: {wrong[:5]}")

    def reads_answer_declared_content_type(self) -> None:
        wrong = []
        for path, op in self.reads():
            got = self.request("GET", self.url(path))
            declared = self._declared(op, got.status_code) or {}
            kinds = set((declared.get("content") or {}).keys())
            kind = got.headers.get("content-type", "").split(";")[0].strip()
            if kind not in kinds:
                wrong.append(f"GET {path}: {kind!r} not in {sorted(kinds)}")
        expect(not wrong, f"undeclared Content-Type: {wrong[:5]}")

    def reads_answer_declared_schema(self) -> None:
        doc = self.document()
        registry = Registry().with_resource(
            DOC_URI, Resource.from_contents(doc, default_specification=DRAFT202012)
        )
        wrong = []
        for path, op in self.reads():
            got = self.request("GET", self.url(path))
            status = str(got.status_code)
            responses = op.get("responses") or {}
            key = next((k for k in (status, f"{status[0]}XX", "default") if k in responses), None)
            if key is None:
                wrong.append(f"GET {path}: {status} undeclared")
                continue
            kind = got.headers.get("content-type", "").split(";")[0].strip()
            response = _resolve(doc, responses[key])
            if "schema" not in (response.get("content") or {}).get(kind, {}):
                wrong.append(f"GET {path}: no schema declared for {status} {kind}")
                continue
            if "$ref" in responses[key]:
                where = responses[key]["$ref"][1:] + _pointer("content", kind, "schema")
            else:
                where = _pointer("paths", path, "get", "responses", key, "content", kind, "schema")
            validator = Draft202012Validator({"$ref": f"{DOC_URI}#{where}"}, registry=registry)
            errors = sorted(validator.iter_errors(got.json()), key=lambda e: list(e.path))
            if errors:
                first = errors[0]
                at = "/".join(str(p) for p in first.path)
                wrong.append(f"GET {path}: at /{at}: {first.message[:200]}")
        expect(not wrong, f"bodies not valid against their declared schema: {wrong[:5]}")

    def invalid_parameter_refused(self) -> None:
        doc = self.document()
        for path, op in self.reads():
            for p in _parameters(doc, path, op):
                schema = _resolve(doc, p.get("schema") or {})
                if p.get("in") == "query" and schema.get("type") == "integer":
                    got = self.request("GET", self.url(path), params={p["name"]: "not-an-integer"})
                    expect(
                        400 <= got.status_code < 500,
                        f"GET {path}?{p['name']}=not-an-integer answered {got.status_code}",
                    )
                    return
        raise AssertionError("no read declares an integer query parameter to send a string to")

    # --- HTTP semantics (RFC 9110) -----------------------------------------------------------------

    def unknown_path_404(self) -> None:
        got = self.request("GET", f"{PREFIX}/no_such_domain_x/no_such_table_x")
        expect(got.status_code == 404, f"answered {got.status_code}")

    def unauthenticated_401_with_challenge(self) -> None:
        path, _ = self.reads()[0]
        got = self.client.get(self.base + self.url(path))
        expect(got.status_code == 401, f"answered {got.status_code}")
        expect("www-authenticate" in got.headers, "a 401 without a WWW-Authenticate header")

    def undeclared_method_405_with_allow(self) -> None:
        doc = self.document()
        path, _ = self.reads()[0]
        expect("delete" not in doc["paths"][path], f"{path} declares DELETE")
        got = self.request("DELETE", self.url(path))
        expect(got.status_code == 405, f"DELETE {path} answered {got.status_code}")
        expect("allow" in got.headers, "a 405 without an Allow header")

    def head_like_get(self) -> None:
        path, _ = self.reads()[0]
        got = self.request("HEAD", self.url(path))
        expect(got.status_code == 200, f"HEAD {path} answered {got.status_code}")
        expect(not got.content, "HEAD answered with a body")


def cases(rest: Rest) -> list[Case]:
    doc, rr, http = "OpenAPI 3.1 document", "OpenAPI 3.1 request and response", "RFC 9110"
    return [
        Case(doc, "served as a JSON object at /data/rest/openapi.json", rest.served_as_json),
        Case(doc, "§4.8.1 openapi names a 3.1 version", rest.version_is_31),
        Case(doc, "valid by openapi-spec-validator (3.1 schema and rules)", rest.validates),
        Case(doc, "every $ref resolves within the document", rest.refs_resolve),
        Case(
            doc, "§4.8.10.1 operationId is unique among all operations", rest.operation_ids_unique
        ),
        Case(
            doc,
            "§4.8.12.1 every path template names a required path parameter",
            rest.path_parameters_declared,
        ),
        Case(doc, "§4.8.16 every operation declares a response", rest.responses_declared),
        Case(doc, "§4.8.7.1 component keys match ^[a-zA-Z0-9.-_]+$", rest.component_keys),
        Case(rr, "a callable read answers a 2xx it declares", rest.reads_answer_declared_status),
        Case(
            rr,
            "a callable read answers a Content-Type it declares",
            rest.reads_answer_declared_content_type,
        ),
        Case(
            rr,
            "a callable read's body is valid against its declared schema",
            rest.reads_answer_declared_schema,
        ),
        Case(
            rr,
            "a value outside a declared parameter's schema is answered 4xx",
            rest.invalid_parameter_refused,
        ),
        Case(http, "§15.5.5 an unknown resource is answered 404", rest.unknown_path_404),
        Case(
            http,
            "§15.5.2 a 401 carries a WWW-Authenticate challenge",
            rest.unauthenticated_401_with_challenge,
        ),
        Case(
            http,
            "§15.5.6 a method the resource lacks is answered 405 with Allow",
            rest.undeclared_method_405_with_allow,
        ),
        Case(http, "§9.3.2 HEAD is answered as GET, without a body", rest.head_like_get),
    ]


def spec_validator() -> Validate:
    """openapi-spec-validator's check of a 3.1 document (the workflow installs it, pinned)."""
    # Installed by the workflow at its pinned version, never a project dependency (the suite is
    # fetched as the GraphQL job fetches graphql-http).
    from openapi_spec_validator import validate  # pyright: ignore[reportMissingImports]
    from openapi_spec_validator.validation.validators import (  # pyright: ignore[reportMissingImports]
        OpenAPIV31SpecValidator,
    )

    def check(doc: dict) -> None:
        validate(doc, cls=OpenAPIV31SpecValidator)

    return check


def main(argv: list[str]) -> int:
    base, role, authorization, version, out_json, out_md = argv
    rest = Rest(base, role, authorization, spec_validator())
    doc = report("rest", SUITE, f"(openapi-spec-validator {version})", run(cases(rest)))
    return write(doc, "REST / OpenAPI", Path(out_json), Path(out_md))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
