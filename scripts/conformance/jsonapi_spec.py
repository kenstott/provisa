# Copyright (c) 2026 Kenneth Stott
# Canary: 5d7b3e19-2c4a-4e8f-b1d6-8a9c0f3e7b52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's JSON:API surface (/data/jsonapi) against JSON:API 1.1 (REQ-2167).

JSON:API publishes no server conformance suite, so the scenario set is derived from the 1.1
specification (https://jsonapi.org/format/1.1/), one case per normative statement a read-only
client can observe, named for its section: content negotiation, document structure, fetching
data, query parameters, and errors. The resource type read is the first collection the surface's
own OpenAPI document (/data/jsonapi/openapi.json) declares.

Every case is passed, skipped by design or failed (REQ-2167, amended 2026-10-28); the run is
judged by baseline_gate.py against scripts/conformance/expected/jsonapi.json.

Usage: jsonapi_spec.py <base url> <role> <authorization> <out.json> <out.md>
"""

# Requirements: REQ-2167

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import httpx

from surface_cases import Case, expect, report, run, write

SUITE = "JSON:API 1.1 specification scenarios"
VERSION = "1.1"
MEDIA = "application/vnd.api+json"
PREFIX = "/data/jsonapi"
TOP_LEVEL = {"data", "errors", "meta", "jsonapi", "links", "included"}
RESOURCE_MEMBERS = {"id", "type", "attributes", "relationships", "links", "meta"}
ERROR_MEMBERS = {"id", "links", "status", "code", "title", "detail", "source", "meta"}
#: §Member Names: globally allowed characters at the start and end; - _ and space also inside.
_GLOBAL = r"a-zA-Z0-9\u0080-\U0010FFFF"
MEMBER_NAME = re.compile(rf"^[{_GLOBAL}](?:[{_GLOBAL}\-_ ]*[{_GLOBAL}])?$")


class JsonApi:
    """The JSON:API surface of one instance, as the job user acting as ``role``."""

    def __init__(self, base: str, role: str, authorization: str) -> None:
        self.base = base.rstrip("/")
        self.headers = {"authorization": authorization, "x-provisa-role": role}
        self.client = httpx.Client(timeout=120)
        self._collection: str | None = None

    def get(self, path: str, accept: str | None = MEDIA, **kwargs: Any) -> httpx.Response:
        headers = {**self.headers, **kwargs.pop("headers", {})}
        if accept is not None:
            headers["accept"] = accept
        return self.client.get(self.base + path, headers=headers, **kwargs)

    def collection(self) -> str:
        """The path of the first collection the surface's OpenAPI document declares."""
        if self._collection is None:
            got = self.get(f"{PREFIX}/openapi.json", accept="application/json")
            expect(got.status_code == 200, f"GET {PREFIX}/openapi.json answered {got.status_code}")
            paths = [
                p
                for p, item in (got.json().get("paths") or {}).items()
                if "get" in item and "{" not in p and "/commands/" not in p
            ]
            expect(bool(paths), "the JSON:API document declares no collection")
            servers = got.json().get("servers") or [{"url": PREFIX}]
            self._collection = servers[0]["url"].rstrip("/") + sorted(paths)[0]
        assert self._collection is not None
        return self._collection

    def read(self, **params: str) -> dict:
        got = self.get(self.collection(), params=params)
        expect(got.status_code == 200, f"GET {self.collection()} answered {got.status_code}")
        return got.json()

    def resources(self) -> list[dict]:
        data = self.read().get("data")
        expect(isinstance(data, list) and bool(data), "the collection read holds no resource")
        assert isinstance(data, list)
        return data

    # --- content negotiation -------------------------------------------------------------------

    def responds_media_type(self) -> None:
        got = self.get(self.collection())
        kind = got.headers.get("content-type", "")
        expect(kind.split(";")[0].strip() == MEDIA, f"Content-Type {kind!r}")
        params = [p.strip().split("=")[0] for p in kind.split(";")[1:] if p.strip()]
        expect(all(p in ("ext", "profile") for p in params), f"media type parameters {params}")

    def plain_accept_served(self) -> None:
        got = self.get(self.collection())
        expect(got.status_code == 200, f"Accept: {MEDIA} answered {got.status_code}")

    def accept_with_other_parameter_406(self) -> None:
        got = self.get(self.collection(), accept=f"{MEDIA}; charset=utf-8")
        expect(got.status_code == 406, f"answered {got.status_code}")

    def accept_with_unknown_ext_406(self) -> None:
        got = self.get(self.collection(), accept=f'{MEDIA}; ext="https://example.invalid/ext"')
        expect(got.status_code == 406, f"answered {got.status_code}")

    def content_type_with_other_parameter_415(self) -> None:
        got = self.get(self.collection(), headers={"content-type": f"{MEDIA}; charset=utf-8"})
        expect(got.status_code == 415, f"answered {got.status_code}")

    # --- document structure ----------------------------------------------------------------------

    def top_level_member(self) -> None:
        doc = self.read()
        expect(isinstance(doc, dict), "the document is not a JSON object")
        expect(bool({"data", "errors", "meta"} & set(doc)), f"top-level members {sorted(doc)}")

    def top_level_members_known(self) -> None:
        doc = self.read()
        unknown = [k for k in doc if k not in TOP_LEVEL and not k.startswith("@")]
        expect(not unknown, f"top-level members the specification does not define: {unknown}")

    def data_and_errors_apart(self) -> None:
        doc = self.read()
        expect(not ("data" in doc and "errors" in doc), "data and errors in one document")

    def collection_is_array(self) -> None:
        expect(isinstance(self.read().get("data"), list), "primary data of a collection")

    def resource_identity(self) -> None:
        bad = [
            r
            for r in self.resources()
            if not isinstance(r.get("type"), str) or not isinstance(r.get("id"), str)
        ]
        expect(not bad, f"resources without a string type and id: {bad[:3]}")

    def resource_members_known(self) -> None:
        bad = sorted({k for r in self.resources() for k in r if k not in RESOURCE_MEMBERS})
        expect(not bad, f"resource object members the specification does not define: {bad}")

    def fields_namespace(self) -> None:
        wrong = []
        for r in self.resources():
            attrs = set((r.get("attributes") or {}).keys())
            rels = set((r.get("relationships") or {}).keys())
            if attrs & rels:
                wrong.append(f"shared by attributes and relationships: {sorted(attrs & rels)}")
            if {"id", "type"} & (attrs | rels):
                wrong.append(f"a field named id or type: {sorted({'id', 'type'} & (attrs | rels))}")
        expect(not wrong, "; ".join(sorted(set(wrong))))

    def member_names(self) -> None:
        bad: set[str] = set()

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                for key, value in node.items():
                    if not key.startswith("@") and not MEMBER_NAME.match(key):
                        bad.add(key)
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)

        doc = self.read()
        walk({k: v for k, v in doc.items() if k in ("data", "included")})
        expect(not bad, f"member names outside the allowed characters: {sorted(bad)[:10]}")

    def jsonapi_object(self) -> None:
        member = self.read().get("jsonapi")
        expect(member is not None, "no jsonapi object stating the version served")
        assert member is not None
        expect(isinstance(member, dict), "jsonapi is not an object")
        expect(member.get("version") == VERSION, f"jsonapi.version {member.get('version')!r}")

    def links_shape(self) -> None:
        links = self.read().get("links")
        expect(isinstance(links, dict), f"top-level links {links!r}")
        assert isinstance(links, dict)
        bad = {
            k: v
            for k, v in links.items()
            if v is not None
            and not isinstance(v, str)
            and not (isinstance(v, dict) and isinstance(v.get("href"), str))
        }
        expect(not bad, f"links neither a string, a link object nor null: {bad}")
        expect("self" in links, "no self link to the document")

    # --- fetching data ---------------------------------------------------------------------------

    def fetch_by_id(self) -> None:
        first = self.resources()[0]
        path = f"{self.collection()}/{first['id']}"
        got = self.get(path)
        expect(got.status_code == 200, f"GET {path} answered {got.status_code}")
        data = got.json().get("data")
        expect(isinstance(data, dict), "a single resource's primary data is not an object")
        expect(
            (data["type"], data["id"]) == (first["type"], first["id"]),
            f"fetched {data.get('type')}/{data.get('id')}, asked {first['type']}/{first['id']}",
        )

    def missing_resource_404(self) -> None:
        path = f"{self.collection()}/no-such-resource-id-0"
        got = self.get(path)
        expect(got.status_code == 404, f"GET {path} answered {got.status_code}")

    def unknown_type_404(self) -> None:
        got = self.get(f"{PREFIX}/no_such_domain_x/no_such_type_x")
        expect(got.status_code == 404, f"answered {got.status_code}")

    # --- query parameters ------------------------------------------------------------------------

    def sparse_fieldset(self) -> None:
        first = self.resources()[0]
        attrs = sorted((first.get("attributes") or {}).keys())
        expect(len(attrs) >= 2, f"{first['type']} has fewer than two attributes to narrow")
        doc = self.read(**{f"fields[{first['type']}]": attrs[0]})
        extra = sorted(
            {k for r in doc["data"] for k in (r.get("attributes") or {}) if k != attrs[0]}
        )
        expect(not extra, f"fields[{first['type']}]={attrs[0]} still returned {extra}")

    def unsupported_sort_400(self) -> None:
        got = self.get(self.collection(), params={"sort": "no_such_field_x"})
        expect(got.status_code == 400, f"sort=no_such_field_x answered {got.status_code}")

    def unknown_include_400(self) -> None:
        got = self.get(self.collection(), params={"include": "no_such_relationship_x"})
        expect(got.status_code == 400, f"include=no_such_relationship_x answered {got.status_code}")

    def reserved_parameter_400(self) -> None:
        got = self.get(self.collection(), params={"unknownparam": "1"})
        expect(got.status_code == 400, f"unknownparam=1 answered {got.status_code}")

    # --- errors ----------------------------------------------------------------------------------

    def _error(self) -> httpx.Response:
        got = self.get(f"{PREFIX}/no_such_domain_x/no_such_type_x")
        expect(got.status_code >= 400, f"an unknown type answered {got.status_code}")
        return got

    def errors_array(self) -> None:
        doc = self._error().json()
        errors = doc.get("errors") if isinstance(doc, dict) else None
        expect(isinstance(errors, list) and bool(errors), f"error document {doc!r}"[:300])

    def error_members_known(self) -> None:
        errors = self._error().json().get("errors") or []
        bad = sorted({k for e in errors for k in e if k not in ERROR_MEMBERS})
        expect(not bad, f"error object members the specification does not define: {bad}")

    def error_status_string(self) -> None:
        got = self._error()
        errors = got.json().get("errors") or []
        expect(bool(errors), "no error object")
        bad = [e.get("status") for e in errors if e.get("status") != str(got.status_code)]
        expect(not bad, f"error status {bad} for an HTTP {got.status_code}")

    def error_media_type(self) -> None:
        kind = self._error().headers.get("content-type", "")
        expect(kind.split(";")[0].strip() == MEDIA, f"Content-Type {kind!r}")


def cases(api: JsonApi) -> list[Case]:
    cn, ds, fd = "Content Negotiation", "Document Structure", "Fetching Data"
    qp, er = "Query Parameters", "Errors"
    return [
        Case(cn, "responses carry Content-Type application/vnd.api+json", api.responds_media_type),
        Case(cn, "Accept: application/vnd.api+json is served", api.plain_accept_served),
        Case(
            cn,
            "every Accept instance modified by a parameter other than ext or profile: 406",
            api.accept_with_other_parameter_406,
        ),
        Case(cn, "Accept naming only an unsupported ext: 406", api.accept_with_unknown_ext_406),
        Case(
            cn,
            "Content-Type with a parameter other than ext or profile: 415",
            api.content_type_with_other_parameter_415,
        ),
        Case(ds, "Top Level: at least one of data, errors, meta", api.top_level_member),
        Case(
            ds,
            "Top Level: no member the specification does not define",
            api.top_level_members_known,
        ),
        Case(ds, "Top Level: data and errors never coexist", api.data_and_errors_apart),
        Case(ds, "Top Level: a collection's primary data is an array", api.collection_is_array),
        Case(ds, "Resource Objects: type and id are strings", api.resource_identity),
        Case(ds, "Resource Objects: only the defined members", api.resource_members_known),
        Case(
            ds,
            "Fields: one namespace, no field named type or id",
            api.fields_namespace,
        ),
        Case(ds, "Member Names: allowed characters only", api.member_names),
        Case(ds, "JSON:API Object: version 1.1 stated", api.jsonapi_object),
        Case(ds, "Links: string, link object or null, with self", api.links_shape),
        Case(fd, "Fetching Resources: a resource fetched by its id", api.fetch_by_id),
        Case(fd, "Fetching Resources: a missing resource is 404", api.missing_resource_404),
        Case(fd, "Fetching Resources: an unknown type is 404", api.unknown_type_404),
        Case(qp, "Sparse Fieldsets: no field beyond those asked", api.sparse_fieldset),
        Case(qp, "Sorting: an unsupported sort is 400", api.unsupported_sort_400),
        Case(qp, "Inclusion: an unknown relationship path is 400", api.unknown_include_400),
        Case(
            qp,
            "Query Parameter Families: an unknown all-lowercase parameter is 400",
            api.reserved_parameter_400,
        ),
        Case(er, "Error Objects: errors is an array of error objects", api.errors_array),
        Case(er, "Error Objects: only the defined members", api.error_members_known),
        Case(er, "Error Objects: status is the HTTP status as a string", api.error_status_string),
        Case(er, "Error responses carry the JSON:API media type", api.error_media_type),
    ]


def main(argv: list[str]) -> int:
    base, role, authorization, out_json, out_md = argv
    doc = report("jsonapi", SUITE, VERSION, run(cases(JsonApi(base, role, authorization))))
    return write(doc, "JSON:API", Path(out_json), Path(out_md))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
