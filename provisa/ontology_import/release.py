# Copyright (c) 2026 Kenneth Stott
# Canary: 3a9e15c7-60d2-4b8f-91e4-c7f2a08d5b36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A whole ontology release, every module file of it in one archive (REQ-2237).

A release such as FIBO is many module files that import one another with ``owl:imports``. Its
archive (a ``.zip``) is read whole: every member in an RDF format is one module, and the modules
are read into one graph. The release's own imports are resolved locally, against the modules of
the archive -- by the ontology IRI or version IRI each module declares, and by the archive's XML
catalog (``catalog-v001.xml``, as Protégé and the FIBO releases write it) when it has one. Nothing
is ever fetched: an import of a module that is not in the archive is counted as left out, never
loaded. A member in no RDF format (a README, a licence, the catalog itself) is no module.
"""

# Requirements: REQ-2237

from __future__ import annotations

import io
import posixpath
import zipfile
from dataclasses import dataclass, field
from typing import Any

#: The most an archive may hold once unpacked, and the most members it may have.
MAX_UNPACKED_BYTES = 512 * 1024 * 1024
MAX_MEMBERS = 10_000

_CATALOG = "catalog-v001.xml"
_CATALOG_NS = "{urn:oasis:names:tc:entity:xmlns:xml:catalog}"


def is_archive(file_name: str) -> bool:
    return file_name.lower().endswith(".zip")


@dataclass
class Release:
    """The archive's modules (member path, format) read into one graph, and how each of the
    release's own imports was resolved."""

    graph: Any
    modules: list[str]
    #: Each ``owl:imports`` the modules state, by the imported IRI: the member it resolved to, or
    #: None when the archive holds no module of that IRI.
    imports: dict[str, str | None] = field(default_factory=dict)
    #: Archive members that are no module, by why.
    skipped: dict[str, int] = field(default_factory=dict)


def _members(file_name: str, content: bytes, refused: type[ValueError]) -> dict[str, bytes]:
    try:
        archive = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile:
        raise refused(f"{file_name!r} is not a zip archive") from None
    infos = [i for i in archive.infolist() if not i.is_dir()]
    if len(infos) > MAX_MEMBERS:
        raise refused(f"{file_name!r} holds more than {MAX_MEMBERS} files")
    if sum(i.file_size for i in infos) > MAX_UNPACKED_BYTES:
        raise refused(
            f"{file_name!r} unpacks to more than {MAX_UNPACKED_BYTES // (1024 * 1024)} MB"
        )
    out: dict[str, bytes] = {}
    for info in infos:
        name = posixpath.normpath(info.filename)
        if name.startswith("../") or posixpath.isabs(name):
            raise refused(f"{file_name!r} names a file outside itself: {info.filename!r}")
        out[name] = archive.read(info)
    return out


def _catalog(members: dict[str, bytes], refused: type[ValueError]) -> dict[str, str]:
    """Each IRI the archive's catalogs map to a member of it."""
    mapped: dict[str, str] = {}
    for path, data in members.items():
        if posixpath.basename(path) != _CATALOG:
            continue
        # defusedxml refuses entities and DTD tricks: a catalog from an archive is untrusted.
        from defusedxml import DefusedXmlException
        from defusedxml.ElementTree import ParseError, fromstring

        try:
            root = fromstring(data)
        except (ParseError, DefusedXmlException) as exc:
            raise refused(f"{path!r} is not an XML catalog: {exc}") from None
        for entry in root.iter(f"{_CATALOG_NS}uri"):
            name, uri = entry.get("name"), entry.get("uri")
            if name and uri and "://" not in uri:
                mapped[name] = posixpath.normpath(posixpath.join(posixpath.dirname(path), uri))
    return mapped


def _key(iri: str) -> str:
    return iri.rstrip("/#")


def read(
    file_name: str,
    content: bytes,
    graph_of: Any,
    format_of: Any,
    refused: type[ValueError],
) -> Release:
    """The release ``content`` archives. ``graph_of(text, fmt, member)`` reads one module and
    ``format_of(member)`` names a member's RDF format, or None for a member that is no module."""
    from rdflib import OWL, RDF, Graph

    members = _members(file_name, content, refused)
    graph = Graph()
    modules: list[str] = []
    skipped: dict[str, int] = {}
    declared: dict[str, str] = {}
    stated: list[str] = []
    for path in sorted(members):
        fmt = format_of(path)
        if fmt is None:
            why = "catalog" if posixpath.basename(path) == _CATALOG else "file that is no module"
            skipped[why] = skipped.get(why, 0) + 1
            continue
        try:
            text = members[path].decode("utf-8-sig")
        except UnicodeDecodeError:
            raise refused(f"{file_name!r}: {path!r} is not UTF-8 text") from None
        module = graph_of(text, fmt, f"{file_name}:{path}")
        modules.append(path)
        for ontology in module.subjects(RDF.type, OWL.Ontology):
            declared.setdefault(_key(str(ontology)), path)
            for version in module.objects(ontology, OWL.versionIRI):
                declared.setdefault(_key(str(version)), path)
        stated.extend(str(o) for o in module.objects(None, OWL.imports))
        graph += module
    if not modules:
        raise refused(f"{file_name!r} holds no ontology module")
    catalog = {
        _key(iri): path for iri, path in _catalog(members, refused).items() if path in modules
    }
    imports: dict[str, str | None] = {}
    for iri in stated:
        imports[iri] = declared.get(_key(iri)) or catalog.get(_key(iri))
    return Release(graph, modules, imports, skipped)
