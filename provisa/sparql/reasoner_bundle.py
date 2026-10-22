# Copyright (c) 2026 Kenneth Stott
# Canary: fc0ca7d3-fbd7-4e8f-830d-7b58812e2837
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The reasoner a sparql source runs (REQ-2138): a separate program, never linked into Provisa.

It is ``reasoner/Reasoner.java``, run by java's source launcher on the pinned Temurin JDK
(``code_runtimes.RUNTIMES["temurin"]``) with HermiT and the OWL API on its class path. Each jar is
fetched on first use from Maven Central at the version pinned here, checked against the SHA-256
recorded here before it is kept, and cached under the runtime-deps cache root, version-namespaced;
a cache hit never downloads. HermiT and the OWL API are LGPL, trove4j LGPL-2.1: they run in the
reasoner's own process, as a program Provisa starts, under the dependency rules.

Resolution fails loud: a download that fails or a jar whose sum is not the pinned one raises
``RuntimeUnavailable``; there is no unpinned or unchecked fallback.
"""

# Requirements: REQ-2138

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from provisa.runtime_deps.code_runtimes import (
    CodeRuntimeResolver,
    Fetcher,
    RuntimeUnavailable,
    fetch_https,
)
from provisa.runtime_deps.pgwire_bundles import default_cache_root

MAVEN_CENTRAL = "https://repo1.maven.org/maven2"

#: The reasoner program, run from its source.
PROGRAM = Path(__file__).parent / "reasoner" / "Reasoner.java"


@dataclass(frozen=True)
class Jar:
    """One pinned jar: its Maven coordinate and the SHA-256 of the file Central serves (checked,
    when it was pinned, against Central's published SHA-1)."""

    group: str
    artifact: str
    version: str
    sha256: str

    @property
    def filename(self) -> str:
        return f"{self.artifact}-{self.version}.jar"

    @property
    def url(self) -> str:
        path = self.group.replace(".", "/")
        return f"{MAVEN_CENTRAL}/{path}/{self.artifact}/{self.version}/{self.filename}"


# HermiT and the part of the OWL API it reasons with, and their runtime dependencies: HermiT's
# own tree, cut to what classifying needs (no RDF4J, JSON-LD, OBO or HTTP client), with the
# OWL API at its last 5.1 release and the libraries under it at current releases.
JARS: tuple[Jar, ...] = (
    Jar("net.sourceforge.owlapi", "org.semanticweb.hermit", "1.4.5.519",
        "722e79b4d1e11dd08ecc90b1bf65a7c7342cd25ca7ec2664e3a18235bfc6f278"),
    Jar("net.sourceforge.owlapi", "owlapi-api", "5.1.20",
        "d0ccc2ae4f2315132cd49aa84c411869bceb8573acb5a9c3c7231ecf6789a374"),
    Jar("net.sourceforge.owlapi", "owlapi-impl", "5.1.20",
        "c6326a106d01b8ecf1af02a86224b1d02b907fa75c7c225f192fa768b9ad3cda"),
    Jar("net.sourceforge.owlapi", "owlapi-parsers", "5.1.20",
        "6210599b5fae1608cd1f6e3fbdf906d9874d49189da55f85da8264703713e64c"),
    Jar("net.sourceforge.owlapi", "owlapi-apibinding", "5.1.20",
        "e37eddbb10b896c00c7f20a381d5e13d13a59a18b534d0f9d7ca19cfae9453d4"),
    Jar("com.google.guava", "guava", "33.3.1-jre",
        "4bf0e2c5af8e4525c96e8fde17a4f7307f97f8478f11c4c8e35a0e3298ae4e90"),
    Jar("com.google.guava", "failureaccess", "1.0.2",
        "8a8f81cf9b359e3f6dfa691a1e776985c061ef2f223c9b2c80753e1b458e8064"),
    Jar("com.github.ben-manes.caffeine", "caffeine", "2.9.3",
        "1e0a7bbef1dd791653143f3f05d0e489934bf5481e58a87c9e619cd46b68729b"),
    Jar("org.apache.commons", "commons-rdf-api", "0.5.0",
        "360fd9197008502cbc3fd09afe2ad45d8c8c905e21f42e7cd5b2aa1c39742b5e"),
    Jar("javax.inject", "javax.inject", "1",
        "91c77044a50c481636c32d916fd89c9118a72195390452c81065080f957de7ff"),
    Jar("com.google.code.findbugs", "jsr305", "3.0.2",
        "766ad2a0783f2687962c8ad74ceecc38a28b9f72a2d085ee438b7813e928d0c7"),
    Jar("org.slf4j", "slf4j-api", "1.7.36",
        "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0"),
    Jar("org.slf4j", "slf4j-nop", "1.7.36",
        "c214958b07816cb4412b30c7bdbd4308ffdc6ba2a83767b8f3a9229cbd9274d6"),
    Jar("dk.brics.automaton", "automaton", "1.11-8",
        "a24475f6ccfe1cc7a4fe9e34e05ce687b0ce0c6e8cb781e0eced3b186482c61e"),
    Jar("com.github.vsonnier", "hppcrt", "0.7.5",
        "f3cfbd511d0bd5a29b72bcca97fcaa81889cf6698f045c5f658744829c8784b9"),
    Jar("commons-io", "commons-io", "2.17.0",
        "4aa4ca48f3dfd30b78220b7881d8cb93eac4093ec94361b6befa9487998a550b"),
    Jar("org.tukaani", "xz", "1.10",
        "95c63c1a55b22dd6453890a419cc1a640f790bbf7d8ae82db1e30aefefb08888"),
    Jar("net.sf.trove4j", "trove4j", "3.0.3",
        "3c8616203d61a12a7e3487e8b34f3c198c2b5ba9e90da0c7ea32d99cd4958012"),
)  # fmt: skip


class ReasonerBundle:
    """The pinned JDK and jars, resolved into the runtime-deps cache on first use."""

    def __init__(
        self,
        *,
        cache_root: str | Path | None = None,
        fetcher: Fetcher | None = None,
        host: tuple[str, str] | None = None,
    ) -> None:
        self._root = Path(cache_root) if cache_root is not None else default_cache_root()
        self._fetch = fetcher if fetcher is not None else fetch_https
        self._java = CodeRuntimeResolver(cache_root=self._root, fetcher=fetcher, host=host)

    def lib(self) -> Path:
        """The directory of the pinned jars, each fetched and checked on a miss."""
        lib = self._root / "reasoner" / "lib"
        lib.mkdir(parents=True, exist_ok=True)
        for jar in JARS:
            kept = lib / f"{jar.sha256[:16]}-{jar.filename}"
            if kept.is_file():
                continue
            data = self._fetch(jar.url)
            found = hashlib.sha256(data).hexdigest()
            if found != jar.sha256:
                raise RuntimeUnavailable(
                    f"{jar.filename} from {jar.url} has SHA-256 {found}, not the pinned "
                    f"{jar.sha256}"
                )
            # Staged under a name of its own: two refreshes resolve the bundle together.
            fd, staged = tempfile.mkstemp(dir=lib, prefix=f".{kept.name}.")
            with os.fdopen(fd, "wb") as out:
                out.write(data)
            os.replace(staged, kept)
        return lib

    def command(self, *args: str) -> list[str]:
        """The reasoner's command line: its JDK's java, the pinned jars only on its class path
        (each by name, so a jar left in the directory by another pin is never read), its
        program, ``args``."""
        java = self._java.resolve("temurin")
        lib = self.lib()
        classpath = os.pathsep.join(str(lib / f"{jar.sha256[:16]}-{jar.filename}") for jar in JARS)
        return [str(java), "-cp", classpath, str(PROGRAM), *args]
