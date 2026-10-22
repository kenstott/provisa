# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6b9d41-7c3e-4a85-b1d0-6e9a4c27f583
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An OCI image assembled by Provisa itself (REQ-2098): no builder program, no daemon, no root.

An image is its layers -- each a tar of files, gzip-compressed, named by the SHA-256 of what it
holds -- a configuration naming them with its entrypoint, environment, exposed port and labels, and
a manifest naming the configuration and the layers. Provisa writes all three: the base image's
layers are pulled from its registry by digest, and every other layer is a tar Provisa writes from
files it already holds. Nothing in the image runs while it is built.

A layer is reproducible: its entries in path order, every time zero, every owner root. The same
files make the same layer, with the same digest.

The image is pushed to a registry over the OCI distribution API (``/v2/``), signing in with the
registry's bearer-token challenge, or written as a ``docker load`` archive.
"""

# Requirements: REQ-2098
from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import re
import tarfile
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
OCI_CONFIG = "application/vnd.oci.image.config.v1+json"
OCI_LAYER = "application/vnd.oci.image.layer.v1.tar+gzip"
DOCKER_MANIFEST = "application/vnd.docker.distribution.manifest.v2+json"
DOCKER_LAYER = "application/vnd.docker.image.rootfs.diff.tar.gzip"

_TIMEOUT = httpx.Timeout(60.0, read=600.0, write=600.0)


class ImageRefused(Exception):
    """A build or push that cannot go on, named with why (the registry's own answer included)."""


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


# -- layers -------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Entry:
    """One file of a layer: a regular file (from ``source`` or ``data``), a directory, or a
    symbolic link (``link``), at ``path`` inside the image."""

    path: str
    source: Path | None = None
    data: bytes | None = None
    mode: int = 0o644
    directory: bool = False
    link: str | None = None
    #: The owner's user and group id (root when 0).
    owner: int = 0


def tree(root: Path, at: str) -> list[Entry]:
    """Every file and directory under ``root``, placed at ``at`` in the image, symbolic links kept
    as links; each file's mode its own (an executable stays executable)."""
    out = [Entry(at, directory=True, mode=0o755)]
    for path in sorted(root.rglob("*")):
        inside = f"{at}/{path.relative_to(root).as_posix()}"
        if path.is_symlink():
            out.append(Entry(inside, link=os.readlink(path)))
        elif path.is_dir():
            out.append(Entry(inside, directory=True, mode=0o755))
        else:
            out.append(Entry(inside, source=path, mode=path.stat().st_mode & 0o777))
    return out


def archive_entries(filename: str, data: bytes, at: str) -> list[Entry]:
    """Every member of the archive ``data`` (a ``.tar.gz``/``.tgz``, or a ``.zip``/``.whl``),
    placed at ``at`` in the image, read from the archive itself and never extracted here: an
    archive made for another platform may hold names this machine's disk cannot (two that differ
    only in case). A hard link becomes a copy of what it names; an entry that leaves ``at`` is
    refused."""
    import posixpath
    import zipfile

    def placed(name: str) -> str:
        clean = posixpath.normpath(name.removeprefix("./") or ".")
        if posixpath.isabs(clean) or clean == ".." or clean.startswith("../"):
            raise ImageRefused(f"{filename}: entry escapes the archive: {name}")
        return at if clean == "." else f"{at}/{clean}"

    out: list[Entry] = [Entry(at, directory=True, mode=0o755)]
    if filename.endswith((".zip", ".whl")):
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            for info in archive.infolist():
                mode = (info.external_attr >> 16) & 0o777 or (0o755 if info.is_dir() else 0o644)
                if info.is_dir():
                    out.append(Entry(placed(info.filename), directory=True, mode=0o755))
                elif (info.external_attr >> 16) & 0o170000 == 0o120000:
                    out.append(Entry(placed(info.filename), link=archive.read(info).decode()))
                else:
                    out.append(Entry(placed(info.filename), data=archive.read(info), mode=mode))
        return out
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as archive:
        members = {m.name: m for m in archive.getmembers()}
        for member in members.values():
            path = placed(member.name)
            if member.isdir():
                out.append(Entry(path, directory=True, mode=member.mode & 0o777 or 0o755))
            elif member.issym():
                out.append(Entry(path, link=member.linkname))
            elif member.islnk():
                target = members[member.linkname]
                extracted = archive.extractfile(target)
                assert extracted is not None
                out.append(Entry(path, data=extracted.read(), mode=target.mode & 0o777))
            elif member.isfile():
                extracted = archive.extractfile(member)
                assert extracted is not None
                out.append(Entry(path, data=extracted.read(), mode=member.mode & 0o777))
    return out


@dataclass(frozen=True)
class Layer:
    """A layer as an image names it: its compressed blob (on disk), that blob's digest and size,
    and the digest of the tar it decompresses to (its diff id)."""

    blob: Path
    digest: str
    size: int
    diff_id: str
    media_type: str = OCI_LAYER


def _parents(paths: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    for p in paths:
        parts = p.strip("/").split("/")[:-1]
        for i in range(1, len(parts) + 1):
            seen.add("/".join(parts[:i]))
    return sorted(seen)


def write_layer(entries: Iterable[Entry], workdir: Path) -> Layer:
    """A reproducible layer of ``entries``: path order, time zero, owner root; every parent
    directory an entry needs is in it too."""
    entries = list(entries)
    named = {e.path.strip("/"): e for e in entries}
    for parent in _parents(named):
        named.setdefault(parent, Entry(parent, directory=True, mode=0o755))
    workdir.mkdir(parents=True, exist_ok=True)
    plain = Path(tempfile.mkstemp(dir=workdir, suffix=".tar")[1])
    with tarfile.open(plain, "w", format=tarfile.PAX_FORMAT) as tar:
        for name in sorted(named):
            entry = named[name]
            info = tarfile.TarInfo(name)
            info.mtime = 0
            info.uid = info.gid = entry.owner
            info.uname = info.gname = ""
            info.mode = entry.mode
            if entry.directory:
                info.type = tarfile.DIRTYPE
                tar.addfile(info)
            elif entry.link is not None:
                info.type = tarfile.SYMTYPE
                info.linkname = entry.link
                tar.addfile(info)
            elif entry.data is not None:
                info.size = len(entry.data)
                tar.addfile(info, io.BytesIO(entry.data))
            else:
                assert entry.source is not None
                info.size = entry.source.stat().st_size
                with entry.source.open("rb") as f:
                    tar.addfile(info, f)
    diff_id = sha256_of(plain)
    blob = plain.with_suffix(".tar.gz")
    with plain.open("rb") as src, blob.open("wb") as raw:
        # No name and mtime 0 in the gzip header: the same tar compresses to the same blob.
        with gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as gz:
            for chunk in iter(lambda: src.read(1 << 20), b""):
                gz.write(chunk)
    plain.unlink()
    return Layer(blob, sha256_of(blob), blob.stat().st_size, diff_id)


# -- the image ----------------------------------------------------------------------------------


@dataclass
class Image:
    """An image to push or archive: its layers (the base image's first) and its configuration."""

    architecture: str  # amd64 | arm64
    layers: list[Layer]
    entrypoint: list[str]
    env: dict[str, str] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)
    exposed: list[int] = field(default_factory=list)
    working_dir: str = "/"
    user: str = ""
    base_config: dict = field(default_factory=dict)

    def config(self) -> bytes:
        """The image configuration: the base image's environment beneath this image's."""
        base_env = [
            e for e in self.base_config.get("Env") or [] if e.split("=", 1)[0] not in self.env
        ]
        body = {
            "architecture": self.architecture,
            "os": "linux",
            "config": {
                "Entrypoint": self.entrypoint,
                "Env": [*base_env, *(f"{k}={v}" for k, v in self.env.items())],
                "Labels": dict(sorted(self.labels.items())),
                "ExposedPorts": {f"{p}/tcp": {} for p in self.exposed},
                "WorkingDir": self.working_dir,
                **({"User": self.user} if self.user else {}),
            },
            "rootfs": {"type": "layers", "diff_ids": [layer.diff_id for layer in self.layers]},
            "history": [{"created_by": "provisa"} for _ in self.layers],
        }
        return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()

    def manifest(self) -> bytes:
        config = self.config()
        body = {
            "schemaVersion": 2,
            "mediaType": OCI_MANIFEST,
            "config": {
                "mediaType": OCI_CONFIG,
                "digest": "sha256:" + hashlib.sha256(config).hexdigest(),
                "size": len(config),
            },
            "layers": [
                {"mediaType": layer.media_type, "digest": layer.digest, "size": layer.size}
                for layer in self.layers
            ],
            "annotations": dict(sorted(self.labels.items())),
        }
        return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()


def docker_archive(image: Image, repo_tag: str, out: Path) -> None:
    """``image`` as a ``docker load`` archive at ``out``, tagged ``repo_tag``: each layer its
    uncompressed tar, the configuration, and the archive's manifest."""
    config = image.config()
    config_name = hashlib.sha256(config).hexdigest() + ".json"
    with tarfile.open(out, "w") as tar:

        def add(name: str, data: bytes) -> None:
            info = tarfile.TarInfo(name)
            info.size, info.mtime = len(data), 0
            tar.addfile(info, io.BytesIO(data))

        names = []
        for layer in image.layers:
            name = f"{layer.diff_id.removeprefix('sha256:')}/layer.tar"
            with gzip.open(layer.blob, "rb") as plain, tempfile.TemporaryFile() as unpacked:
                for chunk in iter(lambda: plain.read(1 << 20), b""):
                    unpacked.write(chunk)
                info = tarfile.TarInfo(name)
                info.size, info.mtime = unpacked.tell(), 0
                unpacked.seek(0)
                tar.addfile(info, unpacked)
            names.append(name)
        add(config_name, config)
        add(
            "manifest.json",
            json.dumps([{"Config": config_name, "RepoTags": [repo_tag], "Layers": names}]).encode(),
        )


# -- a registry ---------------------------------------------------------------------------------


@dataclass
class Registry:
    """A registry spoken to over the OCI distribution API, signing in through its bearer-token
    challenge with ``username``/``password`` (none: anonymous)."""

    host: str
    username: str | None = None
    password: str | None = None
    verify: str | bool = True
    #: Tests answer the registry in-process; None speaks to it over the network.
    transport: httpx.BaseTransport | None = None
    _tokens: dict[str, str] = field(default_factory=dict)

    @property
    def base(self) -> str:
        return f"https://{self.host}/v2"

    def _client(self) -> httpx.Client:
        return httpx.Client(
            timeout=_TIMEOUT, verify=self.verify, follow_redirects=True, transport=self.transport
        )

    def _token(self, client: httpx.Client, challenge: str, scope: str) -> str:
        params = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
        realm = params.pop("realm", None)
        if realm is None:
            raise ImageRefused(f"{self.host} asked for a sign-in it does not describe: {challenge}")
        params["scope"] = scope
        auth = (self.username, self.password) if self.username and self.password else None
        answered = client.get(realm, params=params, auth=auth)
        if answered.status_code != 200:
            raise ImageRefused(
                f"{self.host} refused the sign-in for {scope}: HTTP {answered.status_code} "
                f"{answered.text[:200]}"
            )
        body = answered.json()
        return body.get("token") or body["access_token"]

    def request(
        self, method: str, url: str, scope: str, *, headers: dict | None = None, **kw: Any
    ) -> httpx.Response:
        """``method`` ``url``, signed in for ``scope`` (a bearer token on the registry's challenge,
        or basic sign-in where the registry asks for that)."""
        with self._client() as client:
            sent = dict(headers or {})
            if scope in self._tokens:
                sent["Authorization"] = f"Bearer {self._tokens[scope]}"
            answered = client.request(method, url, headers=sent, **kw)
            if answered.status_code != 401:
                return answered
            challenge = answered.headers.get("www-authenticate", "")
            if challenge.lower().startswith("basic"):
                if not (self.username and self.password):
                    raise ImageRefused(f"{self.host} asks for a user name and password")
                return client.request(
                    method,
                    url,
                    headers=dict(headers or {}),
                    auth=(self.username, self.password),
                    **kw,
                )
            self._tokens[scope] = self._token(client, challenge, scope)
            sent["Authorization"] = f"Bearer {self._tokens[scope]}"
            return client.request(method, url, headers=sent, **kw)

    def manifest(self, repository: str, reference: str) -> tuple[dict, bytes]:
        """A manifest by digest or tag, and its bytes."""
        answered = self.request(
            "GET",
            f"{self.base}/{repository}/manifests/{reference}",
            f"repository:{repository}:pull",
            headers={"Accept": ", ".join((OCI_MANIFEST, OCI_INDEX, DOCKER_MANIFEST))},
        )
        if answered.status_code != 200:
            raise ImageRefused(f"{self.host}/{repository}@{reference}: HTTP {answered.status_code}")
        return answered.json(), answered.content

    def blob(self, repository: str, digest: str, out: Path) -> None:
        """The blob ``digest`` written to ``out``, checked against its digest."""
        answered = self.request(
            "GET", f"{self.base}/{repository}/blobs/{digest}", f"repository:{repository}:pull"
        )
        if answered.status_code != 200:
            raise ImageRefused(
                f"{self.host}/{repository} blob {digest}: HTTP {answered.status_code}"
            )
        out.write_bytes(answered.content)
        if sha256_of(out) != digest:
            out.unlink()
            raise ImageRefused(
                f"{self.host}/{repository} blob {digest} is not what its digest names"
            )

    def push(self, repository: str, tag: str, image: Image) -> str:
        """Push ``image`` as ``repository:tag``; its manifest's digest."""
        scope = f"repository:{repository}:pull,push"
        config = image.config()
        blobs: list[tuple[str, Path | bytes]] = [
            ("sha256:" + hashlib.sha256(config).hexdigest(), config),
            *((layer.digest, layer.blob) for layer in image.layers),
        ]
        for digest, content in blobs:
            held = self.request("HEAD", f"{self.base}/{repository}/blobs/{digest}", scope)
            if held.status_code == 200:
                continue
            started = self.request("POST", f"{self.base}/{repository}/blobs/uploads/", scope)
            if started.status_code not in (201, 202):
                raise ImageRefused(
                    f"{self.host}/{repository} refused an upload: HTTP {started.status_code} "
                    f"{started.text[:200]}"
                )
            location = started.headers["location"]
            if location.startswith("/"):
                location = f"https://{self.host}{location}"
            data = content if isinstance(content, bytes) else content.read_bytes()
            done = self.request(
                "PUT",
                location,
                scope,
                params={"digest": digest},
                headers={"Content-Type": "application/octet-stream"},
                content=data,
            )
            if done.status_code != 201:
                raise ImageRefused(
                    f"{self.host}/{repository} refused blob {digest}: HTTP {done.status_code} "
                    f"{done.text[:200]}"
                )
        manifest = image.manifest()
        put = self.request(
            "PUT",
            f"{self.base}/{repository}/manifests/{tag}",
            scope,
            headers={"Content-Type": OCI_MANIFEST},
            content=manifest,
        )
        if put.status_code not in (200, 201):
            raise ImageRefused(
                f"{self.host}/{repository} refused the manifest: HTTP {put.status_code} "
                f"{put.text[:200]}"
            )
        return "sha256:" + hashlib.sha256(manifest).hexdigest()


def _oci(media_type: str) -> str:
    """A Docker layer's media type as OCI names the same bytes."""
    return OCI_LAYER if media_type == DOCKER_LAYER else media_type


def pull_base(
    registry: Registry, repository: str, digest: str, cache: Path
) -> tuple[list[Layer], dict]:
    """A base image's layers, by its manifest's ``digest``, from ``cache`` or pulled into it, and
    its configuration."""
    manifest, raw = registry.manifest(repository, digest)
    if "sha256:" + hashlib.sha256(raw).hexdigest() != digest:
        raise ImageRefused(f"{registry.host}/{repository}@{digest} answered another manifest")
    cache.mkdir(parents=True, exist_ok=True)
    config_path = cache / manifest["config"]["digest"].replace(":", "-")
    if not config_path.is_file():
        registry.blob(repository, manifest["config"]["digest"], config_path)
    config = json.loads(config_path.read_text())
    layers = []
    for described, diff_id in zip(manifest["layers"], config["rootfs"]["diff_ids"], strict=True):
        blob = cache / described["digest"].replace(":", "-")
        if not blob.is_file():
            registry.blob(repository, described["digest"], blob)
        layers.append(
            Layer(
                blob, described["digest"], described["size"], diff_id, _oci(described["mediaType"])
            )
        )
    return layers, config.get("config") or {}
