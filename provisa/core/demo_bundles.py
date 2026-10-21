# Copyright (c) 2026 Kenneth Stott
# Canary: 548a51a1-b128-4c56-91f2-f54d32ee6e13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A named demo's bundle: built once, published to a registry, pulled by whoever runs the demo
(REQ-2049).

A bundle is one demo, as its build left it: the demo's directory (config.yaml, demo.yaml, its tour
and example queries), what its seeder wrote (logs/), its data stores' dumps (dump/) and the seed
the sandbox tier lands its template from (seeded/). It is published as an OCI artifact in the one
repository every demo shares -- ``<registry>/provisa-demos``, so its visibility is set once --
tagged ``<name>-<version>`` and ``<name>-latest``. A version is never published twice.

The artifact's config blob is the bundle's manifest (``BundleManifest``): the demo, the version,
the commit it was built from and the oldest Provisa that runs it. Its one layer is the bundle, a
gzipped tar. A bundle that needs a newer Provisa than the one pulling it is refused by name before
its layer is fetched.

Only the OCI distribution API is spoken, over httpx, so neither a deployment nor a solution
engineer's machine needs a registry client installed. A registry that answers 401 names the token
service to ask (``WWW-Authenticate: Bearer``); an anonymous token pulls a public repository, and a
push passes the publisher's credentials to that service.
"""

# Requirements: REQ-2049

from __future__ import annotations

import gzip
import hashlib
import io
import json
import re
import tarfile
from dataclasses import asdict, dataclass
from pathlib import Path

import httpx

ARTIFACT_TYPE = "application/vnd.provisa.demo-bundle.v1"
CONFIG_TYPE = "application/vnd.provisa.demo-bundle.manifest.v1+json"
LAYER_TYPE = "application/vnd.provisa.demo-bundle.layer.v1.tar+gzip"
MANIFEST_TYPE = "application/vnd.oci.image.manifest.v1+json"
LATEST = "latest"
MANIFEST_FILE = "bundle.json"

# What a bundle leaves out of its demo's directory: the seeder's own marker volume.
_LEFT_OUT = ("data",)
# A version tag: the build's UTC minute and commit (202610141530-1a2b3c4d5).
_VERSION = re.compile(r"^\d{12}-[0-9a-f]{7,40}$")
# The release versions Provisa is published under (0.1.0, 0.1.0a483, 0.1.0rc1).
_PROVISA_VERSION = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:(a|b|rc)(\d+))?$")
_PRE_RANK = {"a": 0, "b": 1, "rc": 2}


class BundleError(Exception):
    """A bundle could not be published or pulled; the message names why."""


class OldDemoFormat(BundleError):
    """The bundle is in an older demo format than this Provisa reads."""

    def __init__(self, manifest: "BundleManifest") -> None:
        super().__init__(
            f"demo {manifest.demo} {manifest.version} is in demo format {manifest.format}; this "
            f"Provisa reads format {DEMO_FORMAT}: pull its latest "
            f"(start-ui-install.sh --demo {manifest.demo})"
        )
        self.manifest = manifest


class NeedsNewerProvisa(BundleError):
    """The bundle needs a newer Provisa than the one pulling it."""

    def __init__(self, manifest: BundleManifest, running: str) -> None:
        super().__init__(
            f"demo {manifest.demo} {manifest.version} needs Provisa {manifest.requires_provisa} "
            f"or newer; this is Provisa {running}"
        )
        self.manifest = manifest
        self.running = running


#: The demo format this Provisa reads: bumped whenever a demo's files gain something a reader
#: requires. 2: tour.yaml names its ``landing`` (REQ-2010, the tour's "Where next?").
DEMO_FORMAT = 2


@dataclass(frozen=True)
class BundleManifest:
    demo: str
    version: str
    commit: str
    requires_provisa: str
    #: The demo format the bundle was built in (DEMO_FORMAT when it was built).
    format: int = DEMO_FORMAT

    @classmethod
    def read(cls, data: dict) -> BundleManifest:
        missing = [f for f in ("demo", "version", "commit", "requires_provisa") if f not in data]
        if missing:
            raise BundleError(f"the bundle manifest has no {', '.join(missing)}")
        # A manifest naming no format was written before formats were versioned: format 1, the
        # one before DEMO_FORMAT 2 (REQ-2049) -- so such a bundle is refused, not read.
        return cls(
            data["demo"],
            data["version"],
            data["commit"],
            data["requires_provisa"],
            data.get("format", 1),
        )


def seeding_run() -> bool:
    """Whether this process is a bundle build's seeding run (``PROVISA_DEMO_SEED_EXPORT=true``,
    scripts/demo_bundle.py): a throwaway sandbox tier that lands each demo's template from the
    demo's own sources and writes the seed. It serves no organization but the demos'."""
    import os

    value = os.environ.get("PROVISA_DEMO_SEED_EXPORT", "false")
    if value not in ("true", "false"):
        raise ValueError(f"PROVISA_DEMO_SEED_EXPORT must be true or false, got {value!r}")
    return value == "true"


def provisa_version_key(version: str) -> tuple[int, int, int, int, int]:
    """``version`` ordered as PEP 440 orders a release and its pre-releases."""
    match = _PROVISA_VERSION.match(version)
    if match is None:
        raise BundleError(f"{version!r} is not a Provisa release version")
    major, minor, patch, pre, number = match.groups()
    stage, step = (_PRE_RANK[pre], int(number)) if pre else (3, 0)
    return (int(major), int(minor), int(patch), stage, step)


def running_provisa() -> str:
    from importlib.metadata import version

    return version("provisa")


def check_runs_here(manifest: BundleManifest, running: str) -> None:
    """Refuse, by name, a bundle that needs a newer Provisa than ``running``, or one in an older
    demo format than this Provisa reads."""
    if provisa_version_key(running) < provisa_version_key(manifest.requires_provisa):
        raise NeedsNewerProvisa(manifest, running)
    if manifest.format < DEMO_FORMAT:
        raise OldDemoFormat(manifest)


def check_directory(directory: Path) -> None:
    """Refuse, by name, a demo directory that is a bundle (it has a bundle.json) this Provisa
    cannot read -- at load, rather than failing each request that reads it. A checkout's demo
    directory is no bundle: it is the current format by being the source."""
    record = directory / MANIFEST_FILE
    if record.is_file():
        check_runs_here(BundleManifest.read(json.loads(record.read_text())), running_provisa())


def link_declared(directory: Path, beside: Path) -> None:
    """Link into ``beside`` what the config in ``directory`` declares beside it: its replica rows
    (REQ-2089), its code archives (REQ-2101) and its secrets (REQ-2095). Each is read from the
    directory of the config served, so a copy of the config served from elsewhere (the bundle
    build's answering config, a test server's) is given them where it is."""
    from provisa.codesource.declared_code import CODE_DIRECTORY
    from provisa.core.demo_secrets import FILE as SECRETS_FILE
    from provisa.federation.declared_replica_rows import ROWS_DIR

    for folder in (ROWS_DIR, CODE_DIRECTORY):
        if (directory / folder).is_dir():
            (beside / folder).symlink_to((directory / folder).resolve(), target_is_directory=True)
    if (directory / SECRETS_FILE).is_file():
        (beside / SECRETS_FILE).symlink_to((directory / SECRETS_FILE).resolve())


#: Where the project publishes its demos (demo-bundles.yml): what ``start-ui-install.sh --demo``
#: and a deployment's sandbox tier pull from unless told another registry (a test's own).
PUBLISHED_AT = "https://ghcr.io/kenstott"
#: The one repository every demo's bundles are published to.
REPOSITORY = "provisa-demos"


def tag_of(name: str, version: str) -> str:
    """The tag demo ``name``'s bundle ``version`` (or ``latest``) is published under."""
    return f"{name}-{version}"


# -- packing ------------------------------------------------------------------------------------


def pack(directory: Path, manifest: BundleManifest) -> bytes:
    """The bundle of ``directory`` (a built demo), as a gzipped tar, with ``manifest`` in it.
    Byte-for-byte the same for the same files: names sorted, times and owners cleared."""
    if not _VERSION.match(manifest.version):
        raise BundleError(f"{manifest.version!r} is not a bundle version (<UTC minute>-<commit>)")
    files = sorted(
        p
        for p in directory.rglob("*")
        if p.is_file() and p.relative_to(directory).parts[0] not in _LEFT_OUT
    )
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as tar:
        for path in files:
            _add(tar, str(path.relative_to(directory)), path.read_bytes(), path.stat().st_mode)
        _add(tar, MANIFEST_FILE, _manifest_bytes(manifest), 0o644)
    return gzip.compress(raw.getvalue(), mtime=0)


def _add(tar: tarfile.TarFile, name: str, data: bytes, mode: int) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mode = mode & 0o755
    tar.addfile(info, io.BytesIO(data))


def _manifest_bytes(manifest: BundleManifest) -> bytes:
    return json.dumps(asdict(manifest), indent=2, sort_keys=True).encode() + b"\n"


def unpack(blob: bytes, target: Path) -> None:
    """Write the bundle ``blob`` into ``target``, which must not exist. A member that is not a
    plain file or directory, or whose path leaves ``target``, refuses the whole bundle."""
    if target.exists():
        raise BundleError(f"{target} exists; a bundle is unpacked into a new directory")
    staging = target.with_name(f".{target.name}.unpacking")
    if staging.exists():
        raise BundleError(f"{staging} exists: an earlier unpack was interrupted; remove it")
    with tarfile.open(fileobj=io.BytesIO(gzip.decompress(blob)), mode="r") as tar:
        members = tar.getmembers()
        for member in members:
            if not (member.isfile() or member.isdir()):
                raise BundleError(f"the bundle holds {member.name!r}, which is not a file")
        tar.extractall(staging, members=members, filter="data")
    staging.rename(target)


# -- the registry -------------------------------------------------------------------------------


class Registry:
    """One registry namespace, e.g. ``https://ghcr.io/kenstott``. ``credentials`` (user, token)
    are passed to its token service when pushing."""

    def __init__(
        self,
        base: str,
        credentials: tuple[str, str] | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        match = re.match(r"^(https?://[^/]+)/([a-z0-9._/-]+)$", base.rstrip("/"))
        if match is None:
            raise BundleError(f"{base!r} is not a registry namespace (https://host/namespace)")
        self.origin, self.namespace = match.groups()
        self.credentials = credentials
        self.client = client or httpx.Client(timeout=120, follow_redirects=True)
        self._tokens: dict[str, str] = {}

    def _url(self, path: str) -> str:
        return f"{self.origin}/v2/{self.namespace}/{REPOSITORY}/{path}"

    def _request(self, method: str, url: str, scope: str, **kw) -> httpx.Response:
        headers = dict(kw.pop("headers", {}))
        if scope in self._tokens:
            headers["Authorization"] = f"Bearer {self._tokens[scope]}"
        response = self.client.request(method, url, headers=headers, **kw)
        if response.status_code != 401 or scope in self._tokens:
            return response
        self._tokens[scope] = self._token(response.headers.get("www-authenticate", ""), scope)
        headers["Authorization"] = f"Bearer {self._tokens[scope]}"
        return self.client.request(method, url, headers=headers, **kw)

    def _token(self, challenge: str, scope: str) -> str:
        fields = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
        if not challenge.lower().startswith("bearer ") or "realm" not in fields:
            raise BundleError(
                f"{self.origin} asked for credentials it does not name: {challenge!r}"
            )
        params = {"scope": scope}
        if "service" in fields:
            params["service"] = fields["service"]
        auth = httpx.BasicAuth(*self.credentials) if self.credentials else None
        response = self.client.get(fields["realm"], params=params, auth=auth)
        _ok(response, f"a token for {scope}")
        body = response.json()
        return body.get("token") or body["access_token"]

    def _scope(self, actions: str) -> str:
        return f"repository:{self.namespace}/{REPOSITORY}:{actions}"

    # pulling

    def resolve(self, name: str, tag: str) -> tuple[str, dict]:
        """The digest and manifest ``tag`` of demo ``name`` names."""
        response = self._request(
            "GET",
            self._url(f"manifests/{tag_of(name, tag)}"),
            self._scope("pull"),
            headers={"Accept": MANIFEST_TYPE},
        )
        if response.status_code == 404:
            raise BundleError(
                f"demo {name} has no bundle {tag!r} at {self.origin}/{self.namespace}"
            )
        _ok(response, f"demo {name} bundle {tag}")
        manifest = response.json()
        if manifest.get("artifactType") != ARTIFACT_TYPE:
            raise BundleError(f"demo {name} {tag} is not a demo bundle")
        return response.headers["docker-content-digest"], manifest

    def _blob(self, name: str, descriptor: dict) -> bytes:
        response = self._request(
            "GET", self._url(f"blobs/{descriptor['digest']}"), self._scope("pull")
        )
        _ok(response, f"demo {name} blob {descriptor['digest']}")
        if _digest(response.content) != descriptor["digest"]:
            raise BundleError(f"demo {name} blob {descriptor['digest']} does not match its digest")
        return response.content

    def manifest_of(self, name: str, oci_manifest: dict) -> BundleManifest:
        return BundleManifest.read(json.loads(self._blob(name, oci_manifest["config"])))

    def pull(self, name: str, tag: str, target: Path, running: str) -> BundleManifest:
        """Pull demo ``name``'s bundle ``tag`` into the new directory ``target``; refused before
        its layer is fetched when it needs a newer Provisa than ``running``."""
        _resolved, oci_manifest = self.resolve(name, tag)
        manifest = self.manifest_of(name, oci_manifest)
        if manifest.demo != name:
            raise BundleError(f"the bundle at {name} {tag} is demo {manifest.demo}'s")
        check_runs_here(manifest, running)
        (layer,) = oci_manifest["layers"]
        unpack(self._blob(name, layer), target)
        return manifest

    # pushing

    def exists(self, name: str, tag: str) -> bool:
        response = self._request(
            "HEAD",
            self._url(f"manifests/{tag_of(name, tag)}"),
            self._scope("pull"),
            headers={"Accept": MANIFEST_TYPE},
        )
        if response.status_code == 404:
            return False
        _ok(response, f"demo {name} bundle {tag}")
        return True

    def _put_blob(self, name: str, data: bytes) -> dict:
        digest = _digest(data)
        scope = self._scope("pull,push")
        started = self._request("POST", self._url("blobs/uploads/"), scope)
        _ok(started, f"an upload to demo {name}")
        location = httpx.URL(self.origin).join(started.headers["location"])
        done = self._request(
            "PUT",
            str(location.copy_merge_params({"digest": digest})),
            scope,
            content=data,
            headers={"Content-Type": "application/octet-stream"},
        )
        _ok(done, f"blob {digest} of demo {name}")
        return {"digest": digest, "size": len(data)}

    def push(self, manifest: BundleManifest, bundle: bytes) -> str:
        """Publish ``bundle`` as demo ``manifest.demo``'s version ``manifest.version`` and as its
        latest. A version already published is refused: a version names one bundle forever."""
        name = manifest.demo
        if self.exists(name, manifest.version):
            raise BundleError(f"demo {name} {manifest.version} is published already")
        config = {**self._put_blob(name, _manifest_bytes(manifest)), "mediaType": CONFIG_TYPE}
        layer = {
            **self._put_blob(name, bundle),
            "mediaType": LAYER_TYPE,
            "annotations": {"org.opencontainers.image.title": f"{name}-{manifest.version}.tar.gz"},
        }
        body = json.dumps(
            {
                "schemaVersion": 2,
                "mediaType": MANIFEST_TYPE,
                "artifactType": ARTIFACT_TYPE,
                "config": config,
                "layers": [layer],
                "annotations": {
                    "org.opencontainers.image.version": manifest.version,
                    "org.opencontainers.image.revision": manifest.commit,
                },
            },
            sort_keys=True,
        ).encode()
        scope = self._scope("pull,push")
        for tag in (manifest.version, LATEST):
            put = self._request(
                "PUT",
                self._url(f"manifests/{tag_of(name, tag)}"),
                scope,
                content=body,
                headers={"Content-Type": MANIFEST_TYPE},
            )
            _ok(put, f"demo {name} tag {tag}")
        return _digest(body)


def _digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _ok(response: httpx.Response, what: str) -> None:
    if response.status_code >= 300:
        raise BundleError(f"{what}: HTTP {response.status_code} {response.text[:300]}")


# -- where a deployment's demos are (REQ-2049) ----------------------------------------------------
#
# A deployment offering demos as bundles (the sandbox tier) names them in PROVISA_DEMO_BUNDLES. The
# node pulls each bundle (scripts/demo_stores_sync.sh) and marks its version offered
# (<name>/pulled); it starts the demo's stores only once the tier has asked for the demo since the
# node started (<name>/requested, which the node clears when it starts, with <name>/ready): its
# data is loaded into the node's shared stores, its stores.env written (where its stores are, and
# the names of its data), and the version marked ready (<name>/ready). The tier asks for a demo at
# its first open, and again when the registry names a version newer than the ready one (the node
# always pulls the latest published); it records in <name>/in-use the versions its open sandboxes
# and template read, which the node keeps. When a demo's last sandbox closes, the tier rewrites
# <name>/requested as the time its stores are taken down (sandbox.demo_stores_linger after the
# close); an open before then marks it requested again, and at that time the node takes its stores
# down and clears <name>/requested and <name>/ready, so the next open asks for it again.

_PKG_NAMED = Path(__file__).resolve().parents[1] / "_config" / "demo" / "named"
_REPO_NAMED = Path(__file__).resolve().parents[2] / "demo" / "named"
#: Touched whenever the tier asks for a demo: the node's path unit watches this one file.
WANTED_SIGNAL = ".wanted"


def named_demos_dir() -> Path:
    """Where the named demos are when a deployment serves their directories rather than bundles:
    ``$PROVISA_NAMED_DEMOS_DIR`` when the deployment says, else the wheel-staged copy (REQ-1127),
    else the source checkout's ``demo/named``."""
    import os

    configured = os.environ.get("PROVISA_NAMED_DEMOS_DIR")
    if configured:
        return Path(configured)
    return _PKG_NAMED if _PKG_NAMED.is_dir() else _REPO_NAMED


def bundled_demos() -> tuple[str, ...]:
    """The demos this deployment serves as bundles (``PROVISA_DEMO_BUNDLES``, comma-separated);
    none when it serves the directories under ``named_demos_dir()`` instead."""
    import os

    listed = os.environ.get("PROVISA_DEMO_BUNDLES", "")
    return tuple(name.strip() for name in listed.split(",") if name.strip())


def bundles_root() -> Path:
    """Where the node unpacks the bundles: ``<data dir>/demo-bundles/<name>/<version>``."""
    from provisa.core.org_files import data_dir

    return data_dir() / "demo-bundles"


def _read(path: Path) -> str | None:
    return path.read_text().strip() or None if path.is_file() else None


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(f".{path.name}.writing")
    staged.write_text(text + "\n")
    staged.replace(path)


def pulled_version(name: str) -> str | None:
    """The version of demo ``name`` the node has pulled: the one offered."""
    return _read(bundles_root() / name / "pulled")


def ready_version(name: str) -> str | None:
    """The version of demo ``name`` whose stores the node has up: the one served. The node clears
    it when it starts (scripts/demo_stores_sync.sh --boot), before docker does, as its stores are
    not running then."""
    return _read(bundles_root() / name / "ready")


def request(name: str) -> None:
    """Ask the node to bring demo ``name``'s stores up: they are started only once it is asked for
    (its first open since the node started)."""
    _write(bundles_root() / name / "requested", "requested")
    (bundles_root() / WANTED_SIGNAL).touch()


#: ``<name>/requested`` once demo ``name``'s last sandbox has closed: the time its stores are
#: taken down unless it is asked for again first (``until <epoch seconds>``).
LINGER_PREFIX = "until "


def linger(name: str, until: float) -> None:
    """Demo ``name``'s last sandbox has closed: the node takes its stores down at ``until`` (epoch
    seconds) unless it is asked for again first (scripts/demo_stores_sync.sh). A demo not asked
    for is left as it is: none of its stores is up."""
    marker = bundles_root() / name / "requested"
    if marker.is_file():
        _write(marker, f"{LINGER_PREFIX}{int(until)}")


def hold(name: str) -> None:
    """Demo ``name`` is opened again: one lingering since its last sandbox closed is kept up."""
    marker = bundles_root() / name / "requested"
    if (_read(marker) or "").startswith(LINGER_PREFIX):
        _write(marker, "requested")


def record_in_use(name: str, versions: set[str]) -> None:
    """The versions of demo ``name`` something on the tier still reads (its template's, its open
    sandboxes'), so the node keeps their stores up."""
    _write(bundles_root() / name / "in-use", "\n".join(sorted(versions)))


def own_demo_directory(org_id: str | None) -> Path | None:
    """The directory of the demo a sandbox-tier org is: a template's, its demo's (every version,
    when bundled); a sandbox's, the version it was opened from. None for any other org."""
    from provisa.core import org_files
    from provisa.core.org_ids import TEMPLATE_PREFIX, is_sandbox_tier_org, is_template_org

    if org_id is None or not is_sandbox_tier_org(org_id):
        return None
    if is_template_org(org_id):
        name = org_id[len(TEMPLATE_PREFIX) :]
        return (bundles_root() if bundled_demos() else named_demos_dir()) / name
    recorded = org_files.folder(org_id) / SANDBOX_DEMO_DIRECTORY
    return Path(recorded.read_text()) if recorded.is_file() else None


#: Where a sandbox records the demo directory (version) it was opened from, in its org folder.
SANDBOX_DEMO_DIRECTORY = "demo-directory"


def stores_env(directory: Path) -> dict[str, str]:
    """Where a bundle's stores are, as the node wrote it: ``stores.env`` (NAME=value lines), the
    values its configuration's ``${env:NAME}`` references take. None of them when the bundle runs
    no stores."""
    path = directory / "stores.env"
    if not path.is_file():
        if (directory / "docker-compose.run.yml").is_file():
            raise BundleError(f"{directory} runs stores but the node wrote no stores.env for them")
        return {}
    values = {}
    for line in path.read_text().splitlines():
        if line.strip():
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def store_namespaces(directory: Path) -> dict[str, str]:
    """The names of a demo's stores' data where the demo runs its stores itself -- its checkout's
    or a bundle's own containers (start-ui-install.sh --demo, the bundle build, a test instance):
    each variable a store of its run compose file names (``com.provisa.demo.ns_env``), empty. The
    sandbox tier's node sets them for its shared stores in the bundle's stores.env
    (scripts/demo_stores.py)."""
    import yaml

    run = directory / "docker-compose.run.yml"
    if not run.is_file():
        return {}
    services = (yaml.safe_load(run.read_text()) or {}).get("services") or {}
    return {
        labels["com.provisa.demo.ns_env"]: ""
        for labels in ((spec.get("labels") or {}) for spec in services.values())
        if labels.get("com.provisa.demo.ns_env")
    }


def pull_into(spec: str, into: Path, registry_url: str) -> Path:
    """Pull ``<name>[@<version>]`` (its latest when no version is named) into
    ``into/<name>/<version>``, unless that version is there already -- a version names one bundle
    forever -- and return the directory. The pull ``start-ui-install.sh --demo`` and the sandbox
    tier's node run (scripts/demo_bundle.py pull; python -m provisa.core.demo_bundles pull)."""
    name, _, version = spec.partition("@")
    registry = Registry(registry_url)
    if not version:
        _digest, oci_manifest = registry.resolve(name, LATEST)
        version = registry.manifest_of(name, oci_manifest).version
    target = into.expanduser() / name / version
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        registry.pull(name, version, target, running_provisa())
    check_directory(target)  # a version pulled before: still one this Provisa reads
    return target


def _main(argv: list[str] | None = None) -> int:
    """``python -m provisa.core.demo_bundles pull <name>[@<version>] --into DIR``: the pull, where
    only the package is installed (the app image, which the sandbox tier's node runs it in)."""
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(description="Pull a named demo's bundle.")
    sub = parser.add_subparsers(dest="command", required=True)
    q = sub.add_parser("pull", help="pull a demo's bundle and print its directory")
    q.add_argument("spec", help="<name> or <name>@<version>")
    q.add_argument("--into", type=Path, required=True)
    q.add_argument("--registry", default=os.environ.get("PROVISA_DEMO_REGISTRY", PUBLISHED_AT))
    args = parser.parse_args(argv)
    try:
        print(pull_into(args.spec, args.into, args.registry))
    except BundleError as refused:
        print(f"ERROR: {refused}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
