# Copyright (c) 2026 Kenneth Stott
# Canary: 42c5b1b5-3dfe-4af7-9214-1dd3849f5d4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo bundle round trip through a registry speaking the OCI distribution API (REQ-2049).

The registry here is an in-memory one behind httpx.MockTransport that answers the calls a real one
does, its token service included: a bearer challenge on every unauthenticated call, a token for
anyone pulling and only for the publisher pushing."""

# Requirements: REQ-2049

from __future__ import annotations

import hashlib
import io
import json
import re
import tarfile
import gzip

import httpx
import pytest

from provisa.core import demo_bundles as db

_PUBLISHER = ("ci", "s3cret")


class FakeRegistry:
    def __init__(self) -> None:
        self.blobs: dict[str, bytes] = {}
        self.manifests: dict[str, bytes] = {}
        self.uploads = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        url = request.url
        if url.path == "/token":
            scope = url.params["scope"]
            if "push" in scope:
                expected = "Basic " + __import__("base64").b64encode(b"ci:s3cret").decode()
                if request.headers.get("authorization") != expected:
                    return httpx.Response(403)
            return httpx.Response(200, json={"token": f"t:{scope}"})
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer t:"):
            return httpx.Response(
                401,
                headers={
                    "www-authenticate": 'Bearer realm="https://reg.test/token",service="reg.test"'
                },
            )
        if request.method in ("POST", "PUT") and "push" not in auth:
            return httpx.Response(403)
        path = url.path
        assert path.startswith("/v2/acme/provisa-demos/"), path
        rest = path[len("/v2/acme/provisa-demos/") :]
        if rest.startswith("manifests/"):
            tag = rest[len("manifests/") :]
            if request.method in ("GET", "HEAD"):
                if tag not in self.manifests:
                    return httpx.Response(404)
                body = self.manifests[tag]
                digest = "sha256:" + hashlib.sha256(body).hexdigest()
                return httpx.Response(
                    200,
                    content=b"" if request.method == "HEAD" else body,
                    headers={"docker-content-digest": digest},
                )
            self.manifests[tag] = request.content
            return httpx.Response(201)
        if rest == "blobs/uploads/":
            self.uploads += 1
            return httpx.Response(202, headers={"location": f"/upload/{self.uploads}"})
        if rest.startswith("blobs/"):
            digest = rest[len("blobs/") :]
            if digest not in self.blobs:
                return httpx.Response(404)
            return httpx.Response(200, content=self.blobs[digest])
        raise AssertionError(path)

    def upload_handler(self, request: httpx.Request) -> httpx.Response:
        digest = request.url.params["digest"]
        assert digest == "sha256:" + hashlib.sha256(request.content).hexdigest()
        self.blobs[digest] = request.content
        return httpx.Response(201)


@pytest.fixture
def registry_pair():
    fake = FakeRegistry()

    def route(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/upload/"):
            if not request.headers.get("authorization", "").startswith("Bearer t:"):
                return httpx.Response(
                    401, headers={"www-authenticate": 'Bearer realm="https://reg.test/token"'}
                )
            return fake.upload_handler(request)
        return fake.handler(request)

    def make(credentials=None):
        return db.Registry(
            "https://reg.test/acme",
            credentials=credentials,
            client=httpx.Client(transport=httpx.MockTransport(route), follow_redirects=True),
        )

    return fake, make


def _demo(tmp_path):
    demo = tmp_path / "weblogs"
    (demo / "logs" / "nginx").mkdir(parents=True)
    (demo / "data").mkdir()
    (demo / "config.yaml").write_text("sources: []\n")
    (demo / "logs" / "nginx" / "access.json").write_text('{"status": 200}\n')
    (demo / "data" / "marker").write_text("seeded")
    return demo


def _manifest(version="202610141530-1a2b3c4", requires="0.1.0a483"):
    return db.BundleManifest("weblogs", version, "1a2b3c4d5e", requires)


def test_a_bundle_is_published_under_its_version_and_latest_and_pulled_back(
    tmp_path, registry_pair
):
    fake, make = registry_pair
    manifest = _manifest()
    make(_PUBLISHER).push(manifest, db.pack(_demo(tmp_path), manifest))
    assert set(fake.manifests) == {"weblogs-202610141530-1a2b3c4", "weblogs-latest"}

    target = tmp_path / "pulled"
    got = make().pull("weblogs", db.LATEST, target, running="0.1.0a483")
    assert got == manifest
    assert (target / "config.yaml").read_text() == "sources: []\n"
    assert (target / "logs" / "nginx" / "access.json").is_file()
    assert not (target / "data").exists()  # the seeder's marker stays out
    assert json.loads((target / db.MANIFEST_FILE).read_text())["version"] == manifest.version


def test_a_version_is_never_published_twice(tmp_path, registry_pair):
    _fake, make = registry_pair
    manifest = _manifest()
    bundle = db.pack(_demo(tmp_path), manifest)
    make(_PUBLISHER).push(manifest, bundle)
    with pytest.raises(db.BundleError, match="published already"):
        make(_PUBLISHER).push(manifest, bundle)


def test_only_the_publisher_pushes(tmp_path, registry_pair):
    _fake, make = registry_pair
    manifest = _manifest()
    with pytest.raises(db.BundleError, match="HTTP 403"):
        make().push(manifest, db.pack(_demo(tmp_path), manifest))


def test_a_bundle_needing_a_newer_provisa_is_refused_by_name_before_its_layer(
    tmp_path, registry_pair
):
    fake, make = registry_pair
    manifest = _manifest(requires="0.1.0a483")
    make(_PUBLISHER).push(manifest, db.pack(_demo(tmp_path), manifest))
    layer = json.loads(fake.manifests["weblogs-latest"])["layers"][0]["digest"]
    del fake.blobs[layer]  # fetching it would fail differently
    with pytest.raises(
        db.BundleError,
        match=r"weblogs .* needs Provisa 0\.1\.0a483 or newer; this is Provisa 0\.1\.0a482",
    ):
        make().pull("weblogs", db.LATEST, tmp_path / "pulled", running="0.1.0a482")
    assert not (tmp_path / "pulled").exists()


def test_an_unknown_bundle_is_refused_by_name(tmp_path, registry_pair):
    _fake, make = registry_pair
    with pytest.raises(db.BundleError, match="demo weblogs has no bundle '202601010000-abcdef0'"):
        make().pull("weblogs", "202601010000-abcdef0", tmp_path / "x", running="0.1.0")


def test_packing_is_reproducible(tmp_path):
    demo = _demo(tmp_path)
    assert db.pack(demo, _manifest()) == db.pack(demo, _manifest())


def test_a_bundle_that_escapes_its_directory_is_refused(tmp_path):
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w") as tar:
        info = tarfile.TarInfo("../outside")
        info.size = 1
        tar.addfile(info, io.BytesIO(b"x"))
    with pytest.raises(tarfile.FilterError):
        db.unpack(gzip.compress(raw.getvalue()), tmp_path / "target")
    assert not (tmp_path / "outside").exists()


def test_a_link_in_a_bundle_is_refused(tmp_path):
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w") as tar:
        info = tarfile.TarInfo("evil")
        info.type = tarfile.SYMTYPE
        info.linkname = "/etc/passwd"
        tar.addfile(info)
    with pytest.raises(db.BundleError, match="not a file"):
        db.unpack(gzip.compress(raw.getvalue()), tmp_path / "target")


@pytest.mark.parametrize(
    ("running", "required", "runs"),
    [
        ("0.1.0", "0.1.0a483", True),  # a checkout's version is newer than any of its alphas
        ("0.1.0a483", "0.1.0a483", True),
        ("0.1.0a482", "0.1.0a483", False),
        ("0.1.0rc1", "0.1.0a999", True),
        ("0.2.0a1", "0.1.0", True),
    ],
)
def test_the_provisa_version_rule(running, required, runs):
    manifest = _manifest(requires=required)
    if runs:
        db.check_runs_here(manifest, running)
    else:
        with pytest.raises(db.BundleError):
            db.check_runs_here(manifest, running)


def test_a_version_tag_names_the_build_minute_and_commit(tmp_path):
    with pytest.raises(db.BundleError, match="not a bundle version"):
        db.pack(_demo(tmp_path), _manifest(version="latest"))
    assert re.match(r"^weblogs-latest$", db.tag_of("weblogs", db.LATEST))


# --- the demo format (the maintainer's local-dev: a bundle built before tour.yaml's `landing`) ---


def _old_bundle(directory) -> None:
    """A bundle as published before demo formats were versioned (weblogs 202610141838-c48b72e45):
    its bundle.json names no format, and its tour.yaml no landing."""
    directory.mkdir(parents=True)
    (directory / "bundle.json").write_text(
        json.dumps(
            {
                "demo": "weblogs",
                "version": "202610141838-c48b72e45",
                "commit": "c48b72e45" + "0" * 31,
                "requires_provisa": "0.1.0a483",
            }
        )
    )
    (directory / "tour.yaml").write_text("steps: []\n")


def test_a_bundle_in_an_older_demo_format_is_refused_by_name_at_load(tmp_path):
    old = tmp_path / "weblogs" / "202610141838-c48b72e45"
    _old_bundle(old)
    with pytest.raises(db.OldDemoFormat) as refused:
        db.check_directory(old)
    assert str(refused.value) == (
        "demo weblogs 202610141838-c48b72e45 is in demo format 1; this Provisa reads format "
        f"{db.DEMO_FORMAT}: pull its latest (start-ui-install.sh --demo weblogs)"
    )


def test_a_version_pulled_before_is_checked_again_when_asked_for(tmp_path, registry_pair):
    _fake, make = registry_pair
    _old_bundle(tmp_path / "demos" / "weblogs" / "202610141838-c48b72e45")
    with pytest.raises(db.OldDemoFormat, match="demo format 1"):
        db.pull_into("weblogs@202610141838-c48b72e45", tmp_path / "demos", "https://reg.test/acme")


def test_the_current_format_loads_and_a_checkout_is_the_current_format(tmp_path, registry_pair):
    fake, make = registry_pair
    manifest = _manifest()
    assert manifest.format == db.DEMO_FORMAT
    make(_PUBLISHER).push(manifest, db.pack(_demo(tmp_path), manifest))
    target = tmp_path / "pulled"
    make().pull("weblogs", db.LATEST, target, running="0.1.0a483")
    db.check_directory(target)  # its bundle.json names the current format
    assert json.loads((target / db.MANIFEST_FILE).read_text())["format"] == db.DEMO_FORMAT
    db.check_directory(tmp_path / "weblogs")  # a checkout's demo: no bundle.json


def test_a_published_bundle_in_an_older_format_is_refused_before_its_layer(tmp_path, registry_pair):
    fake, make = registry_pair
    old = db.BundleManifest("weblogs", "202610141838-c48b72e45", "c" * 40, "0.1.0a483", format=1)
    make(_PUBLISHER).push(old, db.pack(_demo(tmp_path), old))
    with pytest.raises(db.OldDemoFormat, match="pull its latest"):
        make().pull("weblogs", db.LATEST, tmp_path / "pulled", running="0.1.0a483")
    assert not (tmp_path / "pulled").exists()


# --- a bundle needing a newer runtime is refused by name; one this build satisfies loads ---


def _bundle_dir(directory, manifest: db.BundleManifest) -> None:
    directory.mkdir(parents=True)
    (directory / db.MANIFEST_FILE).write_text(
        json.dumps(
            {
                "demo": manifest.demo,
                "version": manifest.version,
                "commit": manifest.commit,
                "requires_provisa": manifest.requires_provisa,
                "format": manifest.format,
            }
        )
    )


def test_a_bundle_needing_a_newer_release_is_refused_by_name_at_load(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "running_provisa", lambda: "0.1.0a495")
    pulled = tmp_path / "agentdata" / "202610281400-b6900f87a"
    _bundle_dir(
        pulled, db.BundleManifest("agentdata", "202610281400-b6900f87a", "b" * 40, "0.1.0a496")
    )
    with pytest.raises(db.NeedsNewerProvisa) as refused:
        db.check_directory(pulled)
    assert str(refused.value) == (
        "demo agentdata 202610281400-b6900f87a needs Provisa 0.1.0a496 or newer; "
        "this is Provisa 0.1.0a495"
    )


def test_a_bundle_in_a_newer_demo_format_is_refused_by_name_at_load(tmp_path, monkeypatch):
    # A build that is no release reports the unstamped version, newer than every alpha, so only the
    # format can say the bundle needs code this build does not have.
    monkeypatch.setattr(db, "running_provisa", lambda: "0.1.0")
    pulled = tmp_path / "churnprod" / "202611011200-abcdef1"
    newer = db.DEMO_FORMAT + 1
    _bundle_dir(
        pulled,
        db.BundleManifest("churnprod", "202611011200-abcdef1", "a" * 40, "0.1.0a496", newer),
    )
    with pytest.raises(db.NewerDemoFormat) as refused:
        db.check_directory(pulled)
    assert str(refused.value) == (
        f"demo churnprod 202611011200-abcdef1 is in demo format {newer}; this Provisa reads "
        f"format {db.DEMO_FORMAT}: it needs a newer Provisa"
    )


def test_a_published_bundle_in_a_newer_format_is_refused_before_its_layer(tmp_path, registry_pair):
    _fake, make = registry_pair
    newer = db.BundleManifest(
        "weblogs", "202611011200-abcdef1", "a" * 40, "0.1.0a483", format=db.DEMO_FORMAT + 1
    )
    make(_PUBLISHER).push(newer, db.pack(_demo(tmp_path), newer))
    with pytest.raises(db.NewerDemoFormat, match="needs a newer Provisa"):
        make().pull("weblogs", db.LATEST, tmp_path / "pulled", running="0.1.0")
    assert not (tmp_path / "pulled").exists()


def test_a_bundle_this_build_satisfies_loads(tmp_path):
    # The real running version: a build from this checkout carries what its demos use.
    pulled = tmp_path / "agentdata" / "202610281400-b6900f87a"
    _bundle_dir(
        pulled, db.BundleManifest("agentdata", "202610281400-b6900f87a", "b" * 40, "0.1.0a496")
    )
    db.check_directory(pulled)


@pytest.mark.parametrize(
    "name", ["agentdata", "churnprod", "cloudpolicy", "files", "hasuramigration"]
)
def test_this_build_runs_each_demo_it_carries(name):
    import yaml

    from pathlib import Path

    home = Path(__file__).resolve().parents[2] / "demo" / "named" / name
    required = yaml.safe_load((home / "demo.yaml").read_text())["requires_provisa"]
    db.check_runs_here(
        db.BundleManifest(name, "202610281400-b6900f87a", "b" * 40, required), db.running_provisa()
    )
