# Copyright (c) 2026 Kenneth Stott
# Canary: 837b8ef4-e858-460c-af05-d02aec3e76e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An AskAmerica (US government data) source: a Postgres-wire source like its sibling adapters.

The operator gives one thing, the API key (REQ-540). Everything else follows from it:

* The key is presented to AskAmerica's API, which answers with the short-lived, read-only
  object-store credentials the data is read with (``resolve_storage_credentials``) — the one
  place the key is turned into connection details. What it returns is held in memory and handed
  to the adapter's process; it is never written to config or to a log.
* The Postgres endpoint is the source's bundled ``pgwire-govdata`` server, started and reached
  exactly as ``pgwire-sharepoint`` / ``-salesforce`` / ``-cloudops`` are
  (``federation.pgwire_replica``). The server renews its own credentials before they expire,
  from the key it is started with.

The server is the installed bundle, started by its own launcher as it was installed: its own
model, its own prebuilt catalog beside it. Provisa writes nothing into it and rewrites nothing
of it; it hands the launcher a port and the environment above.

A source lists the schemas it offers (``sec``, ``econ``, ...) in ``database``, as the Sources
form writes them. The adapter serves every schema it has; narrowing to the source's list is
Provisa's own — what the Register Table form lists and what a table may be registered from
(:func:`require_schema_served`). One AskAmerica source runs from the one installed bundle.
"""

from __future__ import annotations

import hashlib
import io
import time
import zipfile
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from typing import Any, Callable

from provisa.core.secrets import resolve_secrets

# Requirements: REQ-492, REQ-540

#: AskAmerica's API: where a key is exchanged for the credentials its data is read with.
CREDENTIALS_URL = "https://api.askamerica.ai/v1/catalog/credentials"
#: The API sits behind a bot filter that refuses a client library's default User-Agent.
USER_AGENT = "provisa-askamerica/1.0"
REQUEST_SECONDS = 10.0


class AskAmericaKeyMissing(ValueError):
    """The source has no API key."""

    def __init__(self, source_id: str) -> None:
        super().__init__(f"AskAmerica source {source_id!r}: an API key is required")
        self.source_id = source_id


class AskAmericaKeyRefused(PermissionError):
    """AskAmerica refused the API key: unknown, expired or revoked."""

    def __init__(self, detail: str) -> None:
        super().__init__(f"AskAmerica refused the API key: {detail}")
        self.detail = detail


class AskAmericaUnavailable(RuntimeError):
    """AskAmerica's API did not answer with credentials (not a refusal of the key)."""


@dataclass(frozen=True)
class StorageCredentials:
    """What an API key resolves to: where the data is and the credentials that read it."""

    access_key_id: str
    secret_access_key: str
    session_token: str
    region: str
    endpoint: str
    bucket: str
    expires_at_millis: int

    def __repr__(self) -> str:  # never printed with its secrets
        return f"StorageCredentials(bucket={self.bucket!r}, expires_at_millis={self.expires_at_millis})"


def _fetch(api_key: str) -> tuple[int, dict]:
    import httpx

    response = httpx.get(
        CREDENTIALS_URL,
        headers={"X-API-Key": api_key, "User-Agent": USER_AGENT},
        timeout=REQUEST_SECONDS,
    )
    return response.status_code, response.json()


def resolve_storage_credentials(
    api_key: str, *, fetch: Callable[[str], tuple[int, dict]] | None = None
) -> StorageCredentials:
    """Exchange ``api_key`` for the credentials its data is read with. A refused key is
    ``AskAmericaKeyRefused``; any other answer without credentials is ``AskAmericaUnavailable``."""
    status, body = (fetch if fetch is not None else _fetch)(api_key)
    if status in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
        raise AskAmericaKeyRefused(str(body.get("error", status)))
    if status != HTTPStatus.OK:
        raise AskAmericaUnavailable(
            f"AskAmerica's credentials API answered {status}: {body.get('error', 'no detail')}"
        )
    return StorageCredentials(
        access_key_id=body["access_key_id"],
        secret_access_key=body["secret_access_key"],
        session_token=body["session_token"],
        region=body["region"],
        endpoint=body["endpoint"],
        bucket=body["bucket"],
        expires_at_millis=int((time.time() + int(body["expires_in"])) * 1000),
    )


def api_key(source: Any) -> str:
    """The source's API key, its secret reference resolved. The Sources form keeps it in
    ``username``."""
    key = resolve_secrets(source.username) if source.username else ""
    if not key:
        raise AskAmericaKeyMissing(source.id)
    return key


def schemas(source: Any) -> list[str]:
    """The adapter schemas the source serves: its comma-separated ``database`` list."""
    return [s.strip().lower() for s in (source.database or "").split(",") if s.strip()]


# -- the bundle's seed: its prebuilt DuckDB catalog and conversion records -----------------------
#
# The bundle ships, inside its govdata jar, what the adapter would otherwise build at every
# start by discovering each table from object storage (minutes): the DuckDB catalog (view
# definitions only, no data), one conversion record per schema, and the Iceberg schema cache.
# The adapter's own installer (GovDataSeedInstaller.ensureSeeded) lays them down, but it is
# called only from the adapter's embedded JDBC driver; a pgwire server opens its model through
# the schema factory and never reaches it, and no property makes it. So installing the bundle
# installs its seed here, laid down exactly as that installer does, and the server is started on
# it (``GOVDATA_DUCKDB_CATALOG``).

#: Inside the bundle's govdata jar.
SEED_ZIP_RESOURCE = "duckdb/seed/govdata-seed.zip"
SCHEMA_CACHE_RESOURCE = "duckdb/seed/iceberg-schema-cache.json"
#: Relative to the seed's base, as the adapter's installer has them.
SEED_CATALOG = ".duckdb/govdata.duckdb"
SEED_MARKER = ".duckdb/govdata.duckdb.version"
#: Where the adapter keeps its Iceberg schema cache, and the marker of the bundled copy last
#: installed there (IcebergSchemaCache.cacheDir / installBundled).
SCHEMA_CACHE_NAME = "iceberg-schema-cache.json"


class BundleSeedMissing(RuntimeError):
    """The pgwire-govdata bundle carries no seed: a packaging defect of that bundle. Its server
    is not started, since it would build its whole catalog from object storage at every start."""

    def __init__(self, bundle_dir: Path, why: str) -> None:
        super().__init__(
            f"the pgwire-govdata bundle at {bundle_dir} has no usable seed ({why}): its server "
            f"is not started. The bundle's govdata jar must carry {SEED_ZIP_RESOURCE}"
        )
        self.bundle_dir = bundle_dir


def seed_base(bundle_dir: Path) -> Path:
    """Where the seed is laid down: the directory of the bundle's model, which is the base the
    server keeps each schema's working directory under (``<base>/.aperio/<schema>``)."""
    return Path(bundle_dir) / "model"


def _schema_cache_dir() -> Path:
    return Path.home() / ".aperio" / ".iceberg_metadata_cache"


def _seed_jar(bundle_dir: Path) -> zipfile.ZipFile:
    jars = sorted((Path(bundle_dir) / "jars").glob("*govdata*.jar"))
    for jar in jars:
        archive = zipfile.ZipFile(jar)
        if SEED_ZIP_RESOURCE in archive.namelist():
            return archive
        archive.close()
    raise BundleSeedMissing(
        Path(bundle_dir), f"none of {[j.name for j in jars]} under jars/ carries it"
    )


def install_seed(bundle_dir: Path, *, schema_cache_dir: Path | None = None) -> Path:
    """Lay the bundle's seed down and return the catalog the server is started on.

    As the adapter's installer does: the seed is extracted when the marker beside the catalog
    does not hold the seed's SHA-256, or the catalog is gone — so a new bundle release, whose
    seed differs, is seeded afresh, and an installed one is left alone. Extraction replaces what
    is there (a catalog and records the server built for itself before the seed was installed
    are not a seed: they carry no marker) and discards a write-ahead log left beside a replaced
    catalog, which belongs to the catalog just replaced. A bundle with no seed is refused."""
    base = seed_base(bundle_dir)
    catalog, marker = base / SEED_CATALOG, base / SEED_MARKER
    with _seed_jar(bundle_dir) as jar:
        seed = jar.read(SEED_ZIP_RESOURCE)
        fingerprint = hashlib.sha256(seed).hexdigest()
        if not (catalog.is_file() and marker.is_file() and marker.read_text() == fingerprint):
            _extract_seed(seed, base, Path(bundle_dir))
            if not catalog.is_file():
                raise BundleSeedMissing(Path(bundle_dir), f"its seed holds no {SEED_CATALOG}")
            marker.write_text(fingerprint)
        if SCHEMA_CACHE_RESOURCE in jar.namelist():
            _install_schema_cache(
                jar.read(SCHEMA_CACHE_RESOURCE),
                schema_cache_dir if schema_cache_dir is not None else _schema_cache_dir(),
            )
    return catalog


def _extract_seed(seed: bytes, base: Path, bundle_dir: Path) -> None:
    root = base.resolve()
    with zipfile.ZipFile(io.BytesIO(seed)) as archive:
        for entry in archive.infolist():
            target = (base / entry.filename).resolve()
            if root != target and root not in target.parents:
                raise BundleSeedMissing(
                    bundle_dir, f"its seed entry {entry.filename!r} leaves the seed directory"
                )
            if entry.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(entry))
            if target.suffix == ".duckdb":
                target.with_name(target.name + ".wal").unlink(missing_ok=True)


def _install_schema_cache(cache: bytes, cache_dir: Path) -> None:
    """The bundled Iceberg schema cache, where the adapter reads it. As the adapter's
    ``IcebergSchemaCache.installBundled``: kept when the copy there is the bundled one already
    (its marker holds this copy's MD5 — the adapter's own marker, hence its digest)."""
    target = cache_dir / SCHEMA_CACHE_NAME
    marker = cache_dir / (SCHEMA_CACHE_NAME + ".bundled")
    digest = hashlib.md5(cache, usedforsecurity=False).hexdigest()
    if target.is_file() and marker.is_file() and marker.read_text() == digest:
        return
    cache_dir.mkdir(parents=True, exist_ok=True)
    target.write_bytes(cache)
    marker.write_text(digest)


def server_environment(
    source: Any,
    *,
    catalog: Path,
    resolve: Callable[[str], StorageCredentials] | None = None,
) -> dict[str, str]:
    """The environment the source's ``pgwire-govdata`` server is started with: the key (the
    server meters with it and renews its credentials from it), the credentials the key resolves
    to now, where the data is, and the seeded ``catalog`` it opens (:func:`install_seed`) —
    without which it builds one from object storage. The bundle's model reads each by name."""
    key = api_key(source)
    creds = (resolve if resolve is not None else resolve_storage_credentials)(key)
    return {
        "ASKAMERICA_API_KEY": key,
        "GOVDATA_DUCKDB_CATALOG": str(catalog),
        "GOVDATA_PARQUET_DIR": f"s3://{creds.bucket}",
        "AWS_ACCESS_KEY_ID": creds.access_key_id,
        "AWS_SECRET_ACCESS_KEY": creds.secret_access_key,
        "AWS_SESSION_TOKEN": creds.session_token,
        "AWS_ENDPOINT_OVERRIDE": creds.endpoint,
        "AWS_REGION": creds.region,
        "AWS_CREDENTIALS_EXPIRES_AT_MILLIS": str(creds.expires_at_millis),
    }


class SchemaNotServed(ValueError):
    """A schema named for an AskAmerica source that is not among the schemas the source was
    given. The adapter serves every schema it has; which of them a source offers is the
    source's own list, kept and enforced here."""

    def __init__(self, source_id: str, schema: str, served: list[str]) -> None:
        super().__init__(
            f"AskAmerica source {source_id!r} does not serve schema {schema!r}: its subjects "
            f"bring {served}"
        )
        self.source_id = source_id
        self.schema = schema


def serves_schema(source: Any, schema: str) -> bool:
    """Whether ``schema`` is one of the schemas the source was given (:func:`schemas`)."""
    return schema.strip().lower() in schemas(source)


def require_schema_served(source: Any, schema: str) -> None:
    """Refuse, by name, a schema outside the source's list."""
    if not serves_schema(source, schema):
        raise SchemaNotServed(source.id, schema, schemas(source))
