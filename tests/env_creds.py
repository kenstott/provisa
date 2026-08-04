# Copyright (c) 2026 Kenneth Stott
# Canary: 1ad78d3b-3ec1-4100-821f-75b3c989ab1e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Load live external-provider credentials from .env into the pytest process.

This lives in pytest, NOT in scripts/test-all, because the cloud-DW e2es gate on
`os.environ` inside module-level `@pytest.mark.skipif` conditions that are evaluated at
COLLECTION time. When only the bash wrapper loaded .env, any other invocation -- a bare
`pytest tests/integration`, an IDE run, a single-file rerun -- collected those tests
credential-less and reported them as skipped. The credentials were sitting in .env the
whole time; the run just never saw them, so a full green suite silently hid 20 unexecuted
cloud tests. Loading here means every invocation uses the creds that exist.

Whitelist, not a whole-file source: .env also holds POSTGRES_HOST / PG_* /
PROVISA_REDIRECT_* / PROVISA_ENGINE*, which would repoint the isolated Docker stack at
the developer's dev environment and break the config-default tests. Only external-SaaS
provider prefixes load. An already-exported value always wins, so a caller can override.
"""

import os
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _REPO_ROOT / ".env"

# External-SaaS provider prefixes only. Anything naming a host/port/URL of the local stack
# is deliberately absent -- see the module docstring.
_CRED_PREFIXES = (
    "SNOWFLAKE_",
    "DATABRICKS_",
    "GOOGLE_",
    "BIGQUERY_",
    "FABRIC_",
    "SYNAPSE_",
    "CLICKHOUSE_",
    "SP_",
    "REDSHIFT_",
    # R2 object staging (Cloudflare S3-compatible). The Databricks bulk COPY-INTO, Databricks
    # external-link, and Fabric OPENROWSET tests gate on AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
    # AWS_ENDPOINT_OVERRIDE / CLOUDFLARE_ACCOUNT_ID. These do NOT repoint the local stack: the
    # isolated MinIO is addressed through MINIO_ENDPOINT / PROVISA_OTEL_S3_ENDPOINT, and govdata
    # source registration overwrites AWS_* from its own registered credential
    # (provisa/api/admin/schema_common.py::_configure_govdata_env).
    "AWS_",
    "CLOUDFLARE_",
    # The gsheets e2e READS a durable fixture sheet rather than creating one -- the service account
    # has zero Drive quota and cannot self-seed. Its id is the external prerequisite.
    "GSHEETS_",
)


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        key = key.strip()
        if key.startswith("export "):
            key = key[len("export ") :].strip()
        if not key.startswith(_CRED_PREFIXES):
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        values[key] = value
    return values


def load_provider_creds() -> list[str]:
    """Export external-provider creds from .env. Returns the names it set.

    Called at import of tests/conftest.py so the values are present before any module-level
    skipif condition is evaluated.
    """
    if not _ENV_FILE.is_file():
        return []

    loaded: list[str] = []
    for key, value in _parse_env_file(_ENV_FILE).items():
        if os.environ.get(key):
            continue  # a caller-exported value always wins
        os.environ[key] = value
        loaded.append(key)

    # .env authors the SharePoint Azure AD app under the SP_ prefix, but the sharepoint e2e
    # reads SHAREPOINT_* (its own naming). Bridge the names so a credential that IS present
    # is actually used instead of skipping on a name mismatch. certificate_path is authored
    # relative (./sharepoint.pfx); resolve it so the test works from any cwd.
    for sp_key, sharepoint_key in (
        ("SP_SITE_URL", "SHAREPOINT_SITE_URL"),
        ("SP_TENANT_ID", "SHAREPOINT_TENANT_ID"),
        ("SP_CLIENT_ID", "SHAREPOINT_CLIENT_ID"),
        ("SP_CLIENT_SECRET", "SHAREPOINT_CLIENT_SECRET"),
        ("SP_CERT_PASSWORD", "SHAREPOINT_CERT_PASSWORD"),
    ):
        value = os.environ.get(sp_key)
        if value and not os.environ.get(sharepoint_key):
            os.environ[sharepoint_key] = value
            loaded.append(sharepoint_key)
    cert = os.environ.get("SP_CERT_PATH")
    if cert and not os.environ.get("SHAREPOINT_CERT_PATH"):
        os.environ["SHAREPOINT_CERT_PATH"] = str((_REPO_ROOT / cert).resolve())
        loaded.append("SHAREPOINT_CERT_PATH")

    # The Google Sheets API is enabled for the credentialed project and the key_file secret
    # works, so the live DuckDB gsheets read must RUN whenever those creds loaded rather than
    # phantom-skip on its own opt-in flag.
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") and not os.environ.get(
        "PROVISA_GSHEETS_LIVE"
    ):
        os.environ["PROVISA_GSHEETS_LIVE"] = "1"
        loaded.append("PROVISA_GSHEETS_LIVE")

    return loaded
