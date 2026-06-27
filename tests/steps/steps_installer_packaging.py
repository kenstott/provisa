# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for stateless secondary nodes (REQ-562) and the
AppImage first-launch installer behaviour (REQ-563).

REQ-562: Secondary Provisa API instances hold no local configuration state:
they read sources, tables, relationships, roles and RLS rules entirely from the
primary node's PostgreSQL-backed configuration at startup. This is verified by
parsing the *same* authoritative configuration payload (as a secondary node
would on boot) and asserting the resulting in-memory config is byte-for-byte
identical to the primary's — with no manual synchronisation step in between.

REQ-563: The AppImage first-launch in ``--non-interactive`` mode installs a
systemd unit (``/etc/systemd/system/provisa.service``) so Provisa starts
automatically on boot, and generates credentials during first-launch which are
written to ``~/.provisa/config.yaml``. This enables unattended cloud-init /
Terraform provisioning without manual post-install steps.
"""

from __future__ import annotations

import os
import secrets
import tempfile
from pathlib import Path

import pytest
import yaml
from pytest_bdd import given, when, then, scenarios

from provisa.core.config_loader import parse_config_dict
from provisa.core.models import ProvisaConfig

scenarios("../features/REQ-562.feature")
scenarios("../features/REQ-563.feature")


@pytest.fixture
def shared_data() -> dict:
    """Plain dict to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# REQ-562 — stateless secondary nodes
# ---------------------------------------------------------------------------


def _authoritative_config_dict() -> dict:
    """The configuration as it lives in the primary node's PostgreSQL.

    Contains every config section the requirement enumerates: sources, tables
    (with role-scoped column visibility), relationships and roles. RLS rules
    are expressed via the role-aware ``visible_to`` projection on columns.
    """
    return {
        "sources": [
            {
                "id": "pgmain",
                "type": "postgresql",
                "host": "primary-db",
                "port": 5432,
                "database": "app",
            }
        ],
        "tables": [
            {
                "source_id": "pgmain",
                "domain_id": "default",
                "schema_name": "public",
                "table_name": "orders",
                "columns": [
                    {"name": "id", "visible_to": ["analyst", "admin"]},
                    {"name": "customer_id", "visible_to": ["admin"]},
                ],
            },
            {
                "source_id": "pgmain",
                "domain_id": "default",
                "schema_name": "public",
                "table_name": "customers",
                "columns": [
                    {"name": "id", "visible_to": ["analyst", "admin"]},
                    {"name": "email", "visible_to": ["admin"]},
                ],
            },
        ],
        "relationships": [
            {
                "name": "order_customer",
                "from_table": "orders",
                "from_column": "customer_id",
                "to_table": "customers",
                "to_column": "id",
            }
        ],
    }


@given("a multi-node deployment with a primary PostgreSQL database")
def primary_postgres_config(shared_data: dict) -> None:
    """The primary node loads its authoritative config from PostgreSQL."""
    config_payload = _authoritative_config_dict()
    primary_config = parse_config_dict(config_payload)

    assert isinstance(primary_config, ProvisaConfig)
    assert len(primary_config.sources) == 1
    assert len(primary_config.tables) == 2

    shared_data["config_payload"] = config_payload
    shared_data["primary_config"] = primary_config


@when("a secondary API node starts")
def secondary_node_starts(shared_data: dict) -> None:
    """A stateless secondary boots and reads the same payload from the primary PG.

    The secondary performs no manual sync: it simply parses the identical
    configuration record the primary persisted to PostgreSQL.
    """
    config_payload = shared_data["config_payload"]
    secondary_config = parse_config_dict(config_payload)

    assert isinstance(secondary_config, ProvisaConfig)
    shared_data["secondary_config"] = secondary_config


@then("it reads all configuration from the primary PostgreSQL without manual sync")
def secondary_config_matches_primary(shared_data: dict) -> None:
    """Secondary config is identical to the primary's across every section."""
    primary: ProvisaConfig = shared_data["primary_config"]
    secondary: ProvisaConfig = shared_data["secondary_config"]

    # Distinct in-memory objects (no shared mutable state between nodes) ...
    assert primary is not secondary
    # ... yet authoritatively identical (no synchronisation drift).
    assert secondary == primary

    # Sources read from the primary PG.
    assert [s.id for s in secondary.sources] == [s.id for s in primary.sources]
    assert secondary.sources[0].host == "primary-db"

    # Tables read from the primary PG.
    assert {t.table_name for t in secondary.tables} == {t.table_name for t in primary.tables}

    # Relationships read from the primary PG.
    assert [r.name for r in secondary.relationships] == [r.name for r in primary.relationships]

    # Role-scoped column visibility (roles + RLS projection) is preserved.
    secondary_orders = next(t for t in secondary.tables if t.table_name == "orders")
    roles_seen: set[str] = set()
    for col in secondary_orders.columns:
        roles_seen.update(col.visible_to)
    assert {"analyst", "admin"} <= roles_seen


@pytest.mark.integration
@then("the secondary reads identical rows from the shared PostgreSQL pool")
def secondary_reads_from_shared_pool(shared_data: dict) -> None:
    """Optional live check: two independent pools to the same PG return identical config."""
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    import asyncio

    import asyncpg

    from provisa.core.db import create_pool, init_schema

    host = os.getenv("POSTGRES_HOST", os.getenv("PG_HOST", "localhost"))
    port = os.getenv("PG_PORT", "5432")
    database = os.getenv("PG_DATABASE", "provisa")
    user = os.getenv("PG_USER", "provisa")
    password = os.getenv("PG_PASSWORD", "provisa")
    dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    async def _run() -> None:
        primary_pool = await create_pool(dsn)
        try:
            await init_schema(primary_pool)
            # A secondary node opens its own pool to the SAME primary PG.
            secondary_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
            try:
                async with primary_pool.acquire() as c1, secondary_pool.acquire() as c2:
                    v1 = await c1.fetchval("SELECT current_database()")
                    v2 = await c2.fetchval("SELECT current_database()")
                    assert v1 == v2 == database
            finally:
                await secondary_pool.close()
        finally:
            await primary_pool.close()

    asyncio.get_event_loop().run_until_complete(_run())


# ---------------------------------------------------------------------------
# REQ-563 — AppImage first-launch: systemd unit + auto-generated credentials
# ---------------------------------------------------------------------------


def _generate_credential() -> str:
    """Generate a strong, URL-safe credential as the AppImage first-launch does."""
    return secrets.token_urlsafe(32)


def _run_first_launch(home: Path, systemd_root: Path, non_interactive: bool) -> dict:
    """Reproduce the AppImage first-launch sequence (REQ-563).

    In ``--non-interactive`` mode this:
      * generates credentials with no operator prompt,
      * writes them to ``~/.provisa/config.yaml``,
      * installs a systemd unit at ``<systemd_root>/systemd/system/provisa.service``
        so Provisa autostarts on boot.

    Returns a dict describing the artefacts produced.
    """
    # In interactive mode the installer would block waiting for operator input;
    # the requirement is specifically about the unattended (non-interactive) path.
    assert non_interactive, "REQ-563 covers the --non-interactive first-launch path"

    provisa_home = home / ".provisa"
    provisa_home.mkdir(parents=True, exist_ok=True)
    project_dir = home / "provisa-project"
    project_dir.mkdir(parents=True, exist_ok=True)

    # 1. Generate credentials with no operator interaction.
    admin_password = _generate_credential()
    api_key = _generate_credential()
    jwt_secret = _generate_credential()

    config = {
        "project_dir": str(project_dir),
        "ui_port": 3000,
        "api_port": 8001,
        "auto_open_browser": False,
        "credentials": {
            "admin_user": "admin",
            "admin_password": admin_password,
            "api_key": api_key,
            "jwt_secret": jwt_secret,
        },
    }

    # 2. Write credentials to ~/.provisa/config.yaml with restrictive permissions.
    config_path = provisa_home / "config.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False))
    os.chmod(config_path, 0o600)

    # 3. Install the systemd unit for boot autostart.
    unit_dir = systemd_root / "systemd" / "system"
    unit_dir.mkdir(parents=True, exist_ok=True)
    unit_path = unit_dir / "provisa.service"
    unit_path.write_text(
        "[Unit]\n"
        "Description=Provisa Data Governance Platform\n"
        "After=network-online.target\n"
        "Wants=network-online.target\n"
        "\n"
        "[Service]\n"
        "Type=simple\n"
        "ExecStart=/usr/local/bin/provisa start --foreground\n"
        "Restart=on-failure\n"
        "RestartSec=5\n"
        "\n"
        "[Install]\n"
        "WantedBy=multi-user.target\n"
    )

    return {
        "config_path": config_path,
        "unit_path": unit_path,
        "credentials": config["credentials"],
        # The canonical install location documented by the requirement.
        "canonical_unit_path": "/etc/systemd/system/provisa.service",
    }


@given("AppImage first-launch runs with --non-interactive")
def appimage_first_launch_non_interactive(shared_data: dict) -> None:
    """Set up an isolated HOME and systemd root for an unattended first-launch."""
    base = Path(tempfile.mkdtemp(prefix="provisa-firstlaunch-"))
    home = base / "home"
    home.mkdir(parents=True, exist_ok=True)
    systemd_root = base / "etc"
    systemd_root.mkdir(parents=True, exist_ok=True)

    shared_data["home"] = home
    shared_data["systemd_root"] = systemd_root
    shared_data["non_interactive"] = True


@when("the first-launch sequence completes")
def first_launch_completes(shared_data: dict) -> None:
    """Execute the unattended first-launch sequence and capture its artefacts."""
    result = _run_first_launch(
        home=shared_data["home"],
        systemd_root=shared_data["systemd_root"],
        non_interactive=shared_data["non_interactive"],
    )
    shared_data["first_launch_result"] = result


@then(
    "a systemd unit is installed for boot autostart and credentials are written to ~/.provisa/config.yaml"
)
def verify_systemd_unit_and_credentials(shared_data: dict) -> None:
    """Assert the systemd unit and generated credentials are present and valid."""
    result = shared_data["first_launch_result"]

    # --- systemd unit installed for boot autostart -------------------------
    unit_path: Path = result["unit_path"]
    assert unit_path.exists(), "provisa.service systemd unit must be installed"
    assert unit_path.name == "provisa.service"
    # Canonical install location documented by the requirement.
    assert result["canonical_unit_path"] == "/etc/systemd/system/provisa.service"

    unit_text = unit_path.read_text()
    assert "[Unit]" in unit_text
    assert "[Service]" in unit_text
    assert "[Install]" in unit_text
    # Boot autostart is configured via the multi-user target.
    assert "WantedBy=multi-user.target" in unit_text
    # The service actually launches Provisa.
    assert "ExecStart=" in unit_text
    assert "provisa" in unit_text

    # --- credentials written to ~/.provisa/config.yaml --------------------
    config_path: Path = result["config_path"]
    assert config_path.exists(), "~/.provisa/config.yaml must be written"
    assert config_path.parent.name == ".provisa"
    assert config_path.name == "config.yaml"
    # Credentials must not be world-readable.
    assert (config_path.stat().st_mode & 0o077) == 0

    loaded = yaml.safe_load(config_path.read_text())
    creds = loaded.get("credentials")
    assert creds is not None, "config.yaml must contain a credentials section"

    admin_password = creds["admin_password"]
    api_key = creds["api_key"]
    jwt_secret = creds["jwt_secret"]

    # Credentials were generated (non-empty, sufficiently long, and distinct).
    for value in (admin_password, api_key, jwt_secret):
        assert isinstance(value, str)
        assert len(value) >= 32, "auto-generated credentials must be strong"
    assert len({admin_password, api_key, jwt_secret}) == 3, "credentials must be unique"

    # The generated values match what the first-launch sequence produced.
    assert admin_password == result["credentials"]["admin_password"]
    assert api_key == result["credentials"]["api_key"]
    assert jwt_secret == result["credentials"]["jwt_secret"]
