# Copyright (c) 2026 Kenneth Stott
# Canary: 0f8f5155-0930-4b06-9bfa-ebf2f2051040
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A one-node Spark reads a governed Provisa table through the provisa data source (REQ-2270).

A real server (one worker, simple auth, a database of its own) serves sales.orders with a row rule
and a masked column for the role clerk; a signed-in clerk's session token is the data source's
credential. Spark runs in local mode from provisa-pyspark's locked dependencies, its Python
workers on that same interpreter. The DataFrame holds only the rows the rule allows, the masked
column arrives masked, the columns are the declared types, and the read is audited as the user.

Lands on the TEST instance only: one server over a database the harness creates."""

# Requirements: REQ-2270

from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import textwrap
import time
import urllib.request
from pathlib import Path

import bcrypt
import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_PACKAGE = Path(__file__).resolve().parents[2] / "provisa-pyspark"
_PASSWORD = "correct horse"

# One read, printed as one JSON line: the DataFrame's columns and rows, or the error Spark raised.
_READ = textwrap.dedent(
    """
    import json
    import sys

    from pyspark.sql import SparkSession

    from provisa_pyspark import ProvisaDataSource

    uri, token, table = sys.argv[1:4]
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.dataSource.register(ProvisaDataSource)
    reader = spark.read.format("provisa").option("uri", uri)
    if token:
        reader = reader.option("token", token)
    try:
        frame = reader.load(table)
        out = {
            "columns": [[f.name, f.dataType.simpleString()] for f in frame.schema.fields],
            "rows": [row.asDict() for row in frame.orderBy("id").collect()],
        }
    except Exception as exc:
        out = {"error": f"{type(exc).__name__}: {exc}"}
    finally:
        spark.stop()
    print(json.dumps(out, default=str))
    """
)


def _model() -> dict:
    hashed = bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt()).decode()
    return {
        "auth": {
            "provider": "simple",
            "allow_simple_auth": True,
            "jwt_secret": "spark-reads-test-signing-key-of-some-length",
            "default_role": "clerk",
            "simple": {
                "users": [{"username": "casey", "password_hash": hashed, "roles": ["clerk"]}]
            },
        },
        "tables": [
            {
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "columns": [
                    {"name": "id", "data_type": "integer", "visible_to": ["clerk"]},
                    {"name": "region", "data_type": "varchar", "visible_to": ["clerk"]},
                    {"name": "amount", "data_type": "numeric(10,2)", "visible_to": ["clerk"]},
                    {
                        "name": "email",
                        "data_type": "varchar",
                        "visible_to": ["clerk"],
                        "masks": [
                            {
                                "to": ["clerk"],
                                "mask": {"type": "regex", "pattern": "^[^@]+", "replace": "***"},
                            },
                            {"mask": None},
                        ],
                    },
                ],
            }
        ],
        # org_admin is the reserved administrative role (REQ-1349): not declared.
        "roles": [
            {
                "id": "clerk",
                "description": "Reads the east region's orders, emails masked.",
                "capabilities": ["query_development", "full_results"],
                "domain_access": ["sales"],
            }
        ],
        "rls_rules": [
            {
                "description": "A clerk reads the east region only.",
                "table_id": "orders",
                "role_id": "clerk",
                "filter": "region = 'east'",
            }
        ],
    }


@pytest.fixture(scope="module")
def server():
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config=_model(),
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
    try:
        with own.connect() as conn:
            # create_database seeds (1, 'east') and (2, 'west'); a third row, east, beside them.
            conn.execute(
                sa.text(
                    "ALTER TABLE public.orders ADD COLUMN amount numeric(10,2), ADD COLUMN email text"
                )
            )
            for statement in (
                "UPDATE public.orders SET amount = 19.99, email = 'ann@example.com' WHERE id = 1",
                "UPDATE public.orders SET amount = 7.50, email = 'wes@example.com' WHERE id = 2",
                "INSERT INTO public.orders VALUES (3, 'east', 5.00, 'bob@example.com')",
            ):
                conn.execute(sa.text(statement))
    finally:
        own.dispose()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


@pytest.fixture(scope="module")
def token(server) -> str:
    req = urllib.request.Request(
        f"http://127.0.0.1:{server.ports['http']}/auth/login",
        data=json.dumps({"username": "casey", "password": _PASSWORD}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())["access_token"]


@pytest.fixture(scope="module")
def spark_python(tmp_path_factory) -> Path:
    """A Python holding provisa-pyspark's locked Spark (pyspark, pyarrow) and the package."""
    uv = shutil.which("uv")
    assert uv, "uv is on the PATH of a test run"
    work = tmp_path_factory.mktemp("spark-reads")
    requirements = work / "requirements.txt"
    subprocess.run(
        [uv, "export", "--project", str(_PACKAGE), "--frozen", "--extra", "spark",
         "--no-emit-project", "--format", "requirements-txt", "--output-file", str(requirements)],
        check=True,
    )  # fmt: skip
    venv = work / "venv"
    subprocess.run([uv, "venv", "--python", "3.12", str(venv)], check=True)
    python = venv / "bin" / "python"
    subprocess.run(
        [
            uv,
            "pip",
            "install",
            "--python",
            str(python),
            "--require-hashes",
            "-r",
            str(requirements),
        ],
        check=True,
    )
    subprocess.run(
        [uv, "pip", "install", "--python", str(python), "--no-deps", str(_PACKAGE)], check=True
    )
    return python


def _read(server, spark_python: Path, token: str) -> dict:
    (home,) = sorted((spark_python.parent.parent / "lib").glob("python*/site-packages/pyspark"))
    env = {
        **os.environ,
        # Spark's Python workers on the interpreter holding the package; its own distribution,
        # whatever SPARK_HOME the runner carries.
        "PYSPARK_PYTHON": str(spark_python),
        "PYSPARK_DRIVER_PYTHON": str(spark_python),
        "SPARK_HOME": str(home),
    }
    result = subprocess.run(
        [str(spark_python), "-I", "-c", _READ,
         f"grpc://127.0.0.1:{server.ports['flight']}", token, "sales.orders"],
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )  # fmt: skip
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr[-6000:]}"
    return json.loads(result.stdout.strip().splitlines()[-1])


@pytest.fixture(scope="module")
def clerk_read(server, spark_python, token) -> dict:
    out = _read(server, spark_python, token)
    assert "error" not in out, out
    return out


def _user_id(token: str) -> str:
    """The signed-in user's id: the session token's subject."""
    payload = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))["sub"]


def test_the_dataframe_holds_only_the_rows_the_rule_allows_with_the_mask_applied(clerk_read):
    assert clerk_read["rows"] == [
        {"id": 1, "region": "east", "amount": "19.99", "email": "***@example.com"},
        {"id": 3, "region": "east", "amount": "5.00", "email": "***@example.com"},
    ]


def test_the_columns_are_the_declared_types(clerk_read):
    assert clerk_read["columns"] == [
        ["id", "int"],
        ["region", "string"],
        ["amount", "decimal(10,2)"],
        ["email", "string"],
    ]


def test_the_read_is_audited_as_the_user_and_its_role(server, token, clerk_read):  # noqa: ARG001  # the read must have run
    engine = sa.create_engine(server.url)
    try:
        with engine.connect() as conn:
            schema = conn.execute(
                sa.text(
                    "SELECT table_schema FROM information_schema.tables "
                    "WHERE table_name = 'query_audit_log' AND table_schema LIKE 'org%' LIMIT 1"
                )
            ).scalar_one()
        deadline = time.monotonic() + 60
        while True:
            with engine.connect() as conn:
                row = conn.execute(
                    sa.text(
                        f"SELECT user_id, role_id FROM {schema}.query_audit_log "
                        "WHERE source = 'flight' ORDER BY id DESC LIMIT 1"
                    )
                ).first()
            if row is not None:
                break
            assert time.monotonic() < deadline, "no flight audit row was written"
            time.sleep(0.5)
    finally:
        engine.dispose()
    assert dict(row._mapping) == {"user_id": _user_id(token), "role_id": "clerk"}


def test_a_read_without_the_credential_is_refused(server, spark_python):
    out = _read(server, spark_python, "")
    assert "error" in out, out
    assert "a bearer credential is required" in out["error"], out["error"]
