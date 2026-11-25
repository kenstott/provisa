# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A one-node Spark reads a governed Provisa table through the provisa data source's JVM jar
(REQ-2270), as tests/integration/test_spark_reads_e2e.py reads it through provisa-pyspark.

Maven builds provisa-spark-jar (its JUnit tests first) into the shaded jar that ships; Spark
(provisa-pyspark's locked pyspark, nothing of the Python package) runs in local mode with that jar
on its class path, and ``spark.read.format("provisa")`` resolves to the jar's DataSourceRegister.
A real server (one worker, simple auth, a database of its own) serves sales.orders with a row rule
and a masked column for the role clerk; a signed-in clerk's session token is the credential. The
DataFrame holds only the rows the rule allows, the masked column arrives masked, the columns are
the declared types, and the read is audited as the user.

Needs ``mvn`` and a JDK 21 on the PATH (the core lane's job sets them up). Lands on the TEST
instance only: one server over a database the harness creates."""

# Requirements: REQ-2270

from __future__ import annotations

import json
import os
import shutil
import subprocess
import textwrap
import time
import urllib.request
from pathlib import Path

import pytest
import sqlalchemy as sa

from tests.integration.test_spark_reads_e2e import _PASSWORD, _model, _user_id
from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ROOT = Path(__file__).resolve().parents[2]
_PROJECT = _ROOT / "provisa-spark-jar"
_PYSPARK_LOCK = _ROOT / "provisa-pyspark"

# One read, printed as one JSON line: the DataFrame's columns and rows, or the error Spark raised.
_READ = textwrap.dedent(
    """
    import json
    import sys

    from pyspark.sql import SparkSession

    jar, uri, token, table = sys.argv[1:5]
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.jars", jar)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    reader = spark.read.format("provisa").option("uri", uri)
    if token:
        reader = reader.option("token", token)
    try:
        frame = reader.load(table)
        out = {
            "columns": [[f.name, f.dataType.simpleString()] for f in frame.schema.fields],
            "rows": [row.asDict() for row in frame.orderBy("id").collect()],
            "plan": frame._jdf.queryExecution().executedPlan().toString(),
        }
    except Exception as exc:
        out = {"error": f"{type(exc).__name__}: {exc}"}
    finally:
        spark.stop()
    print(json.dumps(out, default=str))
    """
)


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


def _cached_repository() -> list[str]:
    """Maven's read-only tail (3.9): what the machine's ~/.m2 already holds resolves from there
    and is never written. Absent on a machine that has no ~/.m2, where Maven fetches it all."""
    cached = Path.home() / ".m2" / "repository"
    return [f"-Dmaven.repo.local.tail={cached}"] if cached.is_dir() else []


@pytest.fixture(scope="module")
def jar(tmp_path_factory) -> Path:
    """The shaded jar, built by Maven after its JUnit tests pass."""
    run = subprocess.run(
        [
            "mvn",
            "-B",
            # Its own local repository: a test must not write to a directory it does not own.
            f"-Dmaven.repo.local={tmp_path_factory.mktemp('m2')}",
            *_cached_repository(),
            "-f",
            str(_PROJECT / "pom.xml"),
            "package",
        ],
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    output = run.stdout[-6000:] + run.stderr[-2000:]
    assert run.returncode == 0, output
    assert "Tests run: 120, Failures: 0, Errors: 0, Skipped: 0" in run.stdout, output
    (built,) = (_PROJECT / "target").glob("provisa-spark_2.13-*[0-9].jar")
    return built


@pytest.fixture(scope="module")
def spark_python(tmp_path_factory) -> Path:
    """A Python holding provisa-pyspark's locked Spark (pyspark, pyarrow), without the package:
    the read is the jar's."""
    uv = shutil.which("uv")
    assert uv, "uv is on the PATH of a test run"
    work = tmp_path_factory.mktemp("spark-jar-reads")
    requirements = work / "requirements.txt"
    subprocess.run(
        [uv, "export", "--project", str(_PYSPARK_LOCK), "--frozen", "--extra", "spark",
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
    return python


def _read(server, spark_python: Path, jar: Path, token: str) -> dict:
    (home,) = sorted((spark_python.parent.parent / "lib").glob("python*/site-packages/pyspark"))
    env = {
        **os.environ,
        "PYSPARK_PYTHON": str(spark_python),
        "PYSPARK_DRIVER_PYTHON": str(spark_python),
        # Its own distribution, whatever SPARK_HOME the runner carries.
        "SPARK_HOME": str(home),
    }
    result = subprocess.run(
        [str(spark_python), "-I", "-c", _READ, str(jar),
         f"grpc://127.0.0.1:{server.ports['flight']}", token, "sales.orders"],
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )  # fmt: skip
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr[-6000:]}"
    return json.loads(result.stdout.strip().splitlines()[-1])


@pytest.fixture(scope="module")
def clerk_read(server, spark_python, jar, token) -> dict:
    out = _read(server, spark_python, jar, token)
    assert "error" not in out, out
    return out


def test_the_read_is_the_jars_columnar_scan(clerk_read):
    # The DataSource V2 scan of the jar's ProvisaTable, its batches columnar.
    assert "BatchScan sales.orders" in clerk_read["plan"], clerk_read["plan"]
    assert "provisa sales.orders" in clerk_read["plan"], clerk_read["plan"]
    assert "ColumnarToRow" in clerk_read["plan"], clerk_read["plan"]


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


def test_a_read_without_the_credential_is_refused(server, spark_python, jar):
    out = _read(server, spark_python, jar, "")
    assert "error" in out, out
    assert "a bearer credential is required" in out["error"], out["error"]
