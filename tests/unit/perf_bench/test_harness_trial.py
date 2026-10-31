# Copyright (c) 2026 Kenneth Stott
# Canary: 8992d523-0a77-4827-b716-6c86600ec629
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The trial reports what its cells did, and its stack boots with no hand step."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import harness_trial as ht  # noqa: E402

PORTS = {
    "bench-postgresql": 21100,
    "bench-clickhouse": 21101,
    "bench-mongodb": 21102,
    "neo4j-http": 21103,
    "neo4j-bolt": 21104,
    "trino": 21105,
    "minio": 21106,
    "minio-console": 21107,
    "zaychik": 21108,
    "mcp": 21109,
}


def _cell(query: str, transport: str, rows: float = 10, errors: int = 0) -> dict[str, Any]:
    return {
        "query_id": query,
        "category": "c",
        "transport": transport,
        "dialect": "sql",
        "cache_pass": "nocache",
        "n": 3,
        "errors": errors,
        "first_error": "boom" if errors else None,
        "total_rows": rows * 3,
    }


def _report(*cells: dict[str, Any]) -> dict[str, Any]:
    return {"results": list(cells), "transports_tested": sorted(ht.TRANSPORTS)}


def _all_failed(query: str, transport: str) -> dict[str, Any]:
    return {
        "query_id": query,
        "category": "c",
        "transport": transport,
        "dialect": "sql",
        "cache_pass": "nocache",
        "errors": 3,
        "first_error": 'extension "pg_clickhouse" is not available',
        "status": "all_failed",
    }


def test_a_clean_report_is_a_passed_engine() -> None:
    verdict = ht.matrix_verdict(_report(_cell("a", "sql"), _cell("a", "flight")))
    assert not ht.engine_failed(verdict)


def test_cells_that_all_failed_fail_the_engine() -> None:
    # stock Postgres on the pg engine: run_benchmark.py exits 0 with these in its report
    verdict = ht.matrix_verdict(_report(_cell("a", "sql"), _all_failed("b", "sql")))
    assert ht.engine_failed(verdict)
    assert verdict["failed"][0]["all_failed"] is True
    assert "pg_clickhouse" in verdict["failed"][0]["first_error"]


def test_one_failed_iteration_fails_the_engine() -> None:
    verdict = ht.matrix_verdict(_report(_cell("a", "sql", errors=1)))
    assert ht.engine_failed(verdict)
    assert verdict["failed"][0]["all_failed"] is False


def test_transports_that_disagree_on_rows_fail_the_engine() -> None:
    verdict = ht.matrix_verdict(_report(_cell("a", "sql", 3046), _cell("a", "graphql", 1000)))
    assert ht.engine_failed(verdict)
    assert verdict["row_disagreements"][0]["query"] == "a"


def test_a_missing_transport_fails_the_engine() -> None:
    report = _report(_cell("a", "sql"))
    report["transports_tested"] = sorted(ht.TRANSPORTS - {"grpc"})
    verdict = ht.matrix_verdict(report)
    assert verdict["unavailable_transports"] == ["grpc"]
    assert ht.engine_failed(verdict)


def test_a_run_that_wrote_no_report_fails_the_engine(tmp_path: Path) -> None:
    (tmp_path / "pg").mkdir()
    assert ht.engine_failed(ht.read_verdict(tmp_path, "pg", False, set()))


def test_only_this_runs_report_is_read(tmp_path: Path) -> None:
    engine = tmp_path / "pg"
    engine.mkdir()
    old = engine / "1.json"
    old.write_text(json.dumps(_report(_cell("a", "sql"))))
    assert ht.engine_failed(ht.read_verdict(tmp_path, "pg", False, {old}))
    new = engine / "2.json"
    new.write_text(json.dumps(_report(_all_failed("a", "sql"))))
    verdict = ht.read_verdict(tmp_path, "pg", False, {old})
    assert verdict["failed"]


def test_an_optimistic_step_with_errors_fails_the_engine() -> None:
    good = {"steps": [{"errors": 0, "first_error": None}]}
    bad = {"steps": [{"errors": 2, "first_error": "boom"}]}
    down = {"error": "connect: refused", "steps": []}
    reports = {t: good for t in ht.lp.ol.TRANSPORTS}
    assert not ht.engine_failed(ht.optimistic_verdict(reports))
    verdict = ht.optimistic_verdict({**reports, "grpc": bad, "mcp": down})
    assert {f["transport"] for f in verdict["failed"]} == {"grpc", "mcp"}
    assert ht.engine_failed(verdict)


def test_only_transport_reports_are_optimistic_cells(tmp_path: Path) -> None:
    folder = tmp_path / "pg" / "optimistic"
    folder.mkdir(parents=True)
    for name in ("summary", *ht.lp.ol.TRANSPORTS):
        (folder / f"{name}.json").write_text(json.dumps({"steps": []}))
    assert ht.read_verdict(tmp_path, "pg", True, set())["cells"] == len(ht.lp.ol.TRANSPORTS)


def test_a_missing_optimistic_transport_fails_the_engine() -> None:
    reports = {t: {"steps": []} for t in ht.lp.ol.TRANSPORTS if t != "sparql"}
    assert ht.optimistic_verdict(reports)["unavailable_transports"] == ["sparql"]


def _stack(tmp_path: Path, *engines: str, pg_image: str = ht.PERF_PG_IMAGE) -> dict[str, Any]:
    path = ht.compose_file(PORTS, tmp_path, list(engines), pg_image)
    return yaml.safe_load(path.read_text())["services"]


def test_duckdb_needs_no_host_network(tmp_path: Path) -> None:
    services = _stack(tmp_path, "duckdb")
    assert set(services) == {"postgresql", "mongodb", "clickhouse", "neo4j"}
    assert not any("network_mode" in s for s in services.values())


def test_pg_builds_the_perf_image_on_the_host_network(tmp_path: Path) -> None:
    pg = _stack(tmp_path, "pg")["postgresql"]
    assert pg["image"] == ht.PERF_PG_IMAGE
    assert pg["build"]["dockerfile"] == "Dockerfile.postgres"
    assert Path(pg["build"]["context"]).is_absolute()
    assert pg["network_mode"] == "host"
    assert "ports" not in pg
    assert f"port={PORTS['bench-postgresql']}" in pg["command"]
    assert "shared_preload_libraries=pg_duckdb" in pg["command"]
    assert str(PORTS["bench-postgresql"]) in pg["healthcheck"]["test"][1]


def test_a_named_pg_image_is_not_built(tmp_path: Path) -> None:
    pg = _stack(tmp_path, "pg", pg_image="postgres:16")["postgresql"]
    assert "build" not in pg
    assert "shared_preload_libraries=pg_duckdb" not in pg["command"]


def test_pg_publishes_clickhouses_native_port(tmp_path: Path) -> None:
    assert "127.0.0.1:9000:9000" in _stack(tmp_path, "pg")["clickhouse"]["ports"]
    assert "127.0.0.1:9000:9000" not in _stack(tmp_path, "trino")["clickhouse"]["ports"]


def test_trino_brings_everything_its_boot_needs(tmp_path: Path) -> None:
    services = _stack(tmp_path, "trino")
    assert {"trino", "zaychik", "minio"} <= set(services)
    for name in ("trino", "zaychik", "minio"):
        assert services[name]["network_mode"] == "host"
        assert "ports" not in services[name]
        assert "healthcheck" in services[name]
    assert services["zaychik"]["build"]["context"] == str(ht.REPO / "zaychik")
    assert services["minio"]["environment"]["MINIO_API_PORT_NUMBER"] == str(PORTS["minio"])
    config = next(v for v in services["trino"]["volumes"] if "config.properties:" in v)
    text = Path(config.split(":")[0]).read_text()
    assert f"http-server.http.port={PORTS['trino']}" in text
    assert f"discovery.uri=http://localhost:{PORTS['trino']}" in text
    assert services["trino"]["environment"]["HOSTNAME"] == "benchtrino"


def test_the_trino_server_has_the_store_and_proxy_names() -> None:
    env = ht.engine_env("trino", PORTS)
    assert env["PROVISA_OTEL_S3_ACCESS_KEY"] and env["PROVISA_OTEL_S3_SECRET_KEY"]
    assert env["PROVISA_OTEL_S3_ENDPOINT"].endswith(f":{PORTS['minio']}")
    assert env["ZAYCHIK_PORT"] == str(PORTS["zaychik"])
    assert not any("host.docker.internal" in v for v in env.values())


def test_the_pg_server_lands_replicas_where_the_engine_reads() -> None:
    env = ht.engine_env("pg", PORTS)
    assert env["PROVISA_MATERIALIZE_URL"] == env["PROVISA_ENGINE_URL"]


@pytest.mark.parametrize("engine", ["pg", "trino"])
def test_a_container_engine_is_refused_off_linux(
    engine: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(ht.sys, "platform", "darwin")
    monkeypatch.setattr(ht.sys, "argv", ["x", "--engine", engine, "--output-dir", "o"])
    with pytest.raises(SystemExit):
        ht.main()
    assert "Linux" in capsys.readouterr().err
