# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7b51d0-9a2c-4f86-b1d4-7c05e2a9f613
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1874: the captive Postgres+wrappers lifecycle -- start, health, stop, one container per
wrapped source, and the container-runtime prerequisite refused by name when it is absent."""

from __future__ import annotations

import pytest

from provisa.federation import captive_pg
from provisa.federation.captive_pg import (
    CaptiveContainerError,
    CaptivePostgres,
    ContainerRuntimeMissing,
    require_container_runtime,
)

IMAGE = "example.test/captive-pg:1"


class FakeDocker:
    """Records each docker CLI call; answers from a tiny model of the daemon's containers."""

    def __init__(self, *, cli: bool = True, daemon: bool = True) -> None:
        self.cli = cli
        self.daemon = daemon
        self.calls: list[tuple[str, ...]] = []
        self.envs: list[dict[str, str] | None] = []
        self.containers: dict[str, bool] = {}  # name -> running

    def __call__(self, args: tuple[str, ...], env: dict[str, str] | None = None) -> str:
        self.calls.append(args)
        self.envs.append(env)
        if not self.cli:
            raise FileNotFoundError("docker")
        verb = args[0]
        if verb == "version":
            if not self.daemon:
                raise captive_pg.DockerFailed("Cannot connect to the Docker daemon")
            return "27.0.1\n"
        if verb == "run":
            name = args[args.index("--name") + 1]
            if name in self.containers:
                raise captive_pg.DockerFailed(f"Conflict. The container name {name} is in use")
            self.containers[name] = True
            return "cid\n"
        if verb == "rm":
            self.containers.pop(args[-1], None)
            return ""
        if verb == "inspect":
            name = args[-1]
            if name not in self.containers:
                raise captive_pg.DockerFailed(f"No such object: {name}")
            return "true\n" if self.containers[name] else "false\n"
        if verb == "logs":
            return "FATAL: wrapper failed\n"
        raise AssertionError(f"unexpected docker call {args}")


def _captive(docker: FakeDocker, source_id: str = "hub", org_id: str = "org1", **kw):
    return CaptivePostgres(
        org_id=org_id,
        source_id=source_id,
        image=IMAGE,
        port=6543,
        docker=docker,
        **kw,
    )


# -- prerequisite ---------------------------------------------------------------------------


def test_missing_docker_cli_is_refused_by_name():
    docker = FakeDocker(cli=False)
    with pytest.raises(ContainerRuntimeMissing) as exc:
        require_container_runtime(docker=docker)
    assert "requires a local container runtime" in str(exc.value)


def test_unreachable_daemon_is_refused_by_name():
    docker = FakeDocker(daemon=False)
    with pytest.raises(ContainerRuntimeMissing) as exc:
        require_container_runtime(docker=docker)
    assert "requires a local container runtime" in str(exc.value)
    assert "Cannot connect to the Docker daemon" in str(exc.value)


def test_refusal_names_no_internals():
    """REQ-1874's UX constraint: the user never sees wrappers, FDW or captive instance."""
    docker = FakeDocker(cli=False)
    with pytest.raises(ContainerRuntimeMissing) as exc:
        _captive(docker).start()
    text = str(exc.value).lower()
    for word in ("wrapper", "fdw", "captive"):
        assert word not in text


def test_start_without_runtime_starts_nothing():
    docker = FakeDocker(daemon=False)
    with pytest.raises(ContainerRuntimeMissing):
        _captive(docker).start()
    assert not [c for c in docker.calls if c[0] == "run"]


# -- start ----------------------------------------------------------------------------------


def test_start_runs_the_image_detached_on_its_own_port():
    docker = FakeDocker()
    cap = _captive(docker)
    cap.start()
    run = next(c for c in docker.calls if c[0] == "run")
    assert run[-1] == IMAGE
    assert "-d" in run
    assert run[run.index("-p") + 1] == "127.0.0.1:6543:5432"
    assert run[run.index("--name") + 1] == cap.container_name
    assert docker.containers[cap.container_name] is True


def test_password_is_never_on_the_command_line():
    docker = FakeDocker()
    cap = _captive(docker)
    cap.start()
    idx = next(i for i, c in enumerate(docker.calls) if c[0] == "run")
    run, env = docker.calls[idx], docker.envs[idx]
    assert "POSTGRES_PASSWORD" in run  # taken from the CLI's environment
    assert all(cap.password not in arg for arg in run)
    assert env is not None and env["POSTGRES_PASSWORD"] == cap.password


def test_start_twice_is_a_lifecycle_error():
    docker = FakeDocker()
    cap = _captive(docker)
    cap.start()
    with pytest.raises(CaptiveContainerError):
        cap.start()


def test_start_reclaims_this_sources_container_left_by_a_dead_process():
    docker = FakeDocker()
    cap = _captive(docker)
    docker.containers[cap.container_name] = False  # left behind by a crashed run
    cap.start()
    verbs = [c[0] for c in docker.calls]
    assert verbs.index("rm") < verbs.index("run")
    assert docker.containers[cap.container_name] is True


def test_docker_run_failure_is_raised_with_its_reason():
    def failing(args, env=None):
        if args[0] == "run":
            raise captive_pg.DockerFailed("pull access denied")
        return FakeDocker()(args, env)

    with pytest.raises(CaptiveContainerError) as exc:
        _captive(failing).start()
    assert "pull access denied" in str(exc.value)


# -- isolation: one container per wrapped source --------------------------------------------


def test_each_source_gets_its_own_container():
    docker = FakeDocker()
    a = _captive(docker, source_id="hubspot_src")
    b = _captive(docker, source_id="airtable_src")
    assert a.container_name != b.container_name


def test_same_source_id_in_two_orgs_gets_two_containers():
    docker = FakeDocker()
    assert (
        _captive(docker, org_id="o1").container_name != _captive(docker, org_id="o2").container_name
    )


def test_two_instances_never_share_a_container(monkeypatch, tmp_path):
    """test / local-dev / cloud-dev on one machine: the same org and source id in two instances
    (two data directories) name two containers."""
    docker = FakeDocker()
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "a"))
    first = _captive(docker).container_name
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "b"))
    second = _captive(docker).container_name
    assert first != second


def test_container_name_is_stable_for_a_source():
    docker = FakeDocker()
    assert _captive(docker).container_name == _captive(docker).container_name


# -- health ---------------------------------------------------------------------------------


async def test_health_before_start_is_a_lifecycle_error():
    with pytest.raises(CaptiveContainerError):
        await _captive(FakeDocker()).health()


async def test_health_runs_a_query_against_the_published_port():
    seen = []

    async def probe(host, port, user, password, database):
        seen.append((host, port, user, password, database))
        return True

    docker = FakeDocker()
    cap = _captive(docker, probe=probe)
    cap.start()
    assert await cap.health() is True
    assert seen == [("127.0.0.1", 6543, "postgres", cap.password, "postgres")]


async def test_wait_ready_reports_an_exited_container_with_its_log():
    async def never(*_a):
        return False

    docker = FakeDocker()
    cap = _captive(docker, probe=never)
    cap.start()
    docker.containers[cap.container_name] = False  # the server exited
    with pytest.raises(CaptiveContainerError) as exc:
        await cap.wait_ready(timeout=5)
    assert "FATAL: wrapper failed" in str(exc.value)


async def test_wait_ready_times_out_loud():
    async def never(*_a):
        return False

    docker = FakeDocker()
    cap = _captive(docker, probe=never)
    cap.start()
    with pytest.raises(CaptiveContainerError) as exc:
        await cap.wait_ready(timeout=0.3, interval=0.1)
    assert "not ready" in str(exc.value)


async def test_wait_ready_returns_once_healthy():
    answers = iter([False, False, True])

    async def probe(*_a):
        return next(answers)

    docker = FakeDocker()
    cap = _captive(docker, probe=probe)
    cap.start()
    await cap.wait_ready(timeout=5, interval=0.01)


# -- stop -----------------------------------------------------------------------------------


def test_stop_removes_the_container():
    docker = FakeDocker()
    cap = _captive(docker)
    cap.start()
    cap.stop()
    assert cap.container_name not in docker.containers
    assert docker.calls[-1] == ("rm", "-f", cap.container_name)


def test_stop_is_idempotent():
    docker = FakeDocker()
    cap = _captive(docker)
    cap.stop()
    cap.start()
    cap.stop()
    cap.stop()
    assert [c[0] for c in docker.calls].count("rm") == 1  # only the stop of a started container


def test_restart_after_stop():
    docker = FakeDocker()
    cap = _captive(docker)
    cap.start()
    cap.stop()
    cap.start()
    assert docker.containers[cap.container_name] is True


def test_image_is_required():
    with pytest.raises(ValueError):
        CaptivePostgres(org_id="o", source_id="s", image="", port=1, docker=FakeDocker())
