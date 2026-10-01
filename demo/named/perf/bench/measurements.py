# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""What a benchmark step measures beyond latency (REQ-1911): CPU of each source's container, and
the per-request time split from the server's own stats."""

from __future__ import annotations

import subprocess
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

_DOCKER_STATS = ["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}}"]
_DOCKER_TIMEOUT_S = 30


def docker_stats_text() -> str:
    """One ``docker stats`` snapshot: a ``<name> <cpu%>`` line per running container."""
    return subprocess.run(
        _DOCKER_STATS, capture_output=True, text=True, check=True, timeout=_DOCKER_TIMEOUT_S
    ).stdout


def parse_docker_stats(text: str) -> dict[str, float]:
    """Container name -> CPU in cores (docker reports 100% for one busy core)."""
    out: dict[str, float] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) != 2 or not parts[1].endswith("%"):
            raise ValueError(f"docker stats line is not '<name> <cpu>%': {line!r}")
        try:
            out[parts[0]] = float(parts[1][:-1]) / 100
        except ValueError:
            raise ValueError(f"docker stats line has a malformed CPU: {line!r}") from None
    return out


@dataclass(frozen=True)
class SampleResult:
    cores: Mapping[str, float | None]  # source -> mean cores over the samples; None: never seen
    samples: int
    error: str | None  # the first failure, if any sample failed


class DockerStatsSampler:
    """Samples the CPU of the named containers while a step runs. A container that never shows up
    reports None; a failing ``docker`` call is reported in ``result().error``, not swallowed."""

    def __init__(
        self,
        containers: Mapping[str, str],
        run: Callable[[], str] = docker_stats_text,
        pause_s: float = 0.0,
    ) -> None:
        self._containers = dict(containers)  # source -> container name
        self._run = run
        self._pause_s = pause_s
        self._sums: dict[str, float] = {}
        self._counts: dict[str, int] = {}
        self._samples = 0
        self._error: str | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def sample_once(self) -> None:
        try:
            seen = parse_docker_stats(self._run())
        except Exception as exc:  # noqa: BLE001 - recorded in the result, not hidden
            if self._error is None:
                self._error = f"{type(exc).__name__}: {exc}"
            return
        self._samples += 1
        for container in self._containers.values():
            if container in seen:
                self._sums[container] = self._sums.get(container, 0.0) + seen[container]
                self._counts[container] = self._counts.get(container, 0) + 1

    def start(self) -> None:
        def loop() -> None:
            while not self._stop.is_set():
                self.sample_once()
                self._stop.wait(self._pause_s)

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()

    def result(self) -> SampleResult:
        return SampleResult(
            cores={
                source: (self._sums[c] / self._counts[c] if c in self._counts else None)
                for source, c in self._containers.items()
            },
            samples=self._samples,
            error=self._error,
        )


def source_cpu_fields(
    cores: Mapping[str, float | None],
    *,
    window_s: float,
    attempts: int,
    requests_by_source: Mapping[str, int],
) -> dict[str, Any]:
    """A step's source CPU: cores, CPU ms per request, and CPU ms per request that read the source
    (the requests whose own table is in it)."""

    def per(requests: int, c: float | None) -> float | None:
        return None if c is None or requests == 0 else round(c * window_s / requests * 1000, 3)

    return {
        "source_cpu_cores": dict(cores),
        "source_cpu_ms_per_request": {s: per(attempts, c) for s, c in cores.items()},
        "source_cpu_ms_per_source_request": {
            s: per(requests_by_source.get(s, 0), c) for s, c in cores.items()
        },
    }


# --------------------------------------------------------------------------------------------
# The per-request time split, from the server's own stats (``X-Provisa-Stats: true``)
# --------------------------------------------------------------------------------------------

# The HTTP surfaces that return ``provisa_stats``: GraphQL under ``extensions``, SQL and Cypher at
# the top level of the body.
PROFILE_TRANSPORTS = ("graphql", "data_sql", "cypher_http")


def extract_stats(transport: str, body: Mapping[str, Any]) -> dict[str, Any]:
    stats = (
        body.get("extensions", {}).get("provisa_stats")
        if transport == "graphql"
        else body.get("provisa_stats")
    )
    if stats is None:
        raise ValueError(f"no provisa_stats in the {transport} response")
    return stats


def time_split(stats: Mapping[str, Any]) -> dict[str, Any]:
    """One request's time by where it went: ``source_ms`` (direct reads and API calls),
    ``engine_ms`` (federated reads) and ``provisa_ms`` (the rest of the wall clock: parse, govern,
    serialize, cache). Entries that overlap (parallel fields) can sum past the total; that is
    flagged and ``provisa_ms`` is then 0, never negative."""
    source = engine = 0.0
    cache_hit = False
    for entry in stats["sources"]:
        strategy = entry["strategy"]
        if strategy.startswith(("direct", "api")):
            source += entry["elapsed_ms"]
        elif strategy.startswith("federated"):
            engine += entry["elapsed_ms"]
        elif strategy != "cache":
            raise ValueError(f"unknown strategy {strategy!r} in provisa_stats")
        cache_hit = cache_hit or bool(entry.get("cache_hit"))
    total = stats["total_elapsed_ms"]
    rest = total - source - engine
    return {
        "total_ms": total,
        "source_ms": round(source, 3),
        "engine_ms": round(engine, 3),
        "provisa_ms": round(max(rest, 0.0), 3),
        "cache_hit": cache_hit,
        "entries_exceed_total": rest < 0,
    }


def _send(transport: str, client: Any, query: Any, role: str) -> Mapping[str, Any]:
    headers = {"X-Provisa-Stats": "true"}
    if transport == "graphql":
        resp = client.post("/data/graphql", json={"query": query.graphql}, headers=headers)
    elif transport == "data_sql":
        resp = client.post("/data/sql", json={"sql": query.sql, "role": role}, headers=headers)
    else:
        resp = client.post(
            "/data/cypher", json={"query": query.cypher, "params": {}}, headers=headers
        )
    resp.raise_for_status()
    return resp.json()


def profile_transport(
    setup: Any, transport: str, client: Any, *, role: str, requests: int
) -> dict[str, Any]:
    """Send ``requests`` generated requests one at a time with the stats header and report, per
    source the request read, the mean time split. Concurrency 1, so the numbers are the cost of the
    request and not of queueing."""
    if transport not in PROFILE_TRANSPORTS:
        raise ValueError(f"no per-request time split on {transport}: only {PROFILE_TRANSPORTS}")
    import request_mix
    import request_render

    # uncached: the split is the cost of the request, not of a response-cache hit
    gen = request_mix.RequestGenerator(setup, transport, f"profile/{transport}", cacheable=False)
    renderer = request_render.Renderer(setup)
    groups: dict[str, list[dict[str, Any]]] = {}
    for _ in range(requests):
        spec = gen.next()
        body = _send(transport, client, renderer.render(spec), role)
        groups.setdefault(spec.source, []).append(time_split(extract_stats(transport, body)))
    return {
        "transport": transport,
        "requests": requests,
        "by_source": {
            sid: {
                "requests": len(g),
                **{
                    k: round(sum(x[k] for x in g) / len(g), 3)
                    for k in ("total_ms", "provisa_ms", "engine_ms", "source_ms")
                },
                "cache_hits": sum(1 for x in g if x["cache_hit"]),
                "entries_exceed_total": sum(1 for x in g if x["entries_exceed_total"]),
            }
            for sid, g in groups.items()
        },
    }


# --------------------------------------------------------------------------------------------
# The idle window: what the deployment costs with no load (the refresh cost of landed sources)
# --------------------------------------------------------------------------------------------


def sample_during(sampler: DockerStatsSampler, seconds: float) -> None:
    """Sleep ``seconds`` while ``sampler`` samples its containers."""
    sampler.start()
    try:
        time.sleep(seconds)
    finally:
        sampler.stop()


def measure_idle(
    *,
    window_s: float,
    server_cpu: Callable[[], float],
    sampler: DockerStatsSampler,
    sleep: Callable[[float], None],
) -> dict[str, Any]:
    """CPU of the server and the monitored source containers over ``window_s`` seconds with no
    load. ``sleep`` waits the window (and lets ``sampler`` sample); ``server_cpu`` is the server
    tree's cumulative CPU seconds."""
    cpu0 = server_cpu()
    sleep(window_s)
    cpu1 = server_cpu()
    sampled = sampler.result()
    return {
        "window_s": window_s,
        "server_cpu_cores": round((cpu1 - cpu0) / window_s, 4),
        "source_cpu_cores": dict(sampled.cores),
        "source_cpu_error": sampled.error,
    }
