# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3f0c52-9a41-4e8e-b6a2-0f1c9de2a7b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Peak resident memory of one Provisa boot, stage by stage.

Boots the default demo (config/provisa-install.yaml, DuckDB engine) on an isolated test instance:
its own PROVISA_HOME, an embedded PostgreSQL control plane in a fresh directory, the demo's
petstore/GraphQL/gRPC servers and leased ports, the same shape as the smoke check
(tests/smoke/test_main_is_startable.py). It never touches local-dev.

The backend runs under a child of this script that samples the process's RSS every few
milliseconds and labels each sample with the boot stage it falls in:

* ``import``: importing ``main`` (the app module and everything it pulls in);
* each lifespan ``startup phase`` mark (``load_config``, ``rebuild_schemas`` ...);
* each phase of the boot build (``rebuild_trace``: ``tables`` ... ``converge``) and of its
  after-answer pass (``derived_writes``, ``names_table`` ... ``warm_up``);
* ``background tasks`` and ``protocol listeners`` (``_start_background_tasks`` and
  ``_start_servers``).

It stops the backend once the after-answer pass's warm-up has ended (or ``--settle`` seconds
after /ready), and prints the peak RSS of each stage. ``--tracemalloc`` also records the Python
allocations that grew across the main stages (slower; RSS then includes tracemalloc's own cost).

    .venv/bin/python scripts/boot_memory_profile.py --out boot.json
    .venv/bin/python scripts/boot_memory_profile.py --repo ../other-checkout \\
        --python ../other-checkout/.venv/bin/python --out old.json

Run it through ~/.provisa-agent/heavy.sh (one machine-wide slot).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve()
DEFAULT_REPO = HERE.parents[1]
CONFIG = "config/provisa-install.yaml"

# The stages the brief names, as the lifespan marks and build phases that make them up.
_SNAPSHOT_AT = {
    "import",
    "load_config",
    "rebuild_schemas",
    "names_table",
    "warm_up",
    "protocol listeners",
    "worker ready",
}


# ---------------------------------------------------------------------------------------------
# child: runs inside the backend process
# ---------------------------------------------------------------------------------------------


class _Recorder:
    """RSS samples labelled by stage, and (optionally) tracemalloc snapshots at stage ends."""

    def __init__(self, out: Path, tracemalloc_frames: int, interval_s: float) -> None:
        import psutil

        self._proc = psutil.Process()
        self._out = out
        self._lock = threading.Lock()
        self._t0 = time.perf_counter()
        self._label = "interpreter"
        self._peak = self._rss()
        self._events: list[dict[str, Any]] = []
        self._allocs: list[dict[str, Any]] = []
        self._hooks: list[str] = []
        self._modules: dict[str, list[str]] = {}
        self._frames = tracemalloc_frames
        self._last_snapshot: Any = None
        self._interval = interval_s
        if tracemalloc_frames:
            import tracemalloc

            tracemalloc.start(tracemalloc_frames)
        threading.Thread(target=self._sample, name="boot-mem-sampler", daemon=True).start()

    def _rss(self) -> int:
        return int(self._proc.memory_info().rss)

    def _sample(self) -> None:
        while True:
            rss = self._rss()
            with self._lock:
                if rss > self._peak:
                    self._peak = rss
            time.sleep(self._interval)

    def hook(self, name: str) -> None:
        self._hooks.append(name)

    def event(self, closes: str, opens: str, *, mark: bool = False) -> None:
        """Close the interval in progress, attributing it to ``closes``; open ``opens``."""
        rss = self._rss()
        with self._lock:
            peak = max(self._peak, rss)
            self._peak = rss
            self._label = opens
        entry: dict[str, Any] = {
            "t": round(time.perf_counter() - self._t0, 3),
            "stage": closes,
            "opens": opens,
            "mark": mark,
            "rss_end": rss,
            "rss_peak": peak,
        }
        if mark:
            # The modules loaded by the end of each lifespan stage: what a stage imported is
            # the difference from the stage before.
            entry["modules"] = len(sys.modules)
            self._modules[closes] = sorted(
                sys.modules.copy()
            )  # copy(): atomic under other threads' imports
        if self._frames:
            import tracemalloc

            cur, tpeak = tracemalloc.get_traced_memory()
            tracemalloc.reset_peak()
            entry["py_current"] = cur
            entry["py_peak"] = tpeak
            if closes in _SNAPSHOT_AT:
                self._snapshot(closes)
        self._events.append(entry)
        self._write()

    def current(self) -> str:
        return self._label

    def _snapshot(self, stage: str) -> None:
        import linecache
        import tracemalloc

        snap = tracemalloc.take_snapshot().filter_traces(
            (
                tracemalloc.Filter(False, tracemalloc.__file__),
                # traceback.format() below reads source lines into linecache: ours, not the app's.
                tracemalloc.Filter(False, linecache.__file__),
            )
        )
        prev = self._last_snapshot
        self._last_snapshot = snap
        top: list[dict[str, Any]] = []
        if prev is None:
            stats = snap.statistics("traceback")[:40]
            for s in stats:
                top.append({"size": s.size, "count": s.count, "trace": s.traceback.format()[-6:]})
        else:
            for d in snap.compare_to(prev, "traceback")[:40]:
                top.append(
                    {
                        "size_diff": d.size_diff,
                        "size": d.size,
                        "count_diff": d.count_diff,
                        "trace": d.traceback.format()[-6:],
                    }
                )
        by_file = {}
        for s in snap.statistics("filename")[:40]:
            by_file[str(s.traceback[0].filename)] = s.size
        self._allocs.append({"stage": stage, "top": top, "by_file": by_file})

    def _write(self) -> None:
        tmp = self._out.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(
                {
                    "hooks": self._hooks,
                    "events": self._events,
                    "allocs": self._allocs,
                    "modules": self._modules,
                },
                indent=1,
            )
        )
        tmp.replace(self._out)


_MARK_TAIL = re.compile(r"\s\+\s*[\d.]+s")


def _install_hooks(rec: _Recorder) -> None:
    import logging

    # SIGUSR1 from the parent closes the interval after ready (the settle) as its own stage.
    def _settled(*_: object) -> None:
        if rec.current() != "lifespan":
            rec.event(rec.current(), "lifespan")
        rec.event("after ready", "lifespan", mark=True)

    signal.signal(signal.SIGUSR1, _settled)

    class _Marks(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            msg = record.getMessage()
            if not msg.startswith("startup phase"):
                return
            # "startup phase <name> begin" opens the lifespan; every other mark ends the stage
            # it names ("+1.2s (total ...)", "ready pid=..", "applied pid=.." follow the name).
            words = _MARK_TAIL.split(msg[len("startup phase") :])[0].split()
            words = [w for w in words if not w.startswith("pid=")]
            if words[-1] == "begin":
                rec.event(rec.current(), "lifespan")
                return
            if words[-1] in ("ready", "applied") or words[-2:] == ["found", "complete"]:
                words = words[:-1] if words[-1] != "complete" else words[:-2]
            stage = " ".join(words)
            if stage == "worker":
                stage = "worker ready"
            if rec.current() != "lifespan":
                # A build or pass phase, or a timed call, was the last thing opened: close it
                # under its own name, then the lifespan stage it ran in.
                rec.event(rec.current(), "lifespan")
            rec.event(stage, "lifespan", mark=True)

    handler = _Marks(level=logging.WARNING)
    logging.getLogger("uvicorn.error").addHandler(handler)
    rec.hook("startup-phase log marks")

    from provisa.api import rebuild_trace

    original_phase = rebuild_trace.BuildRecord.phase

    def phase(self: Any, name: str) -> None:
        rec.event(rec.current(), f"build:{name}")
        original_phase(self, name)

    rebuild_trace.BuildRecord.phase = phase  # type: ignore[method-assign]
    rec.hook("rebuild_trace.BuildRecord.phase")

    from provisa.api import build_after_answer

    original_run = build_after_answer.run

    async def run(held: Any, *, before: str | None = None) -> None:
        for step_name, step in list(held.steps.items()):
            if getattr(step, "_boot_mem_wrapped", False):
                continue

            async def wrapped(_step: Any = step, _name: str = step_name) -> None:
                rec.event(rec.current(), f"pass:{_name}")
                try:
                    await _step()
                finally:
                    rec.event(_name, "after pass step")

            wrapped._boot_mem_wrapped = True  # type: ignore[attr-defined]
            held.steps[step_name] = wrapped
        await original_run(held, before=before)

    build_after_answer.run = run
    rec.hook("build_after_answer.run")

    from provisa.api import app as app_module

    for attr, label in (
        ("_start_background_tasks", "background tasks"),
        ("_start_servers", "protocol listeners"),
    ):
        original = getattr(app_module, attr)

        async def timed(*a: Any, _orig: Any = original, _label: str = label, **kw: Any) -> Any:
            rec.event(rec.current(), _label)
            try:
                return await _orig(*a, **kw)
            finally:
                rec.event(_label, "lifespan")

        setattr(app_module, attr, timed)
        rec.hook(f"app.{attr}")


def _child(args: argparse.Namespace) -> None:
    rec = _Recorder(Path(args.out), args.tracemalloc_frames, args.interval)
    sys.path.insert(0, os.getcwd())
    import main  # the backend's app module, as `uvicorn main:app` imports it

    rec.event("import", "lifespan", mark=True)
    import uvicorn

    # uvicorn configures logging in Config() (dictConfig replaces uvicorn.error's handlers), so
    # the marks handler is attached after it.
    config = uvicorn.Config(main.app, host="127.0.0.1", port=args.port, log_level="info")
    _install_hooks(rec)
    uvicorn.Server(config).run()


# ---------------------------------------------------------------------------------------------
# parent: the isolated instance
# ---------------------------------------------------------------------------------------------


def _run(python: str, repo: Path, args: list[str], env: dict[str, str]) -> str:
    done = subprocess.run(
        [python, *args], cwd=repo, env=env, capture_output=True, text=True, check=False
    )
    if done.returncode != 0:
        raise SystemExit(f"{args} failed:\n{done.stdout}\n{done.stderr}")
    return done.stdout


def _spawn(python: str, repo: Path, name: str, argv: list[str], log_dir: Path, env: dict[str, str]):
    log = (log_dir / f"{name}.log").open("wb")
    return subprocess.Popen(
        [python, *argv],
        cwd=repo,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


def _lease(n: int) -> list[int]:
    sys.path.insert(0, str(DEFAULT_REPO))
    from tests.port_lease import lease_ports

    return list(lease_ports(n))


def _wait_done(
    backend: subprocess.Popen, out: Path, log: Path, settle: float, boot_seconds: float
) -> str:
    """Return once the worker has logged that it serves and ``settle`` seconds have passed (the
    demo's background registration and the builds it requests run in that time)."""
    deadline = time.monotonic() + boot_seconds
    ready_at: float | None = None
    while time.monotonic() < deadline:
        if backend.poll() is not None:
            raise SystemExit(f"the backend exited:\n{log.read_text()[-4000:]}")
        if ready_at is None and out.exists():
            if any(e["stage"] == "worker ready" for e in json.loads(out.read_text())["events"]):
                ready_at = time.monotonic()
        if ready_at is not None and time.monotonic() - ready_at > settle:
            return f"worker ready + {settle:.0f}s"
        time.sleep(0.5)
    raise SystemExit(f"no 'worker ready' in {boot_seconds:.0f}s:\n{log.read_text()[-4000:]}")


def _parent(args: argparse.Namespace) -> None:
    repo = Path(args.repo).resolve()
    python = str(Path(args.python).absolute()) if args.python else str(repo / ".venv/bin/python")
    out = Path(args.out).resolve()
    out.unlink(missing_ok=True)
    root = Path(tempfile.mkdtemp(prefix="provisa-bootmem-", dir="/tmp"))
    home = root / "home"
    (home / "demo").mkdir(parents=True)
    log_dir = Path(args.log_dir).resolve() if args.log_dir else root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    api, pet, gql, demo_grpc, grpc_port, flight, pgwire, mcp, bolt = _lease(9)
    control_dir = home / "demo" / "control-pg"
    procs: list[subprocess.Popen] = []
    base = {k: v for k, v in os.environ.items() if not k.startswith(("PROVISA_", "PG_", "TENANT_"))}
    try:
        _run(python, repo, ["demo/files/create_demo_files.py"], base)
        fields = dict(
            line.split("=", 1)
            for line in _run(
                python,
                repo,
                ["-m", "provisa.core.control_plane_pg", "start", str(control_dir)],
                base,
            ).splitlines()
            if "=" in line
        )
        host, port, password = fields["PG_HOST"], fields["PG_PORT"], fields["PG_PASSWORD"]
        database_url = f"postgresql+psycopg://provisa:{password}@/provisa?host={host}&port={port}"
        for name, app_dir, p in (
            ("petstore", "demo/petstore_server", pet),
            ("graphql-demo", "demo/graphql_server", gql),
        ):
            procs.append(
                _spawn(
                    python,
                    repo,
                    name,
                    [
                        "-m",
                        "uvicorn",
                        "server:app",
                        "--app-dir",
                        app_dir,
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(p),
                    ],
                    log_dir,
                    base,
                )
            )
        procs.append(
            _spawn(
                python,
                repo,
                "grpc-demo",
                ["-m", "demo.grpc_server.server"],
                log_dir,
                {**base, "DEMO_GRPC_PORT": str(demo_grpc)},
            )
        )
        env = {
            **base,
            "PROVISA_HOME": str(home),
            "PROVISA_DATA_DIR": str(home / "demo"),
            "PROVISA_CONFIG": CONFIG,
            "PROVISA_CONFIG_REPLACE": "true",
            "PROVISA_DEMO": "true",
            "PROVISA_ENGINE": "duckdb",
            "PROVISA_IDP": "",
            "PROVISA_REDIS_EMBEDDED": "1",
            "PROVISA_FAKE_KEY_DIR": str(home / "fake-key"),
            "PROVISA_ENGINE_FILES_GID": str(os.getgid()),
            "PROVISA_API_PORT": str(api),
            "PROVISA_LOCAL_ADDRESS": f"http://localhost:{api}",
            "GRPC_PORT": str(grpc_port),
            "FLIGHT_PORT": str(flight),
            "PROVISA_PGWIRE_PORT": str(pgwire),
            "PROVISA_MCP_PORT": str(mcp),
            "PROVISA_MCP_HOST": "127.0.0.1",
            "PROVISA_BOLT_PORT": str(bolt),
            "PROVISA_AIRPORT_PORT": "0",
            "TENANT_DATABASE_URL": database_url,
            "PLATFORM_DATABASE_URL": database_url,
            "PG_HOST": host,
            "PG_PORT": port,
            "PG_DATABASE": "provisa",
            "PG_USER": "provisa",
            "PG_PASSWORD": password,
            "POSTGRES_HOST": host,
            "GRAPHQL_DEMO_ENABLED": "true",
            "GRAPHQL_DEMO_URL": f"http://localhost:{gql}/graphql",
            "PETSTORE_BASE_URL": f"http://localhost:{pet}/api/v3",
            "DEMO_GRPC_PORT": str(demo_grpc),
            "DEMO_GRPC_TARGET": f"localhost:{demo_grpc}",
        }
        child = [
            str(HERE),
            "--child",
            "--out",
            str(out),
            "--port",
            str(api),
            "--interval",
            str(args.interval),
            "--tracemalloc-frames",
            str(args.tracemalloc_frames),
        ]
        backend = _spawn(python, repo, "backend", child, log_dir, env)
        procs.append(backend)
        why = _wait_done(backend, out, log_dir / "backend.log", args.settle, args.boot_timeout)
        # The child closes the interval since ready ("after ready") on SIGUSR1.
        os.kill(backend.pid, signal.SIGUSR1)
        time.sleep(1)
        data = json.loads(out.read_text())
        data["stopped"] = why
        data["repo_head"] = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        out.write_text(json.dumps(data, indent=1))
        print(report(data))
    finally:
        for p in procs:
            if p.poll() is None:
                os.killpg(p.pid, signal.SIGKILL)
            p.wait()
        if (control_dir / "PG_VERSION").exists():
            _run(
                python,
                repo,
                ["-m", "provisa.core.control_plane_pg", "stop", str(control_dir)],
                base,
            )
        if args.log_dir is None:
            shutil.rmtree(root)
        else:
            shutil.rmtree(home)


# ---------------------------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------------------------


def _mb(n: int) -> str:
    return f"{n / 1048576:7.1f}"


def stage_rows(data: dict[str, Any]) -> list[tuple[str, int, int, int]]:
    """Each event as (stage, rss at its start, peak, rss at its end)."""
    rows = []
    prev = None
    for e in data["events"]:
        start = prev if prev is not None else e["rss_end"]
        rows.append((e["stage"], start, e["rss_peak"], e["rss_end"]))
        prev = e["rss_end"]
    return rows


def lifespan_peaks(data: dict[str, Any]) -> list[tuple[str, int, int]]:
    """Each lifespan mark as (stage, peak RSS since the previous mark, RSS at the mark). Build
    phases, pass steps and timed calls inside a stage count toward its peak."""
    out = []
    run = 0
    for e in data["events"]:
        run = max(run, e["rss_peak"])
        if e["mark"]:
            out.append((e["stage"], run, e["rss_end"]))
            run = 0
    return out


def span_peaks(data: dict[str, Any]) -> list[tuple[str, int, int, int]]:
    """Each pass step and timed call as (name, RSS when it began, peak while it ran, RSS when it
    ended). A step that runs in the background (the warm-up) overlaps the lifespan stages after
    it; its span covers whatever ran beside it."""
    events = data["events"]
    out = []
    for i, e in enumerate(events):
        opened = e["opens"]
        name = opened[len("pass:") :] if opened.startswith("pass:") else opened
        if not (opened.startswith("pass:") or opened in ("background tasks", "protocol listeners")):
            continue
        for j in range(i + 1, len(events)):
            if events[j]["stage"] == name:
                peak = max(x["rss_peak"] for x in events[i + 1 : j + 1])
                out.append((name, e["rss_end"], peak, events[j]["rss_end"]))
                break
    return out


def report(data: dict[str, Any]) -> str:
    lines = [f"head {data.get('repo_head', '?')}  stopped: {data.get('stopped', '?')}"]
    lines.append(f"hooks: {', '.join(data['hooks'])}")
    lines.append(f"{'stage':38} {'start MB':>8} {'peak MB':>8} {'end MB':>8}")
    for stage, start, peak, end in stage_rows(data):
        lines.append(f"{stage:38} {_mb(start):>8} {_mb(peak):>8} {_mb(end):>8}")
    lines.append("-- lifespan stages:")
    for stage, peak, end in lifespan_peaks(data):
        lines.append(f"   {stage:35} peak {_mb(peak)}  end {_mb(end)}")
    lines.append("-- pass steps and timed calls:")
    for name, start, peak, end in span_peaks(data):
        lines.append(f"   {name:35} start {_mb(start)}  peak {_mb(peak)}  end {_mb(end)}")
    overall = max(e["rss_peak"] for e in data["events"])
    lines.append(f"overall peak RSS {_mb(overall)} MB")
    for alloc in data.get("allocs", []):
        lines.append(f"-- python allocations grown by {alloc['stage']}:")
        for item in alloc["top"][:12]:
            size = item.get("size_diff", item.get("size", 0))
            where = item["trace"][-2:] if item["trace"] else ["?"]
            lines.append(f"   {size / 1048576:7.2f} MB  {' | '.join(s.strip() for s in where)}")
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--repo", default=str(DEFAULT_REPO), help="checkout to boot (default: this one)")
    p.add_argument(
        "--python", help="interpreter for that checkout (default: <repo>/.venv/bin/python)"
    )
    p.add_argument("--out", help="JSON file the measurements are written to")
    p.add_argument("--log-dir", help="keep the instance's logs here")
    p.add_argument("--interval", type=float, default=0.005, help="RSS sample interval, seconds")
    p.add_argument("--settle", type=float, default=60.0, help="seconds after /ready to stop")
    p.add_argument("--boot-timeout", type=float, default=600.0, help="seconds to 'worker ready'")
    p.add_argument("--tracemalloc-frames", type=int, default=0, help="0 = off; N = frames kept")
    p.add_argument("--report", help="print the report of a JSON file written earlier, and exit")
    p.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--port", type=int, help=argparse.SUPPRESS)
    args = p.parse_args()
    if args.report:
        print(report(json.loads(Path(args.report).read_text())))
    elif not args.out:
        p.error("--out is required")
    elif args.child:
        _child(args)
    else:
        _parent(args)


if __name__ == "__main__":
    main()
