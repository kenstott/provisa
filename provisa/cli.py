# Copyright (c) 2026 Kenneth Stott
# Canary: 569e177d-4d8e-46f7-a269-a776b1e73a6d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``provisa`` console entry point (REQ-1128).

Launches the pip-installed embedded tier (REQ-1126): a self-contained Provisa
system — SQLite control plane + embedded DuckDB engine + in-memory cache — with
no Docker, Node, or external services. ``provisa run`` starts the API app and the
UI static/proxy server together in a single process and serves the precompiled
React UI packaged into the wheel (REQ-1127).

Full multi-engine federation stays available by pointing at a customer-provided
external engine (REQ-1129): set ``TRINO_HOST``/``TRINO_PORT`` (or the
``federation_engine_host``/``federation_engine_port`` config) before launch and
the embedded DuckDB default is replaced by the external coordinator.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

_DEFAULT_DATA_DIR = Path.home() / ".provisa" / "native"
_PKG_ROOT = Path(__file__).resolve().parent  # the installed ``provisa`` package
_REPO_ROOT = Path(__file__).resolve().parents[1]  # repo root when running from source


def _resolve_demo() -> tuple[Path, Path]:
    """Return (demo_config, demo_data_dir): the bundled demo — the pre-federated ``provisa-install``
    config (pet-store + shelter sample domains over embedded SQLite) plus its sample-data directory.

    Prefers the wheel-staged copy under ``provisa/_config`` (REQ-1127); falls back to the repo tree
    when running from a source checkout.
    """
    pkg_cfg = _PKG_ROOT / "_config" / "provisa-install.yaml"
    if pkg_cfg.exists():
        return pkg_cfg, _PKG_ROOT / "_config" / "demo" / "files"
    return _REPO_ROOT / "config" / "provisa-install.yaml", _REPO_ROOT / "demo" / "files"


def _apply_demo_config() -> Path:
    """Point the embedded runtime at the bundled demo (REQ-414 sample federation). Sets PROVISA_CONFIG
    to the demo config and PROVISA_DEMO_DIR to its sample-data dir (the config resolves the embedded
    SQLite paths through ``${env:PROVISA_DEMO_DIR}``). ``setdefault`` so an explicit override wins.
    Unreachable optional demo sources (the openapi/graphql mocks) are best-effort and never abort
    startup (app_loaders), so the demo runs fully offline on the two embedded SQLite sources."""
    cfg, data_dir = _resolve_demo()
    if not cfg.exists():
        raise FileNotFoundError(f"demo config not found (looked for {cfg})")
    os.environ.setdefault("PROVISA_CONFIG", str(cfg))
    os.environ.setdefault("PROVISA_DEMO_DIR", str(data_dir))
    os.environ.setdefault("PROVISA_CONFIG_REPLACE", "true")
    return cfg


def _apply_embedded_env(data_dir: Path) -> list[str]:
    """Resolve and apply the embedded ("native") launch environment (REQ-1126, REQ-1129).

    Reuses the tested capabilities-preset resolver (desktop_profile.load_profile) so the
    embedded tier is the exact same self-contained runtime the desktop installer ships:
    DuckDB engine, SQLite control plane, fakeredis cache. Existing process env wins
    (setdefault) so a customer-provided external engine (TRINO_HOST/PORT, PROVISA_ENGINE_URL)
    layered on before launch is preserved.
    """
    from provisa.core.desktop_profile import load_profile

    profile = load_profile("native", data_dir=data_dir)
    for key, value in profile.env.items():
        os.environ.setdefault(key, value)
    notes = list(profile.notes)

    # Stage the DuckDB extensions OFFLINE from the provisa-duckdb-ext PyPI package (installed by
    # provisa[embedded]) so LOAD never reaches extensions.duckdb.org — required behind an enterprise
    # firewall where only PyPI/Maven/npm/NuGet are proxied. Absent package = a dev checkout without the
    # extra: leave PROVISA_DUCKDB_EXT_DIR unset so DuckDB's network INSTALL still works for local dev.
    if not os.environ.get("PROVISA_DUCKDB_EXT_DIR"):
        from provisa.federation.duckdb_extensions import stage_bundled_extensions

        try:
            ext_dir = stage_bundled_extensions(data_dir / "duckdb-ext")
        except ModuleNotFoundError:
            notes.append(
                "duckdb extensions: provisa-duckdb-ext not installed — DuckDB will INSTALL from the "
                "network on first use (install provisa[embedded] for an offline/air-gapped setup)"
            )
        else:
            os.environ["PROVISA_DUCKDB_EXT_DIR"] = str(ext_dir)
            notes.append(f"duckdb extensions: staged offline (no network) -> {ext_dir}")
    return notes


def _control_plane_drift(data_dir: Path) -> str | None:
    """Return a ``file:table.column`` description of the FIRST schema drift in the embedded control-
    plane DBs, else None.

    V1 has no migrations (``create_all`` never ALTERs an existing table), so a native DB left by an
    OLDER Provisa whose table is missing a column the current ORM writes crashes startup with e.g.
    ``no such column: load_protected`` — and uvicorn swallows that inside its lifespan, so the app
    just dies with no useful message. This detects it BEFORE serving so ``run`` can fail loud with a
    ``--reset`` hint. Only MISSING columns are drift; extra DB columns (newer DB on older code) are
    not this failure mode and are ignored."""
    import sqlite3

    from provisa.core import schema_admin, schema_org

    for fname, meta in (("platform.db", schema_admin.metadata), ("tenant.db", schema_org.metadata)):
        db = data_dir / fname
        if not db.exists():
            continue  # fresh install — create_all builds it current
        con = sqlite3.connect(str(db))
        try:
            present = {
                r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            for table in meta.tables.values():
                if table.name not in present:
                    continue  # a table the ORM will create on start — not drift
                have = {r[1] for r in con.execute(f'PRAGMA table_info("{table.name}")')}
                for col in table.columns:
                    if col.name not in have:
                        return f"{fname}:{table.name}.{col.name}"
        finally:
            con.close()
    return None


def _reset_control_plane(data_dir: Path) -> list[str]:
    """Delete the embedded control-plane SQLite DBs (and their -wal/-shm sidecars) so the next start
    rebuilds them at the current schema. The demo re-seeds from config; a non-demo install re-registers
    from config/UI. Returns the removed file names."""
    removed: list[str] = []
    for base in ("platform.db", "tenant.db"):
        for name in (base, f"{base}-wal", f"{base}-shm"):
            p = data_dir / name
            if p.exists():
                p.unlink()
                if name == base:
                    removed.append(name)
    return removed


async def _announce_ready(
    host: str, api_port: int, ui_port: int, *, demo: bool, open_browser: bool
) -> None:
    """Wait for the API to be genuinely warm (/ready 200 — the boot warmup probe has attached the
    store and warmed the engine), then print a completion line and open the browser. /ready (not
    /health) is the gate so the browser opens onto a warm app whose first query is not cold.

    Best-effort and non-fatal: any failure here must never take down the servers (they run in the
    same gather), and a timeout still tells the user how to open it manually. ``?tour=1`` auto-starts
    the guided tour for a demo run (App.tsx reads the query param)."""
    import httpx

    ready_url = f"http://{host}:{api_port}/ready"
    url = f"http://{host}:{ui_port}/?tour=1" if demo else f"http://{host}:{ui_port}/"
    deadline = 300  # seconds; the servers keep running past this — we just stop polling
    waited = 0.0
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            while waited < deadline:
                try:
                    if (await client.get(ready_url)).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass  # not up yet / still warming (503) — keep polling
                await asyncio.sleep(0.5)
                waited += 0.5
            else:
                print(f"\nProvisa is still starting — open {url} in your browser.", flush=True)
                return

        print(f"\n✓ Provisa is ready — {url}", flush=True)
        if open_browser:
            import webbrowser

            try:
                opened = webbrowser.open(url)
            except Exception:
                opened = False
            if not opened:
                print(f"  Open {url} in your browser to get started.", flush=True)
    except Exception as exc:  # never let the announcer crash the servers
        print(f"\nProvisa is running — open {url} in your browser (announce: {exc}).", flush=True)


async def _serve(
    host: str, api_port: int, ui_port: int, *, demo: bool, open_browser: bool
) -> None:
    import uvicorn

    from provisa.api.app import create_app

    # ui_server reads PROVISA_API_URL at import time to build its reverse-proxy target,
    # so it must be set before the module is imported.
    os.environ.setdefault("PROVISA_API_URL", f"http://127.0.0.1:{api_port}")
    from provisa import ui_server

    api = uvicorn.Server(
        uvicorn.Config(create_app, factory=True, host=host, port=api_port, log_level="info")
    )
    ui = uvicorn.Server(
        uvicorn.Config(ui_server.app, host=host, port=ui_port, log_level="warning")
    )
    await asyncio.gather(
        api.serve(),
        ui.serve(),
        _announce_ready(host, api_port, ui_port, demo=demo, open_browser=open_browser),
    )


def _cmd_run(args: argparse.Namespace) -> int:
    data_dir = Path(args.data_dir).expanduser()
    data_dir.mkdir(parents=True, exist_ok=True)

    # Control-plane schema currency (V1 has no migrations). --reset wipes the native DBs first;
    # otherwise detect drift up front and fail loud with the fix, rather than dying inside uvicorn's
    # swallowed lifespan ("no such column: ...").
    if args.reset:
        removed = _reset_control_plane(data_dir)
        if removed:
            print(f"  · reset control plane: removed {', '.join(removed)} (rebuilt on start)")
    drift = _control_plane_drift(data_dir)
    if drift:
        print(
            f"Control-plane store at {data_dir} is from an older Provisa (missing {drift}) and V1 "
            f"has no migrations.\nRe-run with --reset to rebuild it:  provisa run"
            f"{' --demo' if args.demo else ''} --reset",
            file=sys.stderr,
        )
        return 1

    demo_cfg: Path | None = None
    if args.demo:
        try:
            demo_cfg = _apply_demo_config()
        except FileNotFoundError as exc:
            print(str(exc), file=sys.stderr)
            return 1
    notes = _apply_embedded_env(data_dir)

    print("Provisa (embedded) starting — no Docker, no Node.")
    if demo_cfg is not None:
        print(f"  demo: {demo_cfg.name} — pet-store + shelter sample domains (embedded SQLite)")
    for note in notes:
        print(f"  · {note}")
    print(f"  UI:  http://127.0.0.1:{args.ui_port}")
    print(f"  API: http://127.0.0.1:{args.api_port}")

    try:
        asyncio.run(
            _serve(
                args.host,
                args.api_port,
                args.ui_port,
                demo=args.demo,
                open_browser=not args.no_browser,
            )
        )
    except KeyboardInterrupt:
        print("\nProvisa stopped.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="provisa", description="Provisa embedded runtime")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Start the embedded Provisa system (API + UI)")
    run.add_argument(
        "--demo",
        action="store_true",
        help="Load the bundled demo (pet-store + shelter sample federation over embedded SQLite)",
    )
    run.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    run.add_argument("--api-port", type=int, default=8000, help="API port (default: 8000)")
    run.add_argument("--ui-port", type=int, default=3000, help="UI port (default: 3000)")
    run.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not open a browser when the UI is ready (still prints the URL)",
    )
    run.add_argument(
        "--reset",
        action="store_true",
        help="Rebuild the embedded control-plane store before starting (discards local "
        "control-plane state; use after a Provisa upgrade if startup reports a schema mismatch)",
    )
    run.add_argument(
        "--data-dir",
        default=str(_DEFAULT_DATA_DIR),
        help=f"State directory for the SQLite control plane (default: {_DEFAULT_DATA_DIR})",
    )
    run.set_defaults(func=_cmd_run)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
