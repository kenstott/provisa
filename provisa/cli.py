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
import importlib.util
import os
import platform
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


def _resolve_base_config() -> Path:
    """Return the shipped minimal install skeleton (``provisa-install-base.yaml``): system sources +
    domains, the built-in ``admin`` role, empty tables, ``auth: none``. ``ProvisaConfig`` requires
    ``sources``/``domains``/``tables``/``roles``, so a fileless first-run install has no valid config
    for ``_load_and_build`` to parse; the setup wizard layers ``auth`` onto this base to produce one.

    Prefers the wheel-staged copy under ``provisa/_config`` (REQ-1127); falls back to the repo tree.
    """
    pkg_cfg = _PKG_ROOT / "_config" / "provisa-install-base.yaml"
    if pkg_cfg.exists():
        return pkg_cfg
    return _REPO_ROOT / "config" / "provisa-install-base.yaml"


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
    os.environ.setdefault("PROVISA_DEMO", "1")
    os.environ.setdefault("PROVISA_DEMO_DIR", str(data_dir))
    os.environ.setdefault("PROVISA_CONFIG_REPLACE", "true")
    return cfg


def _apply_embedded_env(data_dir: Path) -> list[str]:
    """Resolve and apply the embedded ("native") launch environment (REQ-1126, REQ-1129).

    Reuses the tested capabilities-preset resolver (desktop_profile.load_profile) so the
    embedded tier is the exact same self-contained runtime the desktop installer ships:
    DuckDB engine, embedded PostgreSQL control plane (REQ-1535), fakeredis cache. Existing process env wins
    (setdefault) so a customer-provided external engine (TRINO_HOST/PORT, PROVISA_ENGINE_URL)
    layered on before launch is preserved.
    """
    from provisa.core.desktop_profile import load_profile

    profile = load_profile("native", data_dir=data_dir)
    for key, value in profile.env.items():
        os.environ.setdefault(key, value)
    notes = list(profile.notes)

    # Stage the DuckDB extensions OFFLINE from the provisa-duckdb-ext PyPI package (installed by
    # provisa[embedded]) so DEPLOY never reaches extensions.duckdb.org — required behind an enterprise
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


async def _control_plane_drift() -> str | None:
    """Return a ``plane:table.column`` description of the FIRST schema drift in the embedded control
    plane, else None.

    V1 has no migrations (``create_all`` never ALTERs an existing table), so a native DB left by an
    OLDER Provisa whose table is missing a column the current ORM writes crashes startup with e.g.
    ``no such column: load_protected`` — and uvicorn swallows that inside its lifespan, so the app
    just dies with no useful message. This detects it BEFORE serving so ``run`` can fail loud with a
    ``--reset`` hint. Only MISSING columns are drift; extra DB columns (newer DB on older code) are
    not this failure mode and are ignored.

    Read AFTER the launch environment is applied (REQ-1535): the embedded plane is a PostgreSQL
    instance whose socket the profile resolver starts and names, so the URL is the only way to
    reach it — there is no file to stat. Both planes are that one instance, and each is inspected
    under its own metadata because they own different tables in it.
    """
    import sqlalchemy as sa

    from provisa.core import schema_admin, schema_org
    from provisa.core.database import create_engine_from_url

    for plane, meta in (
        ("platform", schema_admin.metadata),
        ("tenant", schema_org.metadata),
    ):
        engine = create_engine_from_url(os.environ[f"{plane.upper()}_DATABASE_URL"])
        try:
            async with engine.connect() as conn:
                present = set(await conn.run_sync(lambda c: sa.inspect(c).get_table_names()))
                for table in meta.tables.values():
                    if table.name not in present:
                        continue  # a table the ORM will create on start — not drift
                    have = {
                        c["name"]
                        for c in await conn.run_sync(
                            lambda c, _t=table.name: sa.inspect(c).get_columns(_t)
                        )
                    }
                    for col in table.columns:
                        if col.name not in have:
                            return f"{plane}:{table.name}.{col.name}"
        finally:
            await engine.dispose()
    return None


def _reset_control_plane(data_dir: Path) -> list[str]:
    """Drop the embedded control-plane database so the next start rebuilds it at the current schema.

    REQ-1535 makes that plane a bundled PostgreSQL, so a pristine start is a dropped database rather
    than deleted files — the data directory is the SERVER's and holds the cluster the next start
    boots. The demo re-seeds from config; a non-demo install re-registers from config/UI. Returns
    what was dropped, empty when the install has never been started.
    """
    from provisa.core.control_plane_pg import reset

    pg_dir = data_dir / "control-pg"
    if not pg_dir.exists():
        return []
    reset(str(pg_dir))
    return ["database provisa"]


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


# uvicorn's 5s default expires an idle connection while the browser still holds it pooled, and the
# cloud front door splices client to backend one-to-one, so that close tears down the browser's
# connection too. The browser only finds out when it writes the next request into the dead socket,
# and it does not retry a POST — which is how filling in a form for more than five seconds ended in
# "Failed to fetch". This sits above the front door's 300s splice idle timeout so the proxy, not
# uvicorn, is what closes first.
KEEP_ALIVE_SECONDS = 620


async def _serve(host: str, api_port: int, ui_port: int, *, demo: bool, open_browser: bool) -> None:
    import uvicorn

    from provisa.api.app import create_app

    # ui_server reads PROVISA_API_URL at import time to build its reverse-proxy target,
    # so it must be set before the module is imported.
    os.environ.setdefault("PROVISA_API_URL", f"http://127.0.0.1:{api_port}")
    from provisa import ui_server

    api = uvicorn.Server(
        uvicorn.Config(
            create_app,
            factory=True,
            host=host,
            port=api_port,
            log_level="info",
            timeout_keep_alive=KEEP_ALIVE_SECONDS,
        )
    )
    ui = uvicorn.Server(
        uvicorn.Config(
            ui_server.app,
            host=host,
            port=ui_port,
            log_level="warning",
            timeout_keep_alive=KEEP_ALIVE_SECONDS,
        )
    )
    await asyncio.gather(
        api.serve(),
        ui.serve(),
        _announce_ready(host, api_port, ui_port, demo=demo, open_browser=open_browser),
    )


def _cmd_license_apply(args: argparse.Namespace) -> int:
    """`provisa license apply <file>` — verify + install a license offline (REQ-1139)."""
    from provisa.licensing import apply_license

    result = apply_license(Path(args.file).expanduser())
    if result.valid:
        print("License applied. The trial nag is now silenced on all surfaces.")
        return 0
    print(f"License rejected: {result.reason}", file=sys.stderr)
    return 1


def _cmd_license_status(args: argparse.Namespace) -> int:  # noqa: ARG001
    """`provisa license status` — show machine id, trial state, and license validity (REQ-1139)."""
    import datetime

    from provisa.licensing.state import evaluate

    today = datetime.date.today()
    st = evaluate(
        now_epoch=today.toordinal() * 86400,
        today_iso=today.isoformat(),
    )
    print(f"Machine ID:   {st.machine_id}")
    print(f"First seen:   {st.first_seen}")
    print(f"Elapsed:      {st.elapsed_days:.1f} days")
    print(f"Trial:        {'EXPIRED' if st.trial_expired else 'active'}")
    print(f"Licensed:     {'yes' if st.licensed else f'no ({st.license_reason})'}")
    return 0


def _cmd_metadata_export(args: argparse.Namespace) -> int:
    """`provisa metadata export` — trigger the running server's on-demand metadata publish.

    A thin client for POST /admin/metadata-export/publish (REQ-1072/REQ-1074): the server owns
    the single publish path (entitlement gate, org runtime, snapshot build), so a cron-driven
    export sends exactly what the admin tab's Publish now sends. Under multitenancy the org is
    named by the API host (acme.provisa.org), the same way every other client names it.
    """
    import json
    import urllib.error
    import urllib.request

    api = args.api or os.environ.get("PROVISA_API_URL", "http://127.0.0.1:8000")
    token = args.token or os.environ.get("PROVISA_API_TOKEN", "")
    req = urllib.request.Request(
        f"{api.rstrip('/')}/admin/metadata-export/publish", method="POST", data=b""
    )
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        # B310 wants the scheme pinned: the URL is built from the operator's own --api
        # argument, so it is http(s) by construction, never file:// or a custom handler.
        with urllib.request.urlopen(req, timeout=args.timeout) as resp:  # nosec B310
            payload = json.load(resp)
    except urllib.error.HTTPError as exc:
        print(
            f"publish failed: HTTP {exc.code} — {exc.read().decode(errors='replace')}",
            file=sys.stderr,
        )
        return 1
    except urllib.error.URLError as exc:
        print(f"cannot reach the Provisa API at {api}: {exc.reason}", file=sys.stderr)
        return 1
    ok = payload["ok"]
    print(
        f"{'ok' if ok else 'PARTIAL'}: published {payload['total_published']} assets "
        f"via {payload['provider']}"
    )
    for err in payload["errors"]:
        print(f"  ! {err['asset']}: {err['message']}", file=sys.stderr)
    return 0 if ok else 1


def _api_call(args: argparse.Namespace, method: str, path: str, body: dict | None = None) -> dict:
    """One authenticated call to the Provisa API, returning the decoded JSON.

    The CLI is a THIN CLIENT of the same endpoint the UI calls (REQ-1496): same target, same
    capability check, same report. A deployment pipeline holding a credential that passes that
    check is the organization delegating its own standing, which is the org's decision to make and
    revocable as any other credential is.
    """
    import json
    import urllib.error
    import urllib.request

    api = args.api or os.environ.get("PROVISA_API_URL", "http://127.0.0.1:8000")
    token = args.token or os.environ.get("PROVISA_API_TOKEN", "")
    url = f"{api.rstrip('/')}{path}"
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, method=method, data=data)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        # B310 wants the scheme pinned: the URL is built from the operator's own --api
        # argument, so it is http(s) by construction, never file:// or a custom handler.
        with urllib.request.urlopen(req, timeout=args.timeout) as resp:  # nosec B310
            return json.load(resp)
    except urllib.error.HTTPError as exc:
        raise SystemExit(
            f"{method} {path} failed: HTTP {exc.code} — {exc.read().decode(errors='replace')}"
        ) from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"cannot reach the Provisa API at {api}: {exc.reason}") from exc


def _print_deploy(result: dict) -> None:
    """The REQ-1490 report as a deploy log reads it: what would change, by path."""
    if result.get("requires_approval") and not result["applied"]:
        request = result["request"]
        report = request["report"]
        print(f"Deploy PROPOSED as request {request['id']} — {request['target_env']} is protected.")
    else:
        report = result["report"]
        print(f"Deploy {'APPLIED to' if result['applied'] else 'PLANNED for'} {report['env']}")
    print(f"  ref       {report['ref']}")
    for kind in ("added", "changed", "removed"):
        for path in report[kind]:
            print(f"  {kind[0].upper()} {path}")
    print(f"  unchanged {report['unchanged']}")


def _cmd_env_deploy(args: argparse.Namespace) -> int:
    """`provisa env deploy` — make the tree at a ref an environment's model (REQ-1496).

    This is the command a deployment pipeline runs, and the rule it obeys is the one the UI obeys:
    a deploy is an invocation CARRYING AN IDENTITY against a NAMED control plane. Nothing inside a
    Provisa deployment applies a merged branch on its own; the pipeline holding a credential is
    what makes the deploy happen, and revoking that credential is what stops it.
    """
    result = _api_call(
        args,
        "POST",
        f"/admin/orgs/{args.org}/environments/{args.env}/deploy",
        {
            "ref": args.ref,
            "dry_run": args.dry_run,
            "seed": args.seed,
            "message": args.message or "",
        },
    )
    _print_deploy(result)
    # A proposal is not a deployment. A pipeline that treated a pending approval as success would
    # report a release that has not happened, so the exit code says so.
    return 0 if result["applied"] or args.dry_run else 2


def _cmd_env_fetch(args: argparse.Namespace) -> int:
    """`provisa env fetch` — bring the org's remote branches back (REQ-1541).

    The step BEFORE a deploy in the ordinary flow: the pipeline that just saw a pull request merge
    on the org's git host runs this, and then deploys ``origin/<branch>``. Provisa never runs it on
    a timer, so the branch a deploy names is one somebody fetched deliberately.
    """
    result = _api_call(
        args, "POST", f"/admin/orgs/{args.org}/environments/-/repo-integration/fetch", {}
    )
    for name, sha in sorted(result["branches"].items()):
        print(f"origin/{name}  {sha[:12]}")
    return 0


def _maintenance_request(args: argparse.Namespace, body: dict | None) -> dict:
    """One call to /admin/platform/maintenance. GET when ``body`` is None, PUT otherwise.

    A thin client, like ``metadata export``: the server owns the wording, the ``started_at`` stamp
    and the ``platform_settings`` gate, so the CLI and the admin tab produce the same notice
    (REQ-1466).
    """
    import json
    import urllib.error
    import urllib.request

    api = args.api or os.environ.get("PROVISA_API_URL", "http://127.0.0.1:8000")
    token = args.token or os.environ.get("PROVISA_API_TOKEN", "")
    url = f"{api.rstrip('/')}/admin/platform/maintenance"
    if body is None:
        req = urllib.request.Request(url, method="GET")
    else:
        req = urllib.request.Request(
            url,
            method="PUT",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        # B310 wants the scheme pinned: the URL is built from the operator's own --api
        # argument, so it is http(s) by construction, never file:// or a custom handler.
        with urllib.request.urlopen(req, timeout=args.timeout) as resp:  # nosec B310
            return json.load(resp)
    except urllib.error.HTTPError as exc:
        raise SystemExit(
            f"maintenance request failed: HTTP {exc.code} — {exc.read().decode(errors='replace')}"
        ) from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"cannot reach the Provisa API at {api}: {exc.reason}") from exc


def _print_notice(notice: dict) -> None:
    if not notice["active"]:
        print("Maintenance notice: OFF")
        return
    print("Maintenance notice: ON")
    print(f"  Message:  {notice['message']}")
    print(f"  Since:    {notice['started_at'] or 'unknown'}")
    print(f"  Ends at:  {notice['ends_at'] or 'no estimate given'}")


def _cmd_maintenance_on(args: argparse.Namespace) -> int:
    """`provisa maintenance on` — raise the scheduled-downtime banner (REQ-1466).

    The command an operator runs first when planned work takes the data plane down — switching
    ``var.engine_cluster_mode`` replaces the engine cluster and every shard on it (REQ-1465), so
    queries fail for the duration and the failure has to read as scheduled.
    """
    _print_notice(
        _maintenance_request(
            args, {"active": True, "message": args.message, "ends_at": args.ends_at}
        )
    )
    return 0


def _cmd_maintenance_off(args: argparse.Namespace) -> int:
    """`provisa maintenance off` — clear the banner once the work is done (REQ-1466)."""
    _print_notice(_maintenance_request(args, {"active": False, "message": None, "ends_at": None}))
    return 0


def _cmd_maintenance_status(args: argparse.Namespace) -> int:
    """`provisa maintenance status` — what the banner is currently saying (REQ-1466)."""
    _print_notice(_maintenance_request(args, None))
    return 0


_SUPPORTED_PY = (3, 12)


def _require_supported_interpreter() -> None:
    """Abort ``provisa run`` on a platform the embedded runtime cannot boot on.

    The native control plane and telemetry store are an embedded PostgreSQL (REQ-1535) supplied by
    pgserver, which publishes cp39-cp312 wheels for linux x86_64 / macOS / win_amd64 and no sdist.
    pyproject encodes that matrix (requires-python <3.13 plus a linux-aarch64 marker) so pip refuses
    or omits it, but a source checkout or hand-built venv bypasses the resolver — those must die HERE
    with the fix, not 60 frames deep in ModuleNotFoundError: pgserver.
    """
    if sys.version_info[:2] != _SUPPORTED_PY:
        _abort_unsupported_interpreter()
    if importlib.util.find_spec("pgserver") is None:
        raise SystemExit(
            "Provisa's embedded PostgreSQL control plane needs pgserver, which is not installed "
            f"for this platform ({sys.platform}/{platform.machine()}).\n"
            "pgserver publishes wheels for linux x86_64, macOS and Windows x86_64 only — there is "
            "no linux/aarch64 build and no source distribution, so the embedded (`provisa run`) "
            "tier does not run here.\n"
            "Use an x86_64 host, or run the container tier, which uses a real PostgreSQL control "
            "plane instead of the embedded one."
        )


def _abort_unsupported_interpreter() -> None:
    """The interpreter half of :func:`_require_supported_interpreter`."""
    have = "%d.%d" % sys.version_info[:2]
    want = "%d.%d" % _SUPPORTED_PY
    raise SystemExit(
        f"Provisa requires Python {want}; this interpreter is {have} ({sys.executable}).\n"
        f"The embedded PostgreSQL control plane is supplied by pgserver, which ships no wheel for "
        f"{have} (and no source distribution), so the runtime cannot start.\n"
        f"Create the environment on {want} and reinstall:\n"
        f"    python{want} -m venv .venv && .venv/bin/pip install 'provisa[embedded]'"
    )


def _cmd_run(args: argparse.Namespace) -> int:
    _require_supported_interpreter()
    data_dir = Path(args.data_dir).expanduser()
    data_dir.mkdir(parents=True, exist_ok=True)

    # Control-plane schema currency (V1 has no migrations). --reset wipes the native DBs first;
    # otherwise detect drift up front and fail loud with the fix, rather than dying inside uvicorn's
    # swallowed lifespan ("no such column: ...").
    if args.reset:
        removed = _reset_control_plane(data_dir)
        if removed:
            print(f"  · reset control plane: dropped {', '.join(removed)} (rebuilt on start)")

    demo_cfg: Path | None = None
    if args.demo:
        try:
            demo_cfg = _apply_demo_config()
        except FileNotFoundError as exc:
            print(str(exc), file=sys.stderr)
            return 1
    # The launch environment starts the embedded plane and publishes its URLs, so drift is read
    # after it and not before: the check needs the socket the resolver has just chosen (REQ-1535).
    notes = _apply_embedded_env(data_dir)
    drift = asyncio.run(_control_plane_drift())
    if drift:
        print(
            f"Control-plane store at {data_dir} is from an older Provisa (missing {drift}) and V1 "
            f"has no migrations.\nRe-run with --reset to rebuild it:  provisa run"
            f"{' --demo' if args.demo else ''} --reset",
            file=sys.stderr,
        )
        return 1

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
        help="Deploy the bundled demo (pet-store + shelter sample federation over embedded SQLite)",
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

    # REQ-1139: offline license application + status.
    lic = sub.add_parser("license", help="Manage the Provisa license (offline)")
    lic_sub = lic.add_subparsers(dest="license_command", required=True)
    lic_apply = lic_sub.add_parser("apply", help="Verify and install a license file")
    lic_apply.add_argument("file", help="Path to the license.json issued by provisa.dev")
    lic_apply.set_defaults(func=_cmd_license_apply)
    lic_status = lic_sub.add_parser(
        "status", help="Show machine id, trial state, and license status"
    )
    lic_status.set_defaults(func=_cmd_license_status)

    meta = sub.add_parser("metadata", help="Governed-metadata operations")
    meta_sub = meta.add_subparsers(dest="metadata_command", required=True)
    meta_export = meta_sub.add_parser(
        "export",
        help="Publish the full metadata snapshot to the org's configured catalog "
        "(the on-demand REQ-1072 reconcile, runnable from cron)",
    )
    meta_export.add_argument(
        "--api",
        default=None,
        help="Provisa API base URL (default: $PROVISA_API_URL or http://127.0.0.1:8000). "
        "Under multitenancy the host names the org (acme.provisa.org)",
    )
    meta_export.add_argument(
        "--token",
        default=None,
        help="Bearer token for an identity holding org_settings (default: $PROVISA_API_TOKEN; "
        "unauthenticated deployments need none)",
    )
    meta_export.add_argument(
        "--timeout", type=int, default=300, help="Publish timeout in seconds (default: 300)"
    )
    meta_export.set_defaults(func=_cmd_metadata_export)

    # REQ-1496: the deploy, as a command, so a deployment pipeline can perform it. The rule is not
    # that a machine may never deploy -- it is that a deploy is always an invocation carrying an
    # identity against a named control plane, never something a control plane does to itself on
    # noticing a commit.
    env = sub.add_parser("env", help="Environment operations against a running Provisa")
    env_sub = env.add_subparsers(dest="env_command", required=True)
    env_deploy = env_sub.add_parser(
        "deploy", help="Deploy the model at a ref into an environment, making it that environment's"
    )
    env_deploy.add_argument("--org", required=True, help="Organization holding the environment")
    env_deploy.add_argument("--env", required=True, help="Environment that will hold the result")
    env_deploy.add_argument(
        "--ref", required=True, help="Branch or commit in the org's repository to deploy"
    )
    env_deploy.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what the deploy would do and apply none of it",
    )
    env_deploy.add_argument(
        "--seed",
        action="store_true",
        help="Also apply the creation-only classes (roles). Only correct when this deploy is what "
        "creates the environment: a tree carries the roles of whatever control plane projected it",
    )
    env_deploy.add_argument("--message", default=None, help="Note carried onto an approval request")
    env_deploy.add_argument(
        "--api",
        default=None,
        help="Provisa API base URL (default: $PROVISA_API_URL or http://127.0.0.1:8000)",
    )
    env_deploy.add_argument(
        "--token",
        default=None,
        help="Bearer token for an identity that may write the environment "
        "(default: $PROVISA_API_TOKEN)",
    )
    env_deploy.add_argument(
        "--timeout", type=int, default=300, help="Request timeout in seconds (default: 300)"
    )
    env_deploy.set_defaults(func=_cmd_env_deploy)

    # REQ-1541: the other direction. The projection is pushed out, the review and the merge happen
    # on the org's own git host, and this is what brings the result back as ``origin/<branch>`` for
    # a deploy to name.
    env_fetch = env_sub.add_parser(
        "fetch", help="Fetch the org's remote branches into its Provisa repository"
    )
    env_fetch.add_argument("--org", required=True, help="Organization whose remote is fetched")
    env_fetch.add_argument(
        "--api",
        default=None,
        help="Provisa API base URL (default: $PROVISA_API_URL or http://127.0.0.1:8000)",
    )
    env_fetch.add_argument(
        "--token",
        default=None,
        help="Bearer token for an org administrator (default: $PROVISA_API_TOKEN)",
    )
    env_fetch.add_argument(
        "--timeout", type=int, default=300, help="Request timeout in seconds (default: 300)"
    )
    env_fetch.set_defaults(func=_cmd_env_fetch)

    # REQ-1466: the same on/off control the platform admin has in the UI, runnable from a deploy
    # script — the banner goes up before the engine-cluster switch (REQ-1465) and down after it,
    # and neither step should need a browser.
    maint = sub.add_parser(
        "maintenance", help="Raise or clear the deployment's scheduled-maintenance banner"
    )
    maint_sub = maint.add_subparsers(dest="maintenance_command", required=True)
    maint_on = maint_sub.add_parser("on", help="Show the scheduled-maintenance banner")
    maint_on.add_argument(
        "--message",
        default=None,
        help="Override the deployment's standard wording (default: the server's standard message)",
    )
    maint_on.add_argument(
        "--ends-at",
        default=None,
        help="ISO-8601 instant the work is expected to end, e.g. 2026-08-14T22:30:00Z "
        "(default: state that no estimate is being offered)",
    )
    maint_on.set_defaults(func=_cmd_maintenance_on)
    maint_off = maint_sub.add_parser("off", help="Clear the scheduled-maintenance banner")
    maint_off.set_defaults(func=_cmd_maintenance_off)
    maint_status = maint_sub.add_parser("status", help="Show the current maintenance notice")
    maint_status.set_defaults(func=_cmd_maintenance_status)
    for p in (maint_on, maint_off, maint_status):
        p.add_argument(
            "--api",
            default=None,
            help="Provisa API base URL (default: $PROVISA_API_URL or http://127.0.0.1:8000)",
        )
        p.add_argument(
            "--token",
            default=None,
            help="Bearer token for an identity holding platform_settings "
            "(default: $PROVISA_API_TOKEN)",
        )
        p.add_argument(
            "--timeout", type=int, default=30, help="Request timeout in seconds (default: 30)"
        )

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
