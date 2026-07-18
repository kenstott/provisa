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
    return profile.notes


async def _serve(host: str, api_port: int, ui_port: int) -> None:
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
    await asyncio.gather(api.serve(), ui.serve())


def _cmd_run(args: argparse.Namespace) -> int:
    data_dir = Path(args.data_dir).expanduser()
    data_dir.mkdir(parents=True, exist_ok=True)
    notes = _apply_embedded_env(data_dir)

    print("Provisa (embedded) starting — no Docker, no Node.")
    for note in notes:
        print(f"  · {note}")
    print(f"  UI:  http://127.0.0.1:{args.ui_port}")
    print(f"  API: http://127.0.0.1:{args.api_port}")

    try:
        asyncio.run(_serve(args.host, args.api_port, args.ui_port))
    except KeyboardInterrupt:
        print("\nProvisa stopped.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="provisa", description="Provisa embedded runtime")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Start the embedded Provisa system (API + UI)")
    run.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    run.add_argument("--api-port", type=int, default=8000, help="API port (default: 8000)")
    run.add_argument("--ui-port", type=int, default=3000, help="UI port (default: 3000)")
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
