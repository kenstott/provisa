# Copyright (c) 2026 Kenneth Stott
# Canary: 0348cd9c-f44e-4f7d-95ba-9ce55a4be2cf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a Node source's worker is started (REQ-1984): its runtime runs Provisa's host program
(``host.mjs``) on the bundle's entry module. The worker itself, its protocol and its pool are
every code source's (:mod:`provisa.codesource.worker`).

Deno runs the host under its own permissions: it reads the host, the bundle and the socket's
directory, writes the socket, and reaches only the hosts on the egress allow-list. Node and Bun
run under the operating system's sandbox (:mod:`provisa.codesource.sandbox`), reaching the
network only through the egress proxy."""

# Requirements: REQ-1984
from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.sandbox import sandboxed
from provisa.codesource.worker import (
    CODE_HASH_ENV,
    CONNECTIONS_ENV,
    HostCommand,
    private_environment,
)
from provisa.node.install import code_command

HOST = Path(__file__).with_name("host.mjs")
#: REQ-2106: "@provisa/services" for Deno, which resolves bare names only through an import map.
SERVICES_IMPORT_MAP = Path(__file__).with_name("services-import-map.json")

#: The files a package with no ``main`` or ``exports`` is entered by, in order.
_INDEXES = ("index.mjs", "index.js", "index.cjs")


def entry_module(bundle: Path) -> Path:
    """The module the bundle is entered by: its package's ``exports`` (``"."``, its ``import`` or
    ``default``), else its ``main``, else an ``index`` file. A bundle with none, or one that names
    a file it does not hold, is refused by name."""
    named = _package_entry(bundle)
    if named is not None:
        path = (bundle / named).resolve()
        if not path.is_relative_to(bundle.resolve()) or not path.is_file():
            raise BundleRefused(
                "no_entry",
                f"the package names {named} as its entry, and there is no such file",
                entry=named,
            )
        return path
    for name in _INDEXES:
        if (bundle / name).is_file():
            return (bundle / name).resolve()
    raise BundleRefused(
        "no_entry",
        "the bundle has no entry module (a package.json exports or main field, or index.js)",
    )


def _package_entry(bundle: Path) -> str | None:
    manifest = bundle / "package.json"
    if not manifest.is_file():
        return None
    package = json.loads(manifest.read_text())
    exports = package.get("exports")
    if isinstance(exports, dict):
        exports = exports.get(".", exports)
    if isinstance(exports, dict):
        exports = exports.get("import") or exports.get("default")
    if isinstance(exports, str):
        return exports
    main = package.get("main")
    return main if isinstance(main, str) else None


def host_command(
    *, runtime: str, executable: Path, bundle: Path, egress: Sequence[str], host_lib: Path
) -> HostCommand:
    """How ``runtime`` runs the host on ``bundle``. ``egress`` is the deployment's egress
    allow-list: hosts, or host:port. ``host_lib`` is where the host's Arrow library is
    (REQ-2098), which it sends its rows with."""
    entry = entry_module(bundle)
    env = private_environment(NO_COLOR="1", PROVISA_HOST_LIB=str(host_lib))
    if runtime in ("node", "bun"):
        return sandboxed(
            lambda address: [*code_command(runtime, executable), str(HOST), address, str(entry)],
            executable=executable,
            read=[HOST.parent, bundle, host_lib],
            env=env,
            cwd=bundle,
            egress=egress,
        )
    if runtime == "deno":
        from provisa.codesource.sandbox import egress_proxy
        from provisa.codesource.services import SERVICES_ENV, SERVICES_HOST

        # Deno's npm packages were installed into the bundle's own cache (node/install.py).
        env["DENO_DIR"] = str(bundle / ".deno")
        # REQ-2104, REQ-2106: its fetch goes through the egress proxy, as the other runtimes'
        # does, which answers Provisa's services itself; Deno checks each target against
        # --allow-net, so the services host is named there beside the allow-list's.
        proxy = egress_proxy(egress)
        env.update(proxy.environment())

        def _deno(address: str) -> list[str]:
            socket = Path(address)
            readable = ",".join(str(p) for p in (HOST.parent, bundle, socket.parent, host_lib))
            argv = [
                str(executable),
                "run",
                "--no-prompt",
                f"--allow-read={readable}",
                f"--allow-write={socket.parent}",
                # The hello names its code; the Arrow library is found by its path (REQ-2098);
                # the services' address and the proxy its fetch goes through (REQ-2106); how
                # many connections it makes, and the pool its descriptors' reads wait in.
                f"--allow-env={CODE_HASH_ENV},PROVISA_HOST_LIB,{SERVICES_ENV},"
                f"{CONNECTIONS_ENV},UV_THREADPOOL_SIZE,"
                "HTTP_PROXY,HTTPS_PROXY,http_proxy,https_proxy",
                f"--import-map={SERVICES_IMPORT_MAP}",
            ]
            # The socket is a net address to Deno; the proxy, the services host and the
            # allow-list's hosts are the only others.
            reach = [f"unix:{socket}", f"127.0.0.1:{proxy.port}", SERVICES_HOST, *egress]
            argv.append(f"--allow-net={','.join(reach)}")
            return [*argv, str(HOST), str(socket), str(entry)]

        return HostCommand(argv=_deno, env=env, cwd=bundle)
    raise BundleRefused("unknown_runtime", f"{runtime!r} is not a runtime", runtime=runtime)
