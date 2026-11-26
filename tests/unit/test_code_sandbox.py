# Copyright (c) 2026 Kenneth Stott
# Canary: d308d067-b6b5-49e4-9705-1a2bf18c1786
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The sandbox a code source's worker runs in (REQ-1984, REQ-1993), as data: the seccomp program
judged call by call by a BPF evaluator, the launcher's argv, the macOS profile, and which sandbox
each platform gets. That each holds on a real kernel is the sandbox probe's (CI) and the worker
integration tests' to show."""

# Requirements: REQ-1984, REQ-1993
from __future__ import annotations

import struct
import sys
from pathlib import Path

import pytest

from provisa.codesource import linux_sandbox as lx
from provisa.codesource import macos_sandbox, sandbox
from provisa.codesource.linux_sandbox import SandboxUnavailable

pytestmark = pytest.mark.unit

ALLOW, REFUSE = "allow", "refuse"
AF_INET, AF_INET6, AF_UNIX, AF_NETLINK, AF_PACKET = 2, 10, 1, 16, 17
SOCK_STREAM, SOCK_DGRAM, SOCK_RAW = 1, 2, 3
SOCK_NONBLOCK, SOCK_CLOEXEC = 0o4000, 0o2000000


def _judge(program: bytes, *, arch: int, nr: int, args: tuple[int, ...] = ()) -> str:
    """Run the classic-BPF ``program`` over one seccomp_data, as the kernel does."""
    data = struct.pack("=iIQ6Q", nr, arch, 0, *(list(args) + [0] * (6 - len(args))))
    insns = [struct.unpack("=HBBI", program[i : i + 8]) for i in range(0, len(program), 8)]
    acc, pc = 0, 0
    while True:
        code, jt, jf, k = insns[pc]
        if code == 0x20:  # LD W ABS
            acc = struct.unpack_from("=I", data, k)[0]
        elif code == 0x54:  # ALU AND K
            acc &= k
        elif code == 0x15:  # JEQ K
            pc += jt if acc == k else jf
        elif code == 0x35:  # JGE K
            pc += jt if acc >= k else jf
        elif code == 0x06:  # RET K
            return ALLOW if k == 0x7FFF0000 else REFUSE
        else:
            raise AssertionError(f"instruction {code:#x} the evaluator does not know")
        pc += 1


@pytest.mark.parametrize("machine", ["x86_64", "aarch64"])
@pytest.mark.parametrize(
    ("call", "args", "expected"),
    [
        ("socket", (AF_INET, SOCK_STREAM), ALLOW),
        ("socket", (AF_INET6, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC), ALLOW),
        ("socket", (AF_INET, SOCK_DGRAM), REFUSE),  # UDP: DNS around the proxy
        ("socket", (AF_INET6, SOCK_DGRAM | SOCK_CLOEXEC), REFUSE),
        ("socket", (AF_INET, SOCK_RAW), REFUSE),
        ("socket", (AF_UNIX, SOCK_STREAM), REFUSE),  # the host's Docker or Postgres socket
        ("socket", (AF_NETLINK, SOCK_RAW), REFUSE),
        ("socket", (AF_PACKET, SOCK_RAW), REFUSE),
        ("socketpair", (AF_UNIX, SOCK_STREAM), ALLOW),  # asyncio's self-pipe; reaches nothing
        ("io_uring_setup", (), REFUSE),  # opens sockets without socket()
        ("io_uring_enter", (), REFUSE),
        ("seccomp", (), ALLOW),  # a stricter filter of the code's own may be added
    ],
)
def test_the_filter_allows_only_tcp_sockets(machine, call, args, expected):
    program = lx.socket_filter(machine)
    nr = lx._SYSCALLS[machine][call]
    assert _judge(program, arch=lx._AUDIT_ARCH[machine], nr=nr, args=args) == expected


@pytest.mark.parametrize("machine", ["x86_64", "aarch64"])
def test_every_other_call_is_allowed(machine):
    program = lx.socket_filter(machine)
    for nr in (0, 1, 3, 59, 202, 231):  # read, write, close, execve, futex, exit_group (x86_64)
        assert _judge(program, arch=lx._AUDIT_ARCH[machine], nr=nr) == ALLOW


def test_another_architectures_calls_and_x32_calls_are_refused():
    program = lx.socket_filter("x86_64")
    assert _judge(program, arch=0x40000003, nr=0) == REFUSE  # i386
    assert _judge(program, arch=lx._AUDIT_ARCH["x86_64"], nr=0x40000000 | 41) == REFUSE


def test_the_launcher_starts_from_anywhere_with_no_provisa_package_on_the_path(tmp_path):
    """Isolated (-I), from a directory outside the source, with no site-packages (-S, so no
    installed or editable provisa): as an image that runs Provisa from /app does. As a module (-m)
    it was not found there ("Error while finding module specification for
    'provisa.codesource.linux_sandbox'", churnprod on the sandbox tier); by its path it starts,
    then runs the host or says the sandbox is unavailable where the kernel has no Landlock."""
    import subprocess

    # On a Nix host /bin/true resolves into the store, which a worker reads only as the parent
    # grants it (sandbox._linux_readable); elsewhere the grant is empty and /usr serves it.
    read = lx.nix_store_grant("/bin/true", [], {})
    argv = lx.launcher_argv(sys.executable, read=read, proxy_port=1, argv=["/bin/true"])
    bare = [argv[0], "-I", "-S"]
    started = subprocess.run(
        [*bare, *argv[2:]], cwd=tmp_path, capture_output=True, text=True, env={}
    )
    assert started.returncode in (0, 3), started.stderr
    assert started.returncode == 0 or "code-source sandbox unavailable" in started.stderr
    as_module = subprocess.run(
        [*bare, "-m", "provisa.codesource.linux_sandbox", "--probe"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        env={},
    )
    assert "provisa" in as_module.stderr and as_module.returncode != 0


def test_the_launcher_confines_then_runs_the_host():
    argv = lx.launcher_argv(
        "/opt/provisa/bin/python",
        read=["/cache/code-bundles/node/abc", "/cache/code/node/24.21.0"],
        proxy_port=40123,
        argv=["/cache/code/node/24.21.0/bin/node", "host.mjs", "fd:5", "/b/index.js"],
    )
    assert argv == [
        "/opt/provisa/bin/python",
        "-I",
        lx.__file__,
        "--proxy-port",
        "40123",
        "--read",
        "/cache/code-bundles/node/abc",
        "--read",
        "/cache/code/node/24.21.0",
        "--",
        "/cache/code/node/24.21.0/bin/node",
        "host.mjs",
        "fd:5",
        "/b/index.js",
    ]


def test_a_confined_worker_is_not_given_the_whole_nix_store(monkeypatch):
    """Maintainer 2026-11-26: the whole store holds every other program's files, and on NixOS
    /etc/hosts and the rest of /etc resolve into it; a worker gets only its runtime's closure
    (:func:`lx.nix_store_grant`). With /nix/store granted, the python, go, rust and node workers
    read /etc/hosts (run 45720472876)."""
    granted: list = []
    monkeypatch.setattr(lx, "apply_landlock", lambda read, ports: granted.extend(read) or 4)
    monkeypatch.setattr(lx, "apply_socket_filter", lambda: None)
    lx.confine(["/cache/code/python"], 40123)
    assert "/cache/code/python" in granted
    assert not any(p == "/nix/store" or p.startswith("/nix/") for p in granted)


def _elf64(interpreter: str) -> bytes:
    """A little-endian ELF64 executable header with one PT_INTERP program header."""
    interp = interpreter.encode() + b"\0"
    header = b"\x7fELF" + bytes([2, 1, 1]) + bytes(9)
    header += struct.pack("<HHIQQQIHHHHHH", 2, 62, 1, 0, 64, 0, 0, 64, 56, 1, 64, 0, 0)
    phdr = struct.pack("<IIQQQQQQ", 3, 4, 64 + 56, 0, 0, len(interp), len(interp), 1)
    return header + phdr + interp


def _elf32(interpreter: str) -> bytes:
    interp = interpreter.encode() + b"\0"
    header = b"\x7fELF" + bytes([1, 1, 1]) + bytes(9)
    header += struct.pack("<HHIIIIIHHHHHH", 2, 3, 1, 0, 52, 0, 0, 52, 32, 1, 40, 0, 0)
    phdr = struct.pack("<IIIIIIII", 3, 52 + 32, 0, 0, len(interp), len(interp), 4, 1)
    return header + phdr + interp


def test_the_loader_is_read_from_the_executables_program_headers(tmp_path):
    wide = tmp_path / "wide"
    wide.write_bytes(_elf64("/lib64/ld-linux-x86-64.so.2"))
    narrow = tmp_path / "narrow"
    narrow.write_bytes(_elf32("/lib/ld-linux.so.2"))
    script = tmp_path / "script"
    script.write_text("#!/bin/sh\n")
    assert lx.elf_interpreter(str(wide)) == "/lib64/ld-linux-x86-64.so.2"
    assert lx.elf_interpreter(str(narrow)) == "/lib/ld-linux.so.2"
    assert lx.elf_interpreter(str(script)) is None


def _fake_store(tmp_path: Path, monkeypatch) -> tuple[Path, list]:
    store = (tmp_path / "nix" / "store").resolve()
    store.mkdir(parents=True)
    monkeypatch.setattr(lx, "NIX_STORE", str(store))
    monkeypatch.setattr(lx, "SYSTEM_READ", ())
    lx._closure.cache_clear()
    asked: list = []

    class _Done:
        returncode = 0
        stderr = ""

        def __init__(self, paths):
            self.stdout = "".join(f"{p}\n{p}-dep\n" for p in paths)

    def _run(argv, **kwargs):
        asked.append(argv)
        return _Done(argv[3:])

    monkeypatch.setattr(lx.shutil, "which", lambda name: "/run/current-system/sw/bin/nix-store")
    monkeypatch.setattr(lx.subprocess, "run", _run)
    return store, asked


def test_a_nix_worker_reads_the_closure_of_its_runtime_loader_host_and_nix_ld_libraries(
    tmp_path, monkeypatch
):
    """Only the store paths the worker runs on: the runtime's (outside the store, its loader's:
    nix-ld), the host program's, and the libraries nix-ld loads it with, each with its closure
    (nix-store -qR). Not the store path /etc/hosts resolves to."""
    store, asked = _fake_store(tmp_path, monkeypatch)
    nix_ld = store / "aaa-nix-ld" / "libexec" / "nix-ld"
    nix_ld.parent.mkdir(parents=True)
    nix_ld.write_text("")
    lib64 = tmp_path / "lib64"
    lib64.mkdir()
    (lib64 / "ld-linux-x86-64.so.2").symlink_to(nix_ld)
    runtime = tmp_path / "uv" / "bin" / "python3.12"  # downloaded, outside the store
    runtime.parent.mkdir(parents=True)
    runtime.write_bytes(_elf64(str(lib64 / "ld-linux-x86-64.so.2")))
    host = store / "bbb-provisa" / "lib" / "host"
    host.mkdir(parents=True)
    zlib = store / "ccc-zlib" / "lib" / "libz.so.1"
    zlib.parent.mkdir(parents=True)
    zlib.write_text("")
    libs = tmp_path / "nix-ld-lib"
    libs.mkdir()
    (libs / "libz.so.1").symlink_to(zlib)
    hosts = store / "ddd-etc-hosts"
    hosts.write_text("127.0.0.1 localhost\n")

    grant = lx.nix_store_grant(
        str(runtime),
        [str(host), str(runtime.parent)],
        {"PATH": "/usr/bin", "NIX_LD_LIBRARY_PATH": str(libs)},
    )
    seeds = [str(store / n) for n in ("aaa-nix-ld", "bbb-provisa", "ccc-zlib")]
    assert asked == [["/run/current-system/sw/bin/nix-store", "--query", "--requisites", *seeds]]
    assert grant == [p for s in seeds for p in (s, f"{s}-dep")]
    assert str(hosts) not in grant and str(store) not in grant


def test_with_no_nix_ld_library_path_the_grant_names_nix_lds_own_library_directory(
    tmp_path, monkeypatch
):
    """A worker's environment carries no NIX_LD_LIBRARY_PATH: nix-ld then loads from its own
    directory, and the grant names the libraries there."""
    store, asked = _fake_store(tmp_path, monkeypatch)
    glibc = store / "eee-glibc" / "lib" / "ld-linux-x86-64.so.2"
    glibc.parent.mkdir(parents=True)
    glibc.write_text("")
    libs = tmp_path / "sw" / "share" / "nix-ld" / "lib"
    libs.mkdir(parents=True)
    (libs / "ld.so").symlink_to(glibc)
    monkeypatch.setattr(lx, "NIX_LD_DEFAULT_LIBRARY_PATH", str(libs))
    runtime = tmp_path / "go-host"
    runtime.write_bytes(b"static")
    grant = lx.nix_store_grant(str(runtime), [str(tmp_path)], {"PATH": "/usr/bin"})
    assert grant == [str(store / "eee-glibc"), f"{store / 'eee-glibc'}-dep"]


def test_a_host_with_no_nix_store_grants_nothing_of_it(tmp_path, monkeypatch):
    monkeypatch.setattr(lx, "NIX_STORE", str(tmp_path / "no-store"))
    runtime = tmp_path / "node"
    runtime.write_bytes(_elf64("/lib64/ld-linux-x86-64.so.2"))
    assert lx.nix_store_grant(str(runtime), [str(tmp_path)], {}) == []


def test_a_nix_host_without_nix_store_on_path_refuses_the_worker_by_name(tmp_path, monkeypatch):
    store, _ = _fake_store(tmp_path, monkeypatch)
    monkeypatch.setattr(lx.shutil, "which", lambda name: None)
    pkg = store / "fff-node" / "bin" / "node"
    pkg.parent.mkdir(parents=True)
    pkg.write_bytes(b"static")
    with pytest.raises(SandboxUnavailable, match="no nix-store on PATH"):
        lx.nix_store_grant(str(pkg), [], {})


def test_the_launcher_runs_the_worker_without_ld_library_path(monkeypatch):
    """Maintainer 2026-11-26: the worker's environment is the same on every host. nix-ld, which
    loads the launcher's Python on NixOS, leaves LD_LIBRARY_PATH set (empty when it was unset):
    the preflight probe saw it on the NixOS host only (run 45720472876)."""
    ran: list = []
    monkeypatch.setattr(lx, "confine", lambda read, proxy_port, connect_ports=(): 4)
    monkeypatch.setattr(lx.os, "execve", lambda path, argv, env: ran.append((path, argv, env)))
    monkeypatch.setenv("LD_LIBRARY_PATH", "")
    monkeypatch.setenv("PROVISA_CODE_HASH", "abc")
    lx.main(["--proxy-port", "1", "--", "/bin/true", "x"])
    [(path, argv, env)] = ran
    assert (path, argv) == ("/bin/true", ["/bin/true", "x"])
    assert "LD_LIBRARY_PATH" not in env
    assert env["PROVISA_CODE_HASH"] == "abc"


def test_the_launcher_refuses_to_run_what_it_could_not_confine(monkeypatch, capsys):
    def _no_landlock(read, proxy_port, connect_ports=()):
        raise SandboxUnavailable("the kernel has no Landlock")

    ran: list = []
    monkeypatch.setattr(lx, "confine", _no_landlock)
    monkeypatch.setattr(lx.os, "execve", lambda *a: ran.append(a))
    assert lx.main(["--proxy-port", "1", "--", "/bin/true"]) == 3
    assert ran == []
    assert "code-source sandbox unavailable: the kernel has no Landlock" in capsys.readouterr().err


def test_the_macos_profile_denies_all_but_what_the_worker_needs(tmp_path):
    runtime = tmp_path / "rt" / "bin" / "node"
    code = tmp_path / "code"
    sock = tmp_path / "s" / "worker.sock"
    text = macos_sandbox.profile(
        executable=runtime, read=[code], socket_path=sock, proxy_port=40123
    )
    assert text.startswith("(version 1)\n(deny default)\n")
    assert f'(allow process-exec (literal "{runtime.resolve()}"))' in text
    assert f'(subpath "{code.resolve()}")' in text
    assert f'(path-literal "{sock.resolve()}")' in text
    assert '(remote ip "localhost:40123")' in text
    assert "file-write*" not in text  # writes: /dev/null only, by file-write-data
    assert str(Path.home()) not in text.replace(str(tmp_path), "")


def _host(address: str) -> list[str]:
    return ["/rt/bin/node", "/provisa/node/host.mjs", address, "/b/index.js"]


def test_on_linux_the_host_inherits_its_socket_and_starts_through_the_launcher(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(sandbox.platform, "system", lambda: "Linux")
    asked: list = []
    monkeypatch.setattr(
        sandbox,
        "nix_store_grant",
        lambda executable, read, env: asked.append((executable, read)) or ["/nix/store/a-rt"],
    )
    command = sandbox.sandboxed(
        _host,
        executable=Path("/rt/bin/node"),
        read=[tmp_path],
        env={"PATH": "/usr/bin"},
        cwd=tmp_path,
        egress=["api.example.test"],
        table_reads="pgwire",
    )
    assert command.inherit_socket
    argv = command.argv_for("fd:5")
    assert argv[:3] == [sys.executable, "-I", lx.__file__]
    assert argv[argv.index("--") + 1 :] == _host("fd:5")
    assert command.env["HTTPS_PROXY"].startswith("http://127.0.0.1:")
    # The runtime's Nix store closure is read beside what it is given (an empty list off Nix).
    assert asked == [("/rt/bin/node", [str(tmp_path), "/rt/bin"])]
    reads = [argv[i + 1] for i, a in enumerate(argv[: argv.index("--")]) if a == "--read"]
    assert reads == [str(tmp_path), "/rt/bin", "/nix/store/a-rt"]


def test_on_macos_the_host_starts_under_sandbox_exec(monkeypatch, tmp_path):
    monkeypatch.setattr(sandbox.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(macos_sandbox, "SANDBOX_EXEC", Path(sys.executable))  # present
    command = sandbox.sandboxed(
        _host,
        executable=Path("/rt/bin/node"),
        read=[tmp_path],
        env={},
        cwd=tmp_path,
        egress=[],
        table_reads="pgwire",
    )
    assert not command.inherit_socket
    argv = command.argv_for(str(tmp_path / "s" / "worker.sock"))
    assert argv[0] == sys.executable and argv[1] == "-p"
    assert argv[3:] == _host(str(tmp_path / "s" / "worker.sock"))


def test_a_platform_with_no_sandbox_refuses_a_code_source_by_name(monkeypatch, tmp_path):
    monkeypatch.setattr(sandbox.platform, "system", lambda: "Windows")
    with pytest.raises(SandboxUnavailable, match="a code source cannot run on Windows"):
        sandbox.sandboxed(
            _host,
            executable=Path("/rt/node"),
            read=[],
            env={},
            cwd=tmp_path,
            egress=[],
            table_reads="pgwire",
        )


def test_one_proxy_serves_every_worker_with_the_same_allow_list():
    first = sandbox.egress_proxy(["a.example.test", "B.example.test"])
    assert sandbox.egress_proxy(["b.example.test", "a.example.test"]) is first
    assert sandbox.egress_proxy(["c.example.test"]) is not first


def _framework(tmp_path: Path, *, app: bool) -> tuple[Path, Path]:
    """A python.org (framework) Python laid out under ``tmp_path``: its ``bin/python3.12`` stub,
    and (with ``app``) the ``Python.app`` program the stub re-executes; a venv's ``python3``
    links to the stub."""
    version = tmp_path / "Library/Frameworks/Python.framework/Versions/3.12"
    stub = version / "bin" / "python3.12"
    stub.parent.mkdir(parents=True)
    stub.write_text("")
    real = version / "Resources/Python.app/Contents/MacOS/Python"
    if app:
        real.parent.mkdir(parents=True)
        real.write_text("")
    venv = tmp_path / "venv" / "bin" / "python3"
    venv.parent.mkdir(parents=True)
    venv.symlink_to(stub)
    return venv, real


def _runs(text: str) -> list[str]:
    (line,) = [each for each in text.splitlines() if each.startswith("(allow process-exec")]
    return [part.split('"')[1] for part in line.split("(literal ")[1:]]


def test_a_framework_python_may_run_only_itself_and_the_program_it_re_executes(tmp_path):
    venv, real = _framework(tmp_path, app=True)
    text = macos_sandbox.profile(executable=venv, read=[], socket_path=None, proxy_port=1)
    assert _runs(text) == [str(venv.resolve()), str(real.resolve())]


def test_a_framework_with_no_python_app_adds_nothing_to_run(tmp_path):
    venv, _ = _framework(tmp_path, app=False)
    text = macos_sandbox.profile(executable=venv, read=[], socket_path=None, proxy_port=1)
    assert _runs(text) == [str(venv.resolve())]


def test_any_other_runtime_may_run_only_itself(tmp_path):
    node = tmp_path / "rt" / "bin" / "node"
    node.parent.mkdir(parents=True)
    node.write_text("")
    # A Python.app beside it is not its framework's: only a <Name>.framework/Versions layout is.
    app = tmp_path / "rt" / "Resources/Python.app/Contents/MacOS/Python"
    app.parent.mkdir(parents=True)
    app.write_text("")
    text = macos_sandbox.profile(executable=node, read=[], socket_path=None, proxy_port=1)
    assert _runs(text) == [str(node.resolve())]
