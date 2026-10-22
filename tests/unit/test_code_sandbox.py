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

    argv = lx.launcher_argv(sys.executable, read=[], proxy_port=1, argv=["/bin/true"])
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


def test_the_launcher_refuses_to_run_what_it_could_not_confine(monkeypatch, capsys):
    def _no_landlock(read, proxy_port):
        raise SandboxUnavailable("the kernel has no Landlock")

    ran: list = []
    monkeypatch.setattr(lx, "confine", _no_landlock)
    monkeypatch.setattr(lx.os, "execv", lambda *a: ran.append(a))
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
    command = sandbox.sandboxed(
        _host,
        executable=Path("/rt/bin/node"),
        read=[tmp_path],
        env={"PATH": "/usr/bin"},
        cwd=tmp_path,
        egress=["api.example.test"],
    )
    assert command.inherit_socket
    argv = command.argv_for("fd:5")
    assert argv[:3] == [sys.executable, "-I", lx.__file__]
    assert argv[argv.index("--") + 1 :] == _host("fd:5")
    assert command.env["HTTPS_PROXY"].startswith("http://127.0.0.1:")


def test_on_macos_the_host_starts_under_sandbox_exec(monkeypatch, tmp_path):
    monkeypatch.setattr(sandbox.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(macos_sandbox, "SANDBOX_EXEC", Path(sys.executable))  # present
    command = sandbox.sandboxed(
        _host, executable=Path("/rt/bin/node"), read=[tmp_path], env={}, cwd=tmp_path, egress=[]
    )
    assert not command.inherit_socket
    argv = command.argv_for(str(tmp_path / "s" / "worker.sock"))
    assert argv[0] == sys.executable and argv[1] == "-p"
    assert argv[3:] == _host(str(tmp_path / "s" / "worker.sock"))


def test_a_platform_with_no_sandbox_refuses_a_code_source_by_name(monkeypatch, tmp_path):
    monkeypatch.setattr(sandbox.platform, "system", lambda: "Windows")
    with pytest.raises(SandboxUnavailable, match="a code source cannot run on Windows"):
        sandbox.sandboxed(
            _host, executable=Path("/rt/node"), read=[], env={}, cwd=tmp_path, egress=[]
        )


def test_one_proxy_serves_every_worker_with_the_same_allow_list():
    first = sandbox.egress_proxy(["a.example.test", "B.example.test"])
    assert sandbox.egress_proxy(["b.example.test", "a.example.test"]) is first
    assert sandbox.egress_proxy(["c.example.test"]) is not first
