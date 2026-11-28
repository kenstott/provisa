# Copyright (c) 2026 Kenneth Stott
# Canary: d3f48cf6-b58d-4c57-8143-d7c32d0f4cae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Linux sandbox a code source's worker runs in (REQ-1984, REQ-1993).

A launcher, run as ``python -m provisa.codesource.linux_sandbox``, restricts itself and then
``exec``s the worker, so the restriction holds for the worker and everything it starts:

- Landlock (Linux 6.7 or later, ABI 4): files are read only under the directories named (the
  code, the runtime, the host program, the system's libraries and certificates, the process's own
  ``/proc/self``), written nowhere but ``/dev/null``, and TCP connects only to the egress proxy's
  port and the minted-token listener's (REQ-2226), and binds nowhere. From ABI 6, abstract Unix sockets and signals outside the worker are
  refused too.
- A seccomp filter: a new socket is refused unless it is TCP over IPv4 or IPv6, so there is no
  UDP (no DNS around the proxy), no raw or packet socket, no netlink, and no Unix socket (Landlock
  does not mediate a Unix socket's connect, and a host's Docker or Postgres socket is a path
  away). The worker's own socket is already connected when it starts: it is inherited.

Both need no privilege, user namespace or capability, so they hold under Docker's default seccomp
profile and Kubernetes' RuntimeDefault. A kernel or a runtime that does not give them is refused
by name (:class:`SandboxUnavailable`): a worker never runs unsandboxed.

``--probe`` checks all of it on the host it runs on and prints what it found (CI's sandbox probe).
Standard library only: it runs before anything else of Provisa's in the child."""

# Requirements: REQ-1984, REQ-1993
from __future__ import annotations

import ctypes
import errno
import functools
import os
import platform
import shutil
import socket
import struct
import subprocess  # nosec B404 - nix-store, by its resolved path, with a fixed argv
import sys
from collections.abc import Mapping, Sequence

#: The Landlock ABI that restricts TCP: Linux 6.7.
NET_ABI = 4

_LANDLOCK_CREATE_RULESET_VERSION = 1
_RULE_PATH_BENEATH = 1
_RULE_NET_PORT = 2

_FS_EXECUTE = 1 << 0
_FS_WRITE_FILE = 1 << 1
_FS_READ_FILE = 1 << 2
_FS_READ_DIR = 1 << 3
_FS_REMOVE_DIR = 1 << 4
_FS_REMOVE_FILE = 1 << 5
_FS_MAKE_CHAR = 1 << 6
_FS_MAKE_DIR = 1 << 7
_FS_MAKE_REG = 1 << 8
_FS_MAKE_SOCK = 1 << 9
_FS_MAKE_FIFO = 1 << 10
_FS_MAKE_BLOCK = 1 << 11
_FS_MAKE_SYM = 1 << 12
_FS_REFER = 1 << 13  # ABI 2
_FS_TRUNCATE = 1 << 14  # ABI 3
_FS_IOCTL_DEV = 1 << 15  # ABI 5

_NET_BIND_TCP = 1 << 0
_NET_CONNECT_TCP = 1 << 1

_SCOPE_ABSTRACT_UNIX_SOCKET = 1 << 0  # ABI 6
_SCOPE_SIGNAL = 1 << 1  # ABI 6

_READ = _FS_EXECUTE | _FS_READ_FILE | _FS_READ_DIR

_PR_SET_NO_NEW_PRIVS = 38
_SECCOMP_SET_MODE_FILTER = 1

# The syscall numbers: the generic table (aarch64) and x86_64 share Landlock's.
_SYSCALLS = {
    "x86_64": {
        "landlock_create_ruleset": 444,
        "landlock_add_rule": 445,
        "landlock_restrict_self": 446,
        "seccomp": 317,
        "socket": 41,
        "socketpair": 53,
        "io_uring_setup": 425,
        "io_uring_enter": 426,
        "io_uring_register": 427,
    },
    "aarch64": {
        "landlock_create_ruleset": 444,
        "landlock_add_rule": 445,
        "landlock_restrict_self": 446,
        "seccomp": 277,
        "socket": 198,
        "socketpair": 199,
        "io_uring_setup": 425,
        "io_uring_enter": 426,
        "io_uring_register": 427,
    },
}
_AUDIT_ARCH = {"x86_64": 0xC000003E, "aarch64": 0xC00000B7}

#: Read-only system directories every runtime needs: its libraries, the loader, certificates.
SYSTEM_READ = (
    "/usr",
    "/lib",
    "/lib64",
    "/etc/ssl",
    "/etc/ca-certificates",
    "/etc/pki",
    "/sys/devices/system/cpu",
    "/sys/fs/cgroup",
)


class SandboxUnavailable(RuntimeError):
    """This host cannot sandbox a worker: what is missing, by name. Every platform's sandbox
    raises it (macos_sandbox imports it from here, the one module the launcher may import)."""


def _machine() -> str:
    machine = platform.machine()
    machine = {"amd64": "x86_64", "arm64": "aarch64"}.get(machine.lower(), machine)
    if machine not in _SYSCALLS:
        raise SandboxUnavailable(f"no code-source sandbox for {machine}")
    return machine


_libc = ctypes.CDLL(None, use_errno=True)
_libc.syscall.restype = ctypes.c_long


#: What a raw syscall takes: the C integer and pointer types its arguments are passed as.
_CArg = (
    ctypes.c_int
    | ctypes.c_long
    | ctypes.c_size_t
    | ctypes.c_uint
    | ctypes.c_uint32
    | ctypes.c_void_p
)


def _syscall(name: str, *args: _CArg) -> int:
    nr = _SYSCALLS[_machine()][name]
    result = _libc.syscall(ctypes.c_long(nr), *args)
    if result < 0:
        err = ctypes.get_errno()
        raise OSError(err, f"{name}: {os.strerror(err)}")
    return result


def landlock_abi() -> int:
    """The kernel's Landlock ABI; :class:`SandboxUnavailable` when it has none or a seccomp
    profile refuses it."""
    if platform.system() != "Linux":
        raise SandboxUnavailable("Landlock is a Linux sandbox")
    try:
        return _syscall(
            "landlock_create_ruleset",
            ctypes.c_void_p(None),  # NULL attr: the call asks for the ABI version
            ctypes.c_size_t(0),
            ctypes.c_uint32(_LANDLOCK_CREATE_RULESET_VERSION),
        )
    except OSError as exc:
        if exc.errno == errno.ENOSYS:
            raise SandboxUnavailable(
                "the kernel has no Landlock, or the container's seccomp profile refuses it"
            ) from exc
        if exc.errno == errno.EOPNOTSUPP:
            raise SandboxUnavailable(
                "Landlock is built into the kernel but not enabled (lsm=)"
            ) from exc
        if exc.errno == errno.EPERM:
            raise SandboxUnavailable("the container's seccomp profile refuses Landlock") from exc
        raise


def _no_new_privs() -> None:
    if _libc.prctl(_PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0:
        err = ctypes.get_errno()
        raise SandboxUnavailable(f"prctl(PR_SET_NO_NEW_PRIVS) failed: {os.strerror(err)}")


def apply_landlock(read: Sequence[str], connect_ports: Sequence[int]) -> int:
    """Restrict this process (and what it ``exec``s) to reading under ``read``, writing only
    ``/dev/null``, and connecting over TCP only to ``connect_ports``. Returns the ABI used."""
    abi = landlock_abi()
    if abi < NET_ABI:
        raise SandboxUnavailable(
            f"the kernel's Landlock is ABI {abi}; restricting TCP needs ABI {NET_ABI} (Linux 6.7)"
        )
    handled_fs = (
        _FS_EXECUTE
        | _FS_WRITE_FILE
        | _FS_READ_FILE
        | _FS_READ_DIR
        | _FS_REMOVE_DIR
        | _FS_REMOVE_FILE
        | _FS_MAKE_CHAR
        | _FS_MAKE_DIR
        | _FS_MAKE_REG
        | _FS_MAKE_SOCK
        | _FS_MAKE_FIFO
        | _FS_MAKE_BLOCK
        | _FS_MAKE_SYM
        | _FS_REFER
        | _FS_TRUNCATE
    )
    if abi >= 5:
        handled_fs |= _FS_IOCTL_DEV
    handled_net = _NET_BIND_TCP | _NET_CONNECT_TCP
    if abi >= 6:
        attr = struct.pack(
            "=QQQ", handled_fs, handled_net, _SCOPE_ABSTRACT_UNIX_SOCKET | _SCOPE_SIGNAL
        )
    else:
        attr = struct.pack("=QQ", handled_fs, handled_net)
    buf = ctypes.create_string_buffer(attr, len(attr))
    ruleset = _syscall(
        "landlock_create_ruleset",
        ctypes.cast(buf, ctypes.c_void_p),
        ctypes.c_size_t(len(attr)),
        ctypes.c_uint32(0),
    )
    try:
        for path, access in [
            *((p, _READ) for p in read),
            ("/dev/null", _FS_READ_FILE | _FS_WRITE_FILE),
            ("/dev/urandom", _FS_READ_FILE),
            ("/dev/random", _FS_READ_FILE),
            (f"/proc/{os.getpid()}", _FS_READ_FILE | _FS_READ_DIR),
        ]:
            try:
                fd = os.open(path, os.O_PATH | os.O_CLOEXEC)  # pyright: ignore[reportAttributeAccessIssue] -- Linux only
            except FileNotFoundError:
                continue  # a system directory this host does not have
            try:
                if not os.path.isdir(path):
                    access &= _FS_EXECUTE | _FS_WRITE_FILE | _FS_READ_FILE | _FS_TRUNCATE
                rule = ctypes.create_string_buffer(struct.pack("=Qi", access, fd), 12)
                _syscall(
                    "landlock_add_rule",
                    ctypes.c_int(ruleset),
                    ctypes.c_int(_RULE_PATH_BENEATH),
                    ctypes.cast(rule, ctypes.c_void_p),
                    ctypes.c_uint32(0),
                )
            finally:
                os.close(fd)
        for port in connect_ports:
            rule = ctypes.create_string_buffer(struct.pack("=QQ", _NET_CONNECT_TCP, port), 16)
            _syscall(
                "landlock_add_rule",
                ctypes.c_int(ruleset),
                ctypes.c_int(_RULE_NET_PORT),
                ctypes.cast(rule, ctypes.c_void_p),
                ctypes.c_uint32(0),
            )
        _no_new_privs()
        _syscall("landlock_restrict_self", ctypes.c_int(ruleset), ctypes.c_uint32(0))
    finally:
        os.close(ruleset)
    return abi


# -- the Nix store ----------------------------------------------------------------------------------

NIX_STORE = "/nix/store"
#: nix-ld's library directory on NixOS (its ld.so is in it too): what nix-ld gives the loader when
#: NIX_LD_LIBRARY_PATH is unset, as a worker's environment leaves it. nix-ld's own default, mirrored
#: so the grant names the libraries nix-ld will load; absent on a host without nix-ld.
NIX_LD_DEFAULT_LIBRARY_PATH = "/run/current-system/sw/share/nix-ld/lib"

_ELF_MAGIC = b"\x7fELF"
_PT_INTERP = 3


def _store_path(path: str) -> str | None:
    """The store path (``/nix/store/<hash>-<name>``) ``path`` resolves into, or None when it
    resolves outside the store. Landlock judges the file a symlink resolves to."""
    real = os.path.realpath(path)
    if not real.startswith(NIX_STORE + "/"):
        return None
    return os.path.join(NIX_STORE, real[len(NIX_STORE) + 1 :].split("/")[0])


def elf_interpreter(path: str) -> str | None:
    """The loader (PT_INTERP) the ELF executable ``path`` names; None for a static binary or a
    file that is not ELF."""
    with open(path, "rb") as f:
        header = f.read(64)
        if len(header) < 64 or header[:4] != _ELF_MAGIC:
            return None
        order = "<" if header[5] == 1 else ">"
        if header[4] == 2:  # ELFCLASS64
            (phoff,) = struct.unpack_from(order + "Q", header, 32)
            phentsize, phnum = struct.unpack_from(order + "HH", header, 54)
            entry_format, offset_at, size_at = order + "IIQQQQQ", 2, 5
        else:
            (phoff,) = struct.unpack_from(order + "I", header, 28)
            phentsize, phnum = struct.unpack_from(order + "HH", header, 42)
            entry_format, offset_at, size_at = order + "IIIIIII", 1, 4
        f.seek(phoff)
        table = f.read(phentsize * phnum)
        for i in range(phnum):
            fields = struct.unpack_from(entry_format, table, i * phentsize)
            if fields[0] == _PT_INTERP:
                f.seek(fields[offset_at])
                return f.read(fields[size_at]).rstrip(b"\0").decode()
    return None


def _nix_ld_libraries(environ: Mapping[str, str]) -> list[str]:
    """Each library (and the loader) nix-ld gives a program started with ``environ``."""
    value = environ.get("NIX_LD_LIBRARY_PATH")
    directories = value.split(":") if value else [NIX_LD_DEFAULT_LIBRARY_PATH]
    found = [environ["NIX_LD"]] if environ.get("NIX_LD") else []
    for directory in directories:
        if os.path.isdir(directory):
            found += [os.path.join(directory, name) for name in os.listdir(directory)]
    return found


def _system_certificates() -> list[str]:
    """The system's certificate files under SYSTEM_READ's /etc directories: on NixOS each is a
    symlink into the store, which Landlock judges by its target."""
    found: list[str] = []
    for top in SYSTEM_READ:
        if top.startswith("/etc/") and os.path.isdir(top):
            for root, dirs, files in os.walk(top):
                found += [os.path.join(root, name) for name in (*dirs, *files)]
    return found


@functools.lru_cache(maxsize=64)
def _closure(paths: tuple[str, ...]) -> tuple[str, ...]:
    """``nix-store -qR``: every store path ``paths`` reference, themselves included."""
    nix_store = shutil.which("nix-store")
    if nix_store is None:
        raise SandboxUnavailable(
            "this host has a Nix store but no nix-store on PATH to read a runtime's closure with"
        )
    done = subprocess.run(  # nosec B603
        [nix_store, "--query", "--requisites", *paths], capture_output=True, text=True
    )
    if done.returncode != 0:
        raise SandboxUnavailable(f"nix-store could not read the closure: {done.stderr.strip()}")
    return tuple(line for line in done.stdout.splitlines() if line)


def nix_store_grant(executable: str, read: Sequence[str], environ: Mapping[str, str]) -> list[str]:
    """The Nix store paths a worker reads (REQ-1984): the closure of its runtime ``executable``,
    of what it is given to read (the host program among it), of the runtime's loader, and of the
    library set nix-ld loads it with -- never the whole store, where every other program's files
    are (on NixOS /etc/hosts and the rest of /etc resolve into it). Empty on a host with no
    store, or when nothing the worker runs resolves into it."""
    if not os.path.isdir(NIX_STORE):
        return []
    seeds = [executable, *read, *_nix_ld_libraries(environ), *_system_certificates()]
    loader = elf_interpreter(executable)
    if loader is not None:
        seeds.append(loader)
    store = sorted({p for p in map(_store_path, seeds) if p is not None})
    return list(_closure(tuple(store))) if store else []


# -- seccomp ----------------------------------------------------------------------------------------

_BPF_LD_W_ABS = 0x20
_BPF_JEQ_K = 0x15
_BPF_JGE_K = 0x35
_BPF_ALU_AND_K = 0x54
_BPF_RET_K = 0x06
_RET_ALLOW = 0x7FFF0000
_RET_ERRNO = 0x00050000
_SOCK_TYPE_MASK = 0xF  # SOCK_NONBLOCK and SOCK_CLOEXEC sit above it
_X32_SYSCALL_BIT = 0x40000000
# Linux's values, whatever platform this module is read on.
_AF_INET = 2
_AF_INET6 = 10
_SOCK_STREAM = 1


def _insn(code: int, jt: int, jf: int, k: int) -> bytes:
    return struct.pack("=HBBI", code, jt, jf, k)


def socket_filter(machine: str | None = None) -> bytes:
    """The BPF program, for ``machine`` (this host's by default): ``socket()`` is allowed only for
    TCP over IPv4 or IPv6; ``socketpair()`` allowed (an unnamed connected pair reaches nothing,
    and asyncio's event loop wakes itself through one); io_uring never (it opens sockets without the
    ``socket()`` call); on x86_64, no x32 call at all (the same calls under other numbers); a call
    from another architecture never. Every other system call is allowed. A refusal is EPERM.

    The constants are Linux's own (``AF_INET6`` is 10 there), whatever this module runs on."""
    machine = machine or _machine()
    calls = _SYSCALLS[machine]
    refuse = _insn(_BPF_RET_K, 0, 0, _RET_ERRNO | errno.EPERM)
    allow = _insn(_BPF_RET_K, 0, 0, _RET_ALLOW)
    program = [
        _insn(_BPF_LD_W_ABS, 0, 0, 4),  # arch
        _insn(_BPF_JEQ_K, 1, 0, _AUDIT_ARCH[machine]),
        refuse,  # another architecture's calls
        _insn(_BPF_LD_W_ABS, 0, 0, 0),  # nr
    ]
    if machine == "x86_64":
        program += [_insn(_BPF_JGE_K, 0, 1, _X32_SYSCALL_BIT), refuse]
    for name in ("io_uring_setup", "io_uring_enter", "io_uring_register"):
        program += [_insn(_BPF_JEQ_K, 0, 1, calls[name]), refuse]
    program += [
        _insn(_BPF_JEQ_K, 1, 0, calls["socket"]),
        allow,  # not socket()
        _insn(_BPF_LD_W_ABS, 0, 0, 16),  # args[0]: the family
        _insn(_BPF_JEQ_K, 1, 0, _AF_INET),
        _insn(_BPF_JEQ_K, 0, 3, _AF_INET6),
        _insn(_BPF_LD_W_ABS, 0, 0, 24),  # args[1]: the type
        _insn(_BPF_ALU_AND_K, 0, 0, _SOCK_TYPE_MASK),
        _insn(_BPF_JEQ_K, 1, 0, _SOCK_STREAM),
        refuse,
        allow,
    ]
    return b"".join(program)


def apply_socket_filter() -> None:
    code = socket_filter()
    filters = ctypes.create_string_buffer(code, len(code))
    prog = ctypes.create_string_buffer(
        struct.pack("=HxxxxxxQ", len(code) // 8, ctypes.addressof(filters)), 16
    )
    _no_new_privs()
    try:
        _syscall(
            "seccomp",
            ctypes.c_uint(_SECCOMP_SET_MODE_FILTER),
            ctypes.c_uint(0),
            ctypes.cast(prog, ctypes.c_void_p),
        )
    except OSError as exc:
        raise SandboxUnavailable(f"the seccomp filter could not be installed: {exc}") from exc


def confine(read: Sequence[str], proxy_port: int, connect_ports: Sequence[int] = ()) -> int:
    """Both restrictions on this process; the Landlock ABI used."""
    abi = apply_landlock([*read, *SYSTEM_READ], [proxy_port, *connect_ports])
    apply_socket_filter()
    return abi


# -- the launcher -----------------------------------------------------------------------------------


def launcher_argv(
    python: str,
    *,
    read: Sequence[str],
    proxy_port: int,
    argv: Sequence[str],
    connect_ports: Sequence[int] = (),
) -> list[str]:
    """How a worker is started inside the sandbox: Provisa's Python runs this module, which
    confines itself and ``exec``s ``argv``. Run by its file's path, not ``-m``: isolated (``-I``),
    the working directory is not on the path, and an image that runs Provisa from its source
    directory has no package to find (it imports only the standard library)."""
    out = [python, "-I", os.path.abspath(__file__), "--proxy-port", str(proxy_port)]
    for port in connect_ports:
        out += ["--connect-port", str(port)]
    for path in read:
        out += ["--read", path]
    return [*out, "--", *argv]


def _probe() -> int:
    """Confine this process as a worker would be, then show each restriction holds."""
    print(f"kernel {platform.release()} {platform.machine()}")
    try:
        abi = landlock_abi()
    except SandboxUnavailable as exc:
        print(f"UNAVAILABLE: {exc}")
        return 1
    print(f"landlock ABI {abi} (TCP rules need {NET_ABI})")
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    port = listener.getsockname()[1]
    other = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    other.bind(("127.0.0.1", 0))
    other.listen(4)
    other_port = other.getsockname()[1]
    here = os.path.dirname(os.path.abspath(__file__))
    try:
        confine([here, sys.prefix, sys.base_prefix], port)
    except SandboxUnavailable as exc:
        print(f"UNAVAILABLE: {exc}")
        return 1
    failures = 0

    def expect(what: str, refused: bool, action) -> None:
        nonlocal failures
        try:
            action()
            got = False
        except OSError:
            got = True
        ok = got == refused
        failures += not ok
        print(f"{'ok  ' if ok else 'FAIL'} {what}: {'refused' if got else 'allowed'}")

    expect("read /etc/passwd", True, lambda: open("/etc/passwd").read())
    expect("read the sandbox's own code", False, lambda: open(__file__).read())
    # The write is the one the sandbox must refuse: it never lands.
    expect("write /tmp", True, lambda: open("/tmp/provisa-probe", "w").write("x"))  # nosec B108
    expect(
        "TCP to the proxy's port",
        False,
        lambda: socket.create_connection(("127.0.0.1", port), timeout=5).close(),
    )
    expect(
        "TCP to another port",
        True,
        lambda: socket.create_connection(("127.0.0.1", other_port), timeout=5).close(),
    )
    expect(
        "bind a TCP port",
        True,
        lambda: socket.socket(socket.AF_INET, socket.SOCK_STREAM).bind(("127.0.0.1", 0)),
    )
    expect("UDP socket", True, lambda: socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
    expect("Unix socket", True, lambda: socket.socket(socket.AF_UNIX, socket.SOCK_STREAM))
    expect("socketpair", False, lambda: [s.close() for s in socket.socketpair()])
    print("PASS" if not failures else f"FAIL ({failures})")
    return 1 if failures else 0


def main(args: Sequence[str]) -> int:
    if list(args) == ["--probe"]:
        return _probe()
    read: list[str] = []
    connect_ports: list[int] = []
    proxy_port = None
    rest = list(args)
    while rest and rest[0] != "--":
        flag, value, rest = rest[0], rest[1], rest[2:]
        if flag == "--read":
            read.append(value)
        elif flag == "--proxy-port":
            proxy_port = int(value)
        elif flag == "--connect-port":
            connect_ports.append(int(value))
        else:
            raise SystemExit(f"unknown option {flag}")
    if proxy_port is None or not rest:
        raise SystemExit("usage: --proxy-port N [--read DIR]... -- ARGV")
    try:
        confine(read, proxy_port, connect_ports)
    except SandboxUnavailable as exc:
        sys.stderr.write(f"code-source sandbox unavailable: {exc}\n")
        return 3
    argv = rest[1:]
    # The worker's environment is the one it was given, on every host: nix-ld, which loads this
    # launcher's Python on NixOS, leaves LD_LIBRARY_PATH behind (set, empty when it was unset).
    env = {k: v for k, v in os.environ.items() if k != "LD_LIBRARY_PATH"}
    os.execve(argv[0], argv, env)
    return 127  # execve does not return


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
