# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2a6d0e-41b7-4c93-b5e8-7a1c3d9f0b26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A pgwire proxy that records every statement a client sends (REQ-2204).

A tool connects to the proxy as it would to PostgreSQL; the proxy passes every byte through, both
ways, and writes one JSON line per statement the client runs: a simple Query, or an extended-protocol
Bind of a parsed statement, with the parameter type OIDs from its Parse and the values and formats
from its Bind. Each line names the connection's user and application_name from its startup packet,
so one recording holds several tools, told apart by the user each signs in as. The server's answers
are not recorded: the replay asks PostgreSQL and Provisa again (replay.py).

SSL and GSS encryption requests are refused with 'N', so the conversation stays readable; every tool
here falls back to a plain connection.

Usage:
  proxy.py --listen 0.0.0.0:6543 --upstream 127.0.0.1:5432 --out statements.jsonl
"""

# Requirements: REQ-2204

from __future__ import annotations

import argparse
import asyncio
import json
import struct
import sys
from pathlib import Path
from typing import IO, Any

SSL_REQUEST = 80877103
GSS_REQUEST = 80877104
CANCEL_REQUEST = 80877102

#: Binary parameter values of these types decode to their text form, so a recorded OID can be
#: recognised and replaced (capture.py) whatever format the client sent it in.
_BINARY_INTS = {21: ">h", 23: ">i", 20: ">q", 26: ">I"}
_BOOL = 16


def _cstr(body: bytes, at: int) -> tuple[str, int]:
    end = body.index(b"\0", at)
    return body[at:end].decode(errors="replace"), end + 1


def startup_params(body: bytes) -> dict[str, str]:
    """A StartupMessage's parameters (``body`` after its length and protocol code)."""
    params: dict[str, str] = {}
    at = 0
    while at < len(body) and body[at] != 0:
        key, at = _cstr(body, at)
        value, at = _cstr(body, at)
        params[key] = value
    return params


def parse_message(body: bytes) -> tuple[str, str, list[int]]:
    """A Parse message: (statement name, query, parameter type OIDs)."""
    name, at = _cstr(body, 0)
    query, at = _cstr(body, at)
    (count,) = struct.unpack_from(">h", body, at)
    at += 2
    oids = list(struct.unpack_from(f">{count}I", body, at)) if count else []
    return name, query, oids


def bind_message(body: bytes) -> tuple[str, list[int], list[bytes | None]]:
    """A Bind message: (statement name, parameter format codes, parameter values)."""
    _portal, at = _cstr(body, 0)
    statement, at = _cstr(body, at)
    (nfmt,) = struct.unpack_from(">h", body, at)
    at += 2
    formats = list(struct.unpack_from(f">{nfmt}h", body, at)) if nfmt else []
    at += 2 * nfmt
    (nparams,) = struct.unpack_from(">h", body, at)
    at += 2
    values: list[bytes | None] = []
    for _ in range(nparams):
        (length,) = struct.unpack_from(">i", body, at)
        at += 4
        if length < 0:
            values.append(None)
        else:
            values.append(body[at : at + length])
            at += length
    return statement, formats, values


def param_formats(formats: list[int], count: int) -> list[int]:
    """Each parameter's format code: none given is text for all, one given is for all."""
    if not formats:
        return [0] * count
    if len(formats) == 1:
        return formats * count
    return formats


def param_entry(type_oid: int, fmt: int, value: bytes | None) -> dict[str, Any]:
    """One recorded parameter: its type OID, and its value as text where it has a text form
    (a text parameter; a binary integer, OID or boolean, decoded), else as hex with format 1."""
    if value is None:
        return {"type": type_oid, "value": None}
    if fmt == 0:
        return {"type": type_oid, "value": value.decode(errors="replace")}
    if type_oid in _BINARY_INTS and len(value) == struct.calcsize(_BINARY_INTS[type_oid]):
        return {"type": type_oid, "value": str(struct.unpack(_BINARY_INTS[type_oid], value)[0])}
    if type_oid == _BOOL and len(value) == 1:
        return {"type": type_oid, "value": "t" if value[0] else "f"}
    return {"type": type_oid, "value": value.hex(), "format": 1}


class Session:
    """One client connection's statements: what it parsed, by name, and what it ran."""

    def __init__(self, out: IO[str]) -> None:
        self.out = out
        self.params: dict[str, str] = {}
        self.prepared: dict[str, tuple[str, list[int]]] = {}

    def _write(self, entry: dict[str, Any]) -> None:
        entry = {
            "user": self.params.get("user", ""),
            "application_name": self.params.get("application_name", ""),
            **entry,
        }
        self.out.write(json.dumps(entry) + "\n")
        self.out.flush()

    def message(self, kind: bytes, body: bytes) -> None:
        if kind == b"Q":
            self._write({"protocol": "simple", "sql": body.rstrip(b"\0").decode(errors="replace")})
        elif kind == b"P":
            name, query, oids = parse_message(body)
            self.prepared[name] = (query, oids)
        elif kind == b"B":
            statement, formats, values = bind_message(body)
            query, oids = self.prepared.get(statement, ("", []))
            fmts = param_formats(formats, len(values))
            types = oids + [0] * (len(values) - len(oids))
            self._write(
                {
                    "protocol": "extended",
                    "sql": query,
                    "params": [param_entry(t, f, v) for t, f, v in zip(types, fmts, values)],
                }
            )


async def _read_exact(reader: asyncio.StreamReader, n: int) -> bytes:
    return await reader.readexactly(n)


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        writer.close()


async def _client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    upstream: tuple[str, int],
    out: IO[str],
) -> None:
    session = Session(out)
    up_reader, up_writer = await asyncio.open_connection(*upstream)
    try:
        while True:
            (length,) = struct.unpack(">i", await _read_exact(reader, 4))
            body = await _read_exact(reader, length - 4)
            (code,) = struct.unpack_from(">i", body, 0)
            if code in (SSL_REQUEST, GSS_REQUEST):
                writer.write(b"N")
                await writer.drain()
                continue
            up_writer.write(struct.pack(">i", length) + body)
            await up_writer.drain()
            if code == CANCEL_REQUEST:
                return
            session.params = startup_params(body[4:])
            break
        back = asyncio.create_task(_pipe(up_reader, writer))
        while True:
            kind = await _read_exact(reader, 1)
            (length,) = struct.unpack(">i", await _read_exact(reader, 4))
            body = await _read_exact(reader, length - 4)
            session.message(kind, body)
            up_writer.write(kind + struct.pack(">i", length) + body)
            await up_writer.drain()
            if kind == b"X":
                break
        await back
    except (ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        up_writer.close()
        writer.close()


async def serve(listen: tuple[str, int], upstream: tuple[str, int], out_path: Path) -> None:
    with out_path.open("a") as out:
        server = await asyncio.start_server(
            lambda r, w: _client(r, w, upstream, out), listen[0], listen[1]
        )
        print(f"recording {listen[0]}:{listen[1]} -> {upstream[0]}:{upstream[1]} into {out_path}")
        sys.stdout.flush()
        async with server:
            await server.serve_forever()


def _address(text: str) -> tuple[str, int]:
    host, port = text.rsplit(":", 1)
    return host, int(port)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--listen", required=True, type=_address)
    parser.add_argument("--upstream", required=True, type=_address)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)
    asyncio.run(serve(args.listen, args.upstream, args.out))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
