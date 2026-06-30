# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3f1a2e-4c8b-4e9f-a5d2-1b6c3e8f2a4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bolt TCP server — asyncio-native handler.

Handshake sequence:
  1. Client sends 4-byte magic (0x6060B017)
  2. Client sends four 4-byte version proposals
  3. Server replies with chosen version (4 bytes) or 0x00000000 to reject
  4. Message exchange via BoltSession
"""

from __future__ import annotations

import asyncio
import logging
import ssl

import provisa.bolt.messages as msg
from provisa.bolt.framing import read_message, write_message
from provisa.bolt.messages import (
    MAGIC,
    SUPPORTED_VERSIONS,
    BoltMessage,
    encode_version,
)
from provisa.bolt.packstream import pack_message
from provisa.bolt.session import BoltSession
from provisa.bolt.websocket import BoltReader, BoltWriter

log = logging.getLogger(__name__)


def _decode_message(data: bytes) -> BoltMessage:
    """Decode a raw PackStream message bytes → BoltMessage."""
    if len(data) < 2:
        raise ValueError("Message too short")
    # Tiny struct header: 0xB0 | n_fields, then tag byte
    n_fields = data[0] & 0x0F
    tag = data[1]
    fields: list = []
    if n_fields > 0 and len(data) > 2:
        from provisa.bolt.packstream import unpack_fields

        fields = unpack_fields(data[2:])
    return BoltMessage(tag=tag, fields=fields)


async def _handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    peer = writer.get_extra_info("peername")
    _dbg = logging.getLogger("uvicorn.error")
    _dbg.warning("[BOLT] connection from %s", peer)
    try:
        from provisa.bolt.websocket import detect_and_upgrade

        bolt_reader, bolt_writer = await detect_and_upgrade(reader, writer)
        _dbg.warning("[BOLT] protocol detected for %s", peer)
        await _bolt_handshake_and_serve(bolt_reader, bolt_writer)
    except asyncio.IncompleteReadError:
        _dbg.warning("[BOLT] client %s disconnected (IncompleteRead)", peer)
    except Exception as exc:
        _dbg.warning("[BOLT] error for %s: %r", peer, exc)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        _dbg.warning("[BOLT] connection closed %s", peer)


async def _bolt_handshake_and_serve(
    reader: BoltReader,
    writer: BoltWriter,
) -> None:
    # 1. Read and verify magic
    magic = await reader.readexactly(4)
    if magic != MAGIC:
        log.warning("[BOLT] bad magic: %r", magic)
        return

    # 2. Read four version proposals; each is [minor, major, range, 0]
    #    range > 0 means client accepts (major, minor) down to (major, minor-range)
    _dbg = logging.getLogger("uvicorn.error")
    raw_proposals: list[bytes] = []
    for _ in range(4):
        raw_proposals.append(await reader.readexactly(4))
    _dbg.warning("[BOLT] raw proposals: %s", [b.hex() for b in raw_proposals])

    def _candidates(b4: bytes) -> list[tuple[int, int]]:
        # Wire format: [0x00, range, minor, major]
        rng, minor, major = b4[1], b4[2], b4[3]
        return [(major, minor - i) for i in range(rng + 1) if minor - i >= 0]

    # 3. Negotiate version — first SUPPORTED_VERSIONS entry that any proposal covers wins
    chosen: tuple[int, int] | None = None
    all_candidates = [c for b in raw_proposals for c in _candidates(b)]
    _dbg.warning("[BOLT] candidates: %s", all_candidates)
    for supported in SUPPORTED_VERSIONS:
        if supported in all_candidates:
            chosen = supported
            break

    if chosen is None:
        writer.write(b"\x00\x00\x00\x00")
        await writer.drain()
        _dbg.warning("[BOLT] no supported version; candidates=%s", all_candidates)
        return

    writer.write(encode_version(*chosen))
    await writer.drain()
    log.info("[BOLT] negotiated Bolt %d.%d", *chosen)

    session = BoltSession(writer, chosen)

    # 4. Message loop
    while True:
        data = await read_message(reader)
        if not data:
            break

        try:
            message = _decode_message(data)
        except Exception as exc:
            log.warning("[BOLT] decode error: %s", exc)
            write_message(
                writer,
                pack_message(
                    msg.FAILURE,
                    {
                        "code": "Neo.ClientError.Request.Invalid",
                        "message": f"Decode error: {exc}",
                    },
                ),
            )
            await writer.drain()
            continue

        log.debug("[BOLT] recv tag=0x%02X fields=%r", message.tag, message.fields)

        await _dispatch(session, message)
        await writer.drain()

        if session.state.name == "DEFUNCT":
            break


async def _dispatch(session: BoltSession, message: BoltMessage) -> None:
    tag = message.tag
    fields = message.fields
    _dbg = logging.getLogger("uvicorn.error")
    _dbg.warning("[BOLT] dispatch tag=0x%02X fields=%r", tag, fields)

    if tag == msg.GOODBYE:
        return  # client is done; no response expected
    elif tag == msg.HELLO:
        session.handle_hello(fields)
    elif tag == msg.LOGON:
        session.handle_logon(fields)
    elif tag == msg.LOGOFF:
        session.handle_logoff()
    elif tag == msg.RESET:
        session.handle_reset()
    elif tag == msg.BEGIN:
        session.handle_begin(fields)
    elif tag == msg.COMMIT:
        session.handle_commit()
    elif tag == msg.ROLLBACK:
        session.handle_rollback()
    elif tag == msg.RUN:
        await session.handle_run(fields)
    elif tag == msg.PULL:
        session.handle_pull(fields)
    elif tag == msg.DISCARD:
        session.handle_discard(fields)
    elif tag == msg.ROUTE:
        session.handle_route()
    elif tag == msg.TELEMETRY:
        session.handle_telemetry()
    else:
        log.warning("[BOLT] unknown message tag 0x%02X", tag)
        session.send_failure(
            "Neo.ClientError.Request.Invalid",
            f"Unknown message tag: 0x{tag:02X}",
        )


async def _serve(host: str, port: int, ssl_ctx: ssl.SSLContext | None) -> None:
    server = await asyncio.start_server(
        _handle_client,
        host,
        port,
        ssl=ssl_ctx,
    )
    log.info("[BOLT] listening on %s:%d (TLS=%s)", host, port, ssl_ctx is not None)
    async with server:
        await server.serve_forever()


def start_bolt_server(
    host: str,
    port: int,
    ssl_ctx: ssl.SSLContext | None,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Schedule the Bolt server on the running event loop. Returns immediately."""
    asyncio.run_coroutine_threadsafe(_serve(host, port, ssl_ctx), loop)
    log.info("[BOLT] scheduled on %s:%d", host, port)
