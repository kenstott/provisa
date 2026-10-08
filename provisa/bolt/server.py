# Copyright (c) 2026 Kenneth Stott
# Canary: 6dcaa0cb-f3b7-462f-a04a-4dd9296107de
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bolt TCP server — one thread per connection, each running its own asyncio loop (REQ-1882).

Handshake sequence:
  1. Client sends 4-byte magic (0x6060B017)
  2. Client sends four 4-byte version proposals
  3. Server replies with chosen version (4 bytes) or 0x00000000 to reject
  4. Message exchange via BoltSession
"""

from __future__ import annotations

import asyncio
import logging
import socket
import ssl
import threading

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
from provisa.core.connection_loop import connection_loop

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
    try:
        await _serve(session, reader, writer)
    finally:
        session.close()  # a request left unpulled ends with its connection (REQ-1905)


async def _serve(session: BoltSession, reader: BoltReader, writer: BoltWriter) -> None:
    """The message loop of one negotiated connection."""
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
        await session.handle_hello(fields)
    elif tag == msg.LOGON:
        await session.handle_logon(fields)
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


async def _serve_accepted(sock: socket.socket, ssl_ctx: ssl.SSLContext | None) -> None:
    """Wrap an accepted socket in asyncio streams on the running (connection) loop and serve it."""
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    transport, _ = await loop.connect_accepted_socket(lambda: protocol, sock=sock, ssl=ssl_ctx)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    await _handle_client(reader, writer)


def _serve_connection(sock: socket.socket, ssl_ctx: ssl.SSLContext | None) -> None:
    """Serve one Bolt connection entirely on this thread, on a loop this thread owns (REQ-1882).

    Every coroutine the session runs — handshake, auth, org resolution, governance, execution,
    audit — executes on this connection's loop via ``run_until_complete`` on this thread, so two
    Bolt connections never queue on one shared loop."""
    try:
        with connection_loop() as cl:
            cl.run(_serve_accepted(sock, ssl_ctx))
    except Exception:
        # _handle_client answers and closes every protocol-level failure itself; what reaches here
        # failed before a stream existed (TLS handshake, socket setup). Reported, and the socket
        # is not left open.
        log.exception("[BOLT] connection setup failed")
        sock.close()


# How long the accept thread waits for a connection before it looks again at whether the listener
# was closed: the longest close() waits for that thread.
_ACCEPT_POLL_SECONDS = 0.5


class BoltListener:
    """The Bolt TCP listener: an accept loop on its own thread, one thread per connection."""

    def __init__(self, host: str, port: int, ssl_ctx: ssl.SSLContext | None) -> None:
        # reuse_port=True (REQ-1900): SO_REUSEPORT lets multiple uvicorn `--workers N` processes
        # each run their own Bolt listener on the same port (the kernel load-balances new
        # connections between them) instead of all but one crashing with "Address already in use".
        self._sock = socket.create_server((host, port), reuse_port=True)
        # The accept thread is stopped by the closed flag, read between bounded waits. Closing
        # the socket does not wake a thread blocked in accept(): the thread stayed there, and the
        # descriptor it held was free for the next socket the process opened. Nor can a
        # connection made to the port wake it: the port is shared (SO_REUSEPORT) and the kernel
        # may give that connection to another worker's listener (provisa/api/flight/relay.py).
        self._sock.settimeout(_ACCEPT_POLL_SECONDS)
        self._ssl_ctx = ssl_ctx
        self._closed = False
        self.port = self._sock.getsockname()[1]
        self._thread = threading.Thread(
            target=self._accept_loop, name=f"bolt-accept:{self.port}", daemon=True
        )
        self._thread.start()

    def _accept_loop(self) -> None:
        while not self._closed:
            try:
                conn, _addr = self._sock.accept()
            except TimeoutError:
                continue  # nothing arrived: look at the closed flag again
            except OSError:
                # A transient accept failure (EMFILE, ECONNABORTED) costs one connection, not the
                # listener; it is reported and the loop keeps accepting.
                log.exception("[BOLT] accept failed")
                continue
            if self._closed:
                conn.close()  # arrived as the listener closed
                return
            # The listener's timeout is its own: a connection blocks, as its handler expects (an
            # accepted socket takes the listener's timeout on some platforms).
            conn.settimeout(None)
            threading.Thread(
                target=_serve_connection, args=(conn, self._ssl_ctx), name="bolt-conn", daemon=True
            ).start()

    def close(self) -> None:
        """Stop accepting. Returns once the accept thread has ended; the listening socket is
        closed only then, so no thread is left in accept() on a descriptor that was closed."""
        self._closed = True
        self._thread.join()
        self._sock.close()


def start_bolt_server(
    host: str,
    port: int,
    ssl_ctx: ssl.SSLContext | None,
) -> BoltListener:
    """Start the Bolt listener. Returns once it is bound; connections are served on their own
    threads, each on a loop that thread owns (REQ-1882)."""
    listener = BoltListener(host, port, ssl_ctx)
    log.info("[BOLT] listening on %s:%d (TLS=%s)", host, listener.port, ssl_ctx is not None)
    return listener
