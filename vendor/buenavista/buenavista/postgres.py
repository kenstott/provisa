# Copyright (c) 2026 Kenneth Stott
# Canary: 83b42410-aaf4-4c83-83af-0fae2f3a5f8a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

import datetime
import decimal
import hashlib
import uuid as _uuid_mod
import io
import logging
import os
import random
import socket
import socketserver
import struct
from typing import Any, Dict, List, Optional

from .core import BVType, Connection, Extension, Session, QueryResult, wire_type_name
from .rewrite import Rewriter

logger = logging.getLogger(__name__)


NULL_BYTE = b"\x00"


class ServerResponse:
    """Byte codes for server responses in the PG wire protocol."""

    AUTHENTICATION_REQUEST = b"R"
    BACKEND_KEY_DATA = b"K"
    BIND_COMPLETE = b"2"
    CLOSE_COMPLETE = b"3"
    COMMAND_COMPLETE = b"C"
    COPY_DATA = b"d"
    COPY_DONE = b"c"
    COPY_FAIL = b"f"
    COPY_IN_RESPONSE = b"G"
    COPY_OUT_RESPONSE = b"H"
    DATA_ROW = b"D"
    EMPTY_QUERY_RESPONSE = b"I"
    ERROR_RESPONSE = b"E"
    NO_DATA = b"n"
    NOTICE_RESPONSE = b"N"
    PARAMETER_DESCRIPTION = b"t"
    PARAMETER_STATUS = b"S"
    PARSE_COMPLETE = b"1"
    PORTAL_SUSPENDED = b"s"
    READY_FOR_QUERY = b"Z"
    ROW_DESCRIPTION = b"T"


def _decode_numeric(v: bytes) -> decimal.Decimal:
    """Decode PG binary numeric: ndigits/weight/sign/dscale + base-10000 digit array."""
    if len(v) < 8:
        return decimal.Decimal(0)
    ndigits, weight, sign, dscale = struct.unpack("!hhhh", v[:8])
    if sign == 0xC000:
        return decimal.Decimal("NaN")
    digits = [struct.unpack("!H", v[8 + i * 2 : 10 + i * 2])[0] for i in range(ndigits)]
    value = decimal.Decimal(0)
    for i, d in enumerate(digits):
        value += decimal.Decimal(d) * (decimal.Decimal(10000) ** (weight - i))
    if sign == 0x4000:
        value = -value
    return value.quantize(decimal.Decimal(10) ** -dscale) if dscale else value


_PG_EPOCH = datetime.datetime(2000, 1, 1)
_PG_EPOCH_UTC = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
_PG_DATE_EPOCH = datetime.date(2000, 1, 1)


def _text_bool(s: str) -> bool:
    v = s.strip().lower()
    if v in ("t", "true", "1", "y", "yes", "on"):
        return True
    if v in ("f", "false", "0", "n", "no", "off"):
        return False
    raise ValueError(f"invalid boolean parameter {s!r}")


# Text-format Bind values, decoded by the parameter's declared type OID (REQ-589): the value stays
# BOUND (never spliced into the SQL), so it must arrive as the typed value the client declared.
# An OID not listed (0 = unspecified, text types) keeps the string — the engine casts it by context,
# exactly as it cast the untyped string literal this value used to be spliced in as.
TEXT_PARAM_DECODERS = {
    16: _text_bool,
    20: int,
    21: int,
    23: int,
    700: float,
    701: float,
    1700: decimal.Decimal,
    1082: datetime.date.fromisoformat,
    1114: datetime.datetime.fromisoformat,
    1184: datetime.datetime.fromisoformat,
    2950: _uuid_mod.UUID,
}


TYPE_OIDS = {
    # see Postgres pg_type_d.h — values are (name, decoder_callable, example)
    16: ("BOOLOID", lambda v: v[0] != 0, False),
    17: ("BYTEAOID", bytes, b""),
    20: ("INT8OID", lambda v: struct.unpack("!q", v)[0], 42),
    21: ("INT2OID", lambda v: struct.unpack("!h", v)[0], 42),
    23: ("INT4OID", lambda v: struct.unpack("!i", v)[0], 42),
    25: ("TEXTOID", lambda v: v.decode("utf-8"), ""),
    700: ("FLOAT4OID", lambda v: struct.unpack("!f", v)[0], 42.0),
    701: ("FLOAT8OID", lambda v: struct.unpack("!d", v)[0], 42.0),
    1043: ("VARCHAROID", lambda v: v.decode("utf-8"), ""),
    1082: (
        "DATEOID",
        lambda v: _PG_DATE_EPOCH + datetime.timedelta(days=struct.unpack("!i", v)[0]),
        datetime.date.today(),
    ),
    1114: (
        "TIMESTAMPOID",
        lambda v: _PG_EPOCH + datetime.timedelta(microseconds=struct.unpack("!q", v)[0]),
        datetime.datetime.now(),
    ),
    1184: (
        "TIMESTAMPTZOID",
        lambda v: _PG_EPOCH_UTC + datetime.timedelta(microseconds=struct.unpack("!q", v)[0]),
        datetime.datetime.now(datetime.timezone.utc),
    ),
    1700: ("NUMERICOID", _decode_numeric, decimal.Decimal(0)),
    2950: ("UUIDOID", lambda v: str(_uuid_mod.UUID(bytes=bytes(v))), ""),
    26: ("OIDOID", lambda v: struct.unpack("!I", v)[0], 0),
    0: ("UNSPECIFIED", lambda v: v.decode("utf-8"), ""),
    1028: (
        "_OIDOID",
        lambda v: [
            struct.unpack_from("!I", v, 20 + i * 8 + 4)[0]
            for i in range(struct.unpack_from("!i", v, 12)[0])
        ],
        [],
    ),
}


class TransactionStatus:
    IDLE = b"I"
    IN_TRANSACTION = b"T"
    IN_FAILED_TRANSACTION = b"E"


class ClientCommand:
    """Byte codes for client commands in the PG wire protocol."""

    BIND = b"B"
    CLOSE = b"C"
    DESCRIBE = b"D"
    EXECUTE = b"E"
    FLUSH = b"H"
    QUERY = b"Q"
    PARSE = b"P"
    PASSWORD_MESSAGE = b"p"
    SYNC = b"S"
    TERMINATE = b"X"


# Bind parameters of money: its text (``-$1,234.57``) or its binary int8 cents, as a decimal.
def _money_value(v):
    """A money value as a decimal: a number, or money's own text (``-$1,234.57``)."""
    if isinstance(v, str):
        text = v.strip().replace(",", "").replace("$", "")
        return decimal.Decimal(text)
    return v if isinstance(v, decimal.Decimal) else decimal.Decimal(str(v))


def _money_of_cents(v):
    """cash_recv: money's binary form, its int8 cents, as a decimal."""
    return decimal.Decimal(struct.unpack("!q", v)[0]).scaleb(-2)


TEXT_PARAM_DECODERS[790] = _money_value
TYPE_OIDS[790] = ("MONEYOID", _money_of_cents, decimal.Decimal(0))


# REQ-2194: each wire type's PostgreSQL type OID, what RowDescription advertises. Its values are
# written by the Rust encoder (provisa._encoders.pgwire, send_data_rows); none is written here.
PG_UNKNOWN_OID = 705
BVTYPE_TO_OID: Dict[Any, int] = {
    BVType.NULL: -1,
    BVType.BIGINT: 20,
    BVType.BOOL: 16,
    BVType.BYTES: 17,
    BVType.DATE: 1082,
    BVType.FLOAT: 701,
    BVType.INTEGER: 23,
    BVType.INTERVAL: 1186,
    BVType.JSON: 114,
    BVType.DECIMAL: 1700,
    BVType.TEXT: 25,
    BVType.TIME: 1083,
    BVType.TIMESTAMP: 1114,
    BVType.ARRAY: 2277,
    BVType.INTEGERARRAY: 1007,
    BVType.STRINGARRAY: 1009,
    BVType.SMALLINT: 21,
    BVType.REAL: 700,
    BVType.JSONB: 3802,
    BVType.TIMESTAMPTZ: 1184,
    BVType.TIMETZ: 1266,
    BVType.UUID: 2950,
    BVType.BIGINTARRAY: 1016,
    BVType.SMALLINTARRAY: 1005,
    BVType.FLOATARRAY: 1022,
    BVType.REALARRAY: 1021,
    BVType.BOOLARRAY: 1000,
    BVType.DECIMALARRAY: 1231,
    BVType.OID: 26,
    BVType.REGPROC: 24,
    BVType.REGTYPE: 2206,
    BVType.OIDARRAY: 1028,
    BVType.OIDVECTOR: 30,
    BVType.INT2VECTOR: 22,
    BVType.ACLITEMARRAY: 1034,
    BVType.CHARARRAY: 1002,
    BVType.INET: 869,
    BVType.PG_LSN: 3220,
    BVType.CIDR: 650,
    BVType.MACADDR: 829,
    BVType.XML: 142,
    BVType.POINT: 600,
    BVType.MONEY: 790,
    BVType.MACADDR8: 774,
    BVType.LINE: 628,
    BVType.LSEG: 601,
    BVType.BOX: 603,
    BVType.PATH: 602,
    BVType.POLYGON: 604,
    BVType.CIRCLE: 718,
    BVType.GEOMETRY: 8310,
    BVType.GEOGRAPHY: 8311,
    BVType.INT4RANGE: 3904,
    BVType.INT8RANGE: 3926,
    BVType.NUMRANGE: 3906,
    BVType.TSRANGE: 3908,
    BVType.TSTZRANGE: 3910,
    BVType.DATERANGE: 3912,
    BVType.BIT: 1560,
    BVType.VARBIT: 1562,
    BVType.TSVECTOR: 3614,
    BVType.NAMEARRAY: 1003,
    BVType.REGTYPEARRAY: 2211,
    BVType.REGCLASS: 2205,
    BVType.REGNAMESPACE: 4089,
    BVType.INT4MULTIRANGE: 4451,
    BVType.INT8MULTIRANGE: 4536,
    BVType.NUMMULTIRANGE: 4532,
    BVType.TSMULTIRANGE: 4533,
    BVType.TSTZMULTIRANGE: 4534,
    BVType.DATEMULTIRANGE: 4535,
    BVType.MONEYARRAY: 791,
    BVType.CHAR: 18,
    BVType.NAME: 19,
    BVType.TID: 27,
    BVType.XID: 28,
    BVType.CID: 29,
    BVType.PG_NODE_TREE: 194,
    BVType.ACLITEM: 1033,
    BVType.REFCURSOR: 1790,
    BVType.REGPROCEDURE: 2202,
    BVType.REGOPER: 2203,
    BVType.REGOPERATOR: 2204,
    BVType.TXID_SNAPSHOT: 2970,
    BVType.PG_NDISTINCT: 3361,
    BVType.PG_DEPENDENCIES: 3402,
    BVType.TSQUERY: 3615,
    BVType.GTSVECTOR: 3642,
    BVType.REGCONFIG: 3734,
    BVType.REGDICTIONARY: 3769,
    BVType.JSONPATH: 4072,
    BVType.REGROLE: 4096,
    BVType.REGCOLLATION: 4191,
    BVType.PG_BRIN_BLOOM_SUMMARY: 4600,
    BVType.PG_BRIN_MINMAX_MULTI_SUMMARY: 4601,
    BVType.PG_MCV_LIST: 5017,
    BVType.PG_SNAPSHOT: 5038,
    BVType.XID8: 5069,
    BVType.INTERVALARRAY: 1187,
}


class BVBuffer(object):
    """A helper for reading and writing bytes in the format the PG wire protocol expects."""

    def __init__(self, stream=None):
        if not stream:
            stream = io.BytesIO()
        self.stream = stream

    def read_bytes(self, n) -> bytes:
        return self.stream.read(n)

    def read_byte(self) -> bytes:
        return self.read_bytes(1)

    def read_int16(self) -> int:
        data = self.read_bytes(2)
        return struct.unpack("!h", data)[0]

    def read_uint32(self) -> int:
        data = self.read_bytes(4)
        return struct.unpack("!I", data)[0]

    def read_int32(self) -> int:
        data = self.read_bytes(4)
        return struct.unpack("!i", data)[0]

    def write_bytes(self, value: bytes):
        self.stream.write(value)

    def write_byte(self, value):
        self.stream.write(struct.pack("!c", value))

    def write_int16(self, value: int):
        self.stream.write(struct.pack("!h", value))

    def write_int32(self, value: int):
        self.stream.write(struct.pack("!i", value))

    def write_string(self, value):
        self.stream.write(value.encode() if isinstance(value, str) else value)
        self.stream.write(b"\x00")

    def get_value(self) -> bytes:
        return self.stream.getvalue()


class UnknownStatement(Exception):
    """A message named a prepared statement this connection does not hold."""

    sqlstate = "26000"  # invalid_sql_statement_name

    def __init__(self, name: str):
        said = f'prepared statement "{name}"' if name else "unnamed prepared statement"
        super().__init__(f"{said} does not exist")


class UnknownPortal(Exception):
    """A message named a portal this connection does not hold."""

    sqlstate = "34000"  # invalid_cursor_name

    def __init__(self, name: str):
        super().__init__(f'portal "{name}" does not exist')


class BVContext:
    """Manages the state of a single connection to the server."""

    def __init__(self, session: Session, rewriter: Optional[Rewriter], params: Dict[str, str]):
        self.session = session
        self.rewriter = rewriter
        self.params = params
        # Below 2**31: PostgreSQL's backend pid is an int4 (pg_backend_pid, pg_stat_activity.pid).
        self.process_id = random.randint(1, 2**31 - 1)
        self.secret_key = random.randint(0, 2**32 - 1)
        session.process_id = self.process_id
        # The connection's socket, set when it is registered: what ends it from another thread.
        self.connection = None
        self.stmts = {}
        self.portals = {}
        self.result_cache = {}
        # statement name -> its Describe(Statement) result, held for the Bind that follows (see
        # describe_statement / add_portal).
        self.described = {}
        # statement name -> the shape its Describe(Statement) told the client. Every later Execute
        # of the statement is encoded to it (the client decodes by what it was told once).
        self.statement_shapes = {}
        # statement name -> what the session prepared at Describe(Statement) for the NEXT Bind's
        # Execute to continue from (single use); portal name -> the one its Bind took.
        self.statement_prepared = {}
        self.portal_prepared = {}
        # has_error: the cycle failed, and its later messages are discarded up to its Sync (a
        # Query's ends at its ReadyForQuery). transaction_failed: a statement failed in the open
        # transaction, reported 'E' by ReadyForQuery until the transaction ends.
        self.has_error = False
        self.transaction_failed = False
        self.authenticated = False
        self.salt: bytes | None = None

    def get_hashed_password(self, auth: dict) -> str:
        assert self.salt is not None
        user = self.params["user"]
        password = auth[user]
        first = hashlib.md5(password.encode("utf-8") + user.encode("utf-8")).hexdigest()
        return "md5" + hashlib.md5(first.encode("utf-8") + self.salt).hexdigest()

    def mark_error(self):
        self.has_error = True
        if self.session.in_transaction():
            self.transaction_failed = True

    def transaction_status(self):
        if self.session.in_transaction():
            if self.has_error or self.transaction_failed:
                return TransactionStatus.IN_FAILED_TRANSACTION
            else:
                return TransactionStatus.IN_TRANSACTION
        self.transaction_failed = False
        return TransactionStatus.IDLE

    def execute_sql(
        self, sql: str, params=None, result_fmt=None, *, prepared=None, shape=None
    ) -> QueryResult:
        logger.info("Input SQL: %s", sql)
        if self.rewriter:
            sql = self.rewriter.rewrite(sql)
            logger.info("Rewritten SQL: %s", sql)
        if prepared is None and shape is None:
            qr = self.session.execute_sql(sql, params, result_fmt)
        else:
            qr = self.session.execute_sql(sql, params, result_fmt, prepared=prepared, shape=shape)
        if qr.has_results():
            if result_fmt and len(result_fmt) != qr.column_count():
                qr.result_format = [result_fmt[0]] * qr.column_count()
            else:
                qr.result_format = result_fmt
        return qr

    def statement(self, name: str):
        """The (sql, parameter OIDs) of a prepared statement this connection holds."""
        held = self.stmts.get(name)
        if held is None:
            raise UnknownStatement(name)
        return held

    def portal(self, name: str):
        """The (statement, parameters, result formats) of a portal this connection holds."""
        held = self.portals.get(name)
        if held is None:
            raise UnknownPortal(name)
        return held

    def describe_portal(self, name: str) -> QueryResult:
        stmt, params, result_fmt = self.portal(name)
        sql, _ = self.stmts[stmt]
        query_result = self.execute_sql(
            sql=sql,
            params=params,
            result_fmt=result_fmt,
            prepared=self.portal_prepared.pop(name, None),
            shape=self.statement_shapes.get(stmt),
        )
        self.result_cache[name] = query_result
        return query_result

    def describe_statement(self, name: str) -> QueryResult:
        sql, param_oids = self.statement(name)
        params = []
        for typeoid in param_oids:
            type = TYPE_OIDS.get(typeoid)
            if type:
                params.append(type[2])
            else:
                raise Exception(f"Unsupported parameter type: {typeoid}")
        if self.rewriter:
            sql = self.rewriter.rewrite(sql)
        # The session describes the statement WITHOUT running it where it can: the result carries
        # the shape the client is told (``statement_shape``) and what the session prepared for the
        # Execute to continue from (``prepared``). Every later Execute of the statement is encoded
        # to that shape; the prepared work is taken by the next Bind only.
        #
        # A session result with neither is one it could only produce by running the statement (a
        # catalog answer). That run is handed to the Execute of the Bind that follows (add_portal)
        # instead of running it a second time. Only a statement without parameters qualifies: with
        # parameters this run used placeholder example values.
        qr = self.session.describe_sql(sql, params)
        self._drop_described(name)
        shape = getattr(qr, "statement_shape", None)
        if shape is not None:
            self.statement_shapes[name] = shape
            self.statement_prepared[name] = getattr(qr, "prepared", None)
        elif not param_oids and qr.has_results():
            self.described[name] = qr
        return qr

    def _drop_described(self, stmt: str) -> None:
        self.statement_shapes.pop(stmt, None)
        self.statement_prepared.pop(stmt, None)
        held = self.described.pop(stmt, None)
        if held is not None:
            held.close()

    def release_results(self) -> None:
        """Close every result this connection still holds (suspended portals, Describe-time
        results awaiting a Bind) — each may hold a live engine cursor / source connection."""
        held = list(self.result_cache.values()) + list(self.described.values())
        self.result_cache.clear()
        self.described.clear()
        self.statement_shapes.clear()
        self.statement_prepared.clear()
        self.portal_prepared.clear()
        for qr in held:
            qr.close()

    def holds_described(self, query_result: QueryResult) -> bool:
        return any(held is query_result for held in self.described.values())

    def execute_portal(self, name: str) -> QueryResult:
        if name in self.result_cache:
            query_result = self.result_cache[name]
            del self.result_cache[name]
            return query_result
        else:
            stmt, params, result_fmt = self.portal(name)
            sql, _ = self.stmts[stmt]
            qr = self.execute_sql(
                sql=sql,
                params=params,
                result_fmt=result_fmt,
                prepared=self.portal_prepared.pop(name, None),
                shape=self.statement_shapes.get(stmt),
            )
            return qr

    def add_statement(self, name: str, sql: str, param_oids: List[int]):
        # A Parse of DIFFERENT text replaces the statement, so its held Describe result is stale.
        # A re-Parse of the identical statement is the same statement — asyncpg re-sends Parse for
        # the unnamed statement between its Describe and its Bind — so the held result stays.
        prior = self.stmts.get(name)
        # asyncpg re-Parses with UNSPECIFIED (0) parameter types after its Describe; the types that
        # Describe resolved (and told the client, which then encodes its Bind values in them) must
        # survive, or a binary int8 value is decoded as if untyped.
        if prior is not None and prior[0] == sql and all(o == 0 for o in param_oids):
            param_oids = prior[1]
        if prior != (sql, param_oids):
            self._drop_described(name)
        self.stmts[name] = (sql, param_oids)

    def close_statement(self, name: str):
        # Closing a name that does not exist is no error (the protocol says so).
        self._drop_described(name)
        self.stmts.pop(name, None)

    def add_portal(self, name: str, stmt: str, params: List, result_formats: List[int]):
        # A Bind REPLACES the portal it names (the unnamed portal on every simple client query). A
        # result the PREVIOUS query left cached under this name — suspended because it exactly
        # filled an Execute(limit), e.g. asyncpg's fetchval (limit=1) on a 1-row result — belongs
        # to that old portal: serving it to the new one returned the old, drained result (0 rows)
        # and never ran the new query. Close it with the portal it belonged to.
        stale = self.result_cache.pop(name, None)
        if stale is not None:
            stale.close()
        self.portals[name] = (stmt, params, result_formats)
        # What the statement's Describe prepared goes to THIS portal's Execute, once.
        self.portal_prepared.pop(name, None)
        prepared = self.statement_prepared.pop(stmt, None)
        if prepared is not None:
            self.portal_prepared[name] = prepared
        # Hand the statement's Describe-time result to this portal's Execute (see
        # describe_statement): it is the same statement with no parameters, so the result IS what
        # the Execute would compute. Used once — a later Bind of the statement runs it fresh.
        held = self.described.pop(stmt, None)
        if held is None:
            return
        if params or not held.reusable_for_execute(result_formats):
            held.close()
            return
        if result_formats and len(result_formats) != held.column_count():
            held.result_format = [result_formats[0]] * held.column_count()
        else:
            held.result_format = result_formats
        self.result_cache[name] = held

    def close_portal(self, name: str):
        self.portals.pop(name, None)
        self.portal_prepared.pop(name, None)
        # A Describe-then-Close flow (no Execute — e.g. a client fetching metadata only, or
        # aborting after Describe) leaves an already-executed result in result_cache with nobody
        # ever draining it. Close it here so a live cursor/source-connection it holds is released
        # deterministically instead of waiting on GC (which may never run promptly, or at all, for
        # a generator suspended before its first yield — see StreamingQueryResult.close()).
        query_result = self.result_cache.pop(name, None)
        if query_result is not None:
            query_result.close()

    def flush(self):
        pass

    def sync(self):
        if self.has_error:
            self.has_error = False


class BuenaVistaHandler(socketserver.StreamRequestHandler):
    def handle(self):
        self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        self.r = BVBuffer(self.rfile)
        ctx = None
        try:
            ctx = self.handle_startup(self.server.conn)  # type: ignore[attr-defined]
            if ctx:
                ctx.connection = self.request
                self.server.ctxts[ctx.process_id] = ctx  # type: ignore[attr-defined]
            while ctx:
                type_code = self.r.read_byte()
                if not type_code or type_code == ClientCommand.TERMINATE:
                    break

                msglen = self.r.read_uint32()
                if msglen > 4:
                    payload = self.r.read_bytes(msglen - 4)
                else:
                    payload = b""

                if not ctx.authenticated:
                    if type_code == ClientCommand.PASSWORD_MESSAGE:
                        self.handle_md5_password(ctx, payload)
                    else:
                        raise Exception("Not authenticated")
                elif type_code == ClientCommand.QUERY:
                    self.handle_query(ctx, payload)
                elif type_code == ClientCommand.PARSE:
                    self.handle_parse(ctx, payload)
                elif type_code == ClientCommand.BIND:
                    self.handle_bind(ctx, payload)
                elif type_code == ClientCommand.DESCRIBE:
                    self.handle_describe(ctx, payload)
                elif type_code == ClientCommand.EXECUTE:
                    self.handle_execute(ctx, payload)
                elif type_code == ClientCommand.CLOSE:
                    self.handle_close(ctx, payload)
                elif type_code == ClientCommand.SYNC:
                    ctx.sync()
                    self.send_ready_for_query(ctx)
                elif type_code == ClientCommand.FLUSH:
                    ctx.flush()
                else:
                    raise Exception("Unknown type_code: %s" % type_code)
        except Exception as e:
            logger.exception(e)
            self.send_error(e)

        if ctx:
            ctx.release_results()
            self.server.conn.close_session(ctx.session)  # type: ignore[attr-defined]
            del self.server.ctxts[ctx.process_id]  # type: ignore[attr-defined]
            ctx = None

    def handle_startup(self, conn: Connection) -> Optional["BVContext"]:
        msglen = self.r.read_uint32() - 4
        code = self.r.read_uint32()
        if code == 80877103:  ## SSL request
            self.send_notice()
            return self.handle_startup(conn)
        elif code == 196608:  # Protocol 3.0
            msg = [x.decode("utf-8") for x in self.r.read_bytes(msglen - 4).split(NULL_BYTE)]
            params = dict(zip(msg[::2], msg[1::2]))
            logger.info("Client connection params: %s", params)
            ctx = BVContext(conn.create_session(), self.server.rewriter, params)  # type: ignore[attr-defined]
            self.send_auth_request(ctx)
            return ctx
        elif code == 80877102:  ## Cancel request
            process_id, secret_key = self.r.read_uint32(), self.r.read_uint32()
            ctx = self.server.ctxts.get(process_id)  # type: ignore[attr-defined]
            if ctx and ctx.secret_key == secret_key:
                self.server.conn.close_session(ctx.session)  # type: ignore[attr-defined]
                del self.server.ctxts[ctx.process_id]  # type: ignore[attr-defined]
                ctx = None
            return None
        else:
            raise Exception(f"Unsupported startup message: {code}")

    def send_auth_request(self, ctx: BVContext):
        if self.server.auth is None:  # type: ignore[attr-defined]
            self.send_authentication_ok()
            self.handle_post_auth(ctx)
        else:
            self.send_authentication_md5(ctx)

    def send_authentication_ok(self):
        self.wfile.write(struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 8, 0))

    def send_authentication_md5(self, ctx: BVContext):
        ctx.salt = os.urandom(4)
        self.wfile.write(struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 12, 5))
        self.wfile.write(ctx.salt)

    def handle_md5_password(self, ctx: BVContext, payload: bytes):
        client_side = payload.decode("utf-8").rstrip("\x00")
        server_side = ctx.get_hashed_password(self.server.auth)  # type: ignore[attr-defined]
        if client_side == server_side:
            self.send_authentication_ok()
            self.handle_post_auth(ctx)
        else:
            self.send_error("Invalid password")

    def handle_post_auth(self, ctx: BVContext):
        self.send_parameter_status(self.server.conn.parameters())  # type: ignore[attr-defined]
        self.send_backend_key_data(ctx)
        self.send_ready_for_query(ctx)
        ctx.authenticated = True
        return

    def handle_query(self, ctx: BVContext, payload: bytes):
        logger.debug("Handle query")
        decoded = payload.decode("utf-8").rstrip("\x00")
        try:
            if req := Extension.check_json(decoded):
                method = req.get("method")
                extension = self.server.extensions.get(method)  # type: ignore[attr-defined]
                if not extension:
                    raise Exception("Unknown method: " + str(method))
                else:
                    query_result = extension.apply(req.get("params"), ctx.session)
            else:
                query_result = ctx.execute_sql(decoded)
        except Exception as e:
            self.send_error(e)
            self.send_ready_for_query(ctx)
            return

        if not query_result:
            raise Exception("No query result for query: " + decoded)
        elif query_result.has_results():
            self.send_row_description(query_result)
            row_count = self.send_data_rows(query_result)
            self.send_command_complete("SELECT %d\x00" % row_count)
        else:
            status = query_result.status()
            self.send_command_complete(f"{status}\x00")
        self.send_ready_for_query(ctx)

    def handle_parse(self, ctx: BVContext, payload: bytes):
        logger.debug("Handling parse")
        ba = bytearray(payload)
        stmt_idx = ba.index(NULL_BYTE)
        stmt = ba[:stmt_idx].decode("utf-8")
        query_idx = ba.index(NULL_BYTE, stmt_idx + 1)
        sql = ba[stmt_idx + 1 : query_idx].decode("utf-8")
        logger.debug("Parsed statement: %s", sql)
        buf = BVBuffer(io.BytesIO(ba[query_idx + 1 :]))
        num_params = buf.read_int16()
        param_oids = []
        for _ in range(num_params):
            param_oids.append(buf.read_int32())
        ctx.add_statement(stmt, sql, param_oids)
        self.send_parse_complete()

    def handle_bind(self, ctx: BVContext, payload: bytes):
        logger.debug("Handling bind")
        ba = bytearray(payload)
        portal_idx = ba.index(NULL_BYTE)
        portal = ba[:portal_idx].decode("utf-8")
        stmt_idx = ba.index(NULL_BYTE, portal_idx + 1)
        stmt = ba[portal_idx + 1 : stmt_idx].decode("utf-8")
        buf = BVBuffer(io.BytesIO(ba[stmt_idx + 1 :]))
        num_formats = buf.read_int16()
        formats = []
        for i in range(num_formats):
            formats.append(buf.read_int16())
        num_params = buf.read_int16()
        if num_formats < num_params:
            if formats:
                formats = [formats[0]] * num_params
            else:
                formats = [0] * num_params
        params = []
        for i in range(num_params):
            nb = buf.read_int32()
            if nb == -1:
                params.append(None)
                continue
            v = buf.read_bytes(nb)
            stmt_oids = ctx.stmts[stmt][1]
            typeoid = stmt_oids[i] if i < len(stmt_oids) else 0
            logger.debug("Format: %d, Type: %d", formats[i], typeoid)
            if formats[i] == 0:
                decoded = v.decode("utf-8")
                if typeoid in TEXT_PARAM_DECODERS:
                    params.append(TEXT_PARAM_DECODERS[typeoid](decoded))
                elif typeoid == 0:
                    # Unspecified type — store raw string; do not date-parse
                    if decoded.startswith("{") and decoded.endswith("}"):
                        params.append(decoded[1:-1].split(","))
                    else:
                        params.append(decoded)
                else:
                    if decoded.startswith("{") and decoded.endswith("}"):
                        params.append(decoded[1:-1].split(","))
                    else:
                        params.append(decoded)
            else:
                type = TYPE_OIDS.get(typeoid)
                if type:
                    params.append(type[1](v))
                else:
                    raise Exception(f"Unsupported binary parameter type: {typeoid}")

        logger.debug("Bind params: %s", params)
        num_result_formats = buf.read_int16()
        result_formats = []
        for i in range(num_result_formats):
            result_formats.append(buf.read_int16())
        ctx.add_portal(portal, stmt, params, result_formats)
        self.send_bind_complete()

    def handle_describe(self, ctx: BVContext, payload: bytes):
        logger.debug("Handling describe")
        ba = bytearray(payload)
        describe_type = ba[0]
        query_result = None
        if describe_type == ord("P"):
            portal = ba[1 : len(ba) - 1].decode("utf-8")
            try:
                query_result = ctx.describe_portal(portal)
            except Exception as e:
                self.send_error(e, ctx)
                return
        elif describe_type == ord("S"):
            stmt = ba[1 : len(ba) - 1].decode("utf-8")
            try:
                query_result = ctx.describe_statement(stmt)
            except Exception as e:
                self.send_error(e, ctx)
                return
            param_oids = ctx.stmts[stmt][1]
            self.send_parameter_description(param_oids)
        else:
            raise Exception(f"Unknown describe type: {describe_type}")
        if query_result.has_results():
            self.send_row_description(query_result)
        else:
            self.send_no_data()

    def handle_execute(self, ctx: BVContext, payload: bytes):
        logger.debug("Handling execute")
        if ctx.has_error:
            logger.info("Skipping execute due to previous error")
            return
        ba = bytearray(payload)
        portal_idx = ba.index(NULL_BYTE)
        portal = ba[:portal_idx].decode("utf-8")
        limit = struct.unpack("!i", ba[portal_idx + 1 : portal_idx + 5])[0]
        query_result = None
        try:
            query_result = ctx.execute_portal(portal)
        except Exception as e:
            self.send_error(e, ctx)
            return
        if query_result.has_results():
            row_count = self.send_data_rows(query_result, limit)
            if limit == 0 or row_count < limit:
                self.send_command_complete("SELECT %d\x00" % row_count)
            else:
                ctx.result_cache[portal] = query_result
                self.send_portal_suspend()
        else:
            status = query_result.status()
            self.send_command_complete(f"{status}\x00")

    def handle_close(self, ctx: BVContext, payload: bytes):
        logger.debug("Handling close")
        close_type = payload[0]
        if close_type == ord("S"):
            ctx.close_statement(payload[1:-1].decode("utf-8"))
        elif close_type == ord("P"):
            ctx.close_portal(payload[1:-1].decode("utf-8"))
        else:
            raise Exception(f"Unknown close type: {close_type}")
        self.send_close_complete()

    def send_parameter_description(self, param_oids: List[int]):
        buf = BVBuffer()
        for oid in param_oids:
            buf.write_int32(oid)
        out = buf.get_value()
        sig = struct.pack(
            "!cih",
            ServerResponse.PARAMETER_DESCRIPTION,
            len(out) + 6,
            len(param_oids),
        )
        self.wfile.write(sig + out)

    def send_row_description(self, query_result: QueryResult):
        buf = BVBuffer()
        for i in range(query_result.column_count()):
            name, bvtype = query_result.column(i)
            oid = query_result.column_type_oid(i)
            if oid is None:
                oid = BVTYPE_TO_OID.get(bvtype, PG_UNKNOWN_OID)
            buf.write_string(name)
            fmt = query_result.result_format[i] if query_result.result_format else 0
            typmod = query_result.column_typmod(i)
            buf.write_bytes(struct.pack("!ihihih", 0, 0, oid, 0, typmod, fmt))
        out = buf.get_value()
        sig = struct.pack(
            "!cih",
            ServerResponse.ROW_DESCRIPTION,
            len(out) + 6,
            query_result.column_count(),
        )
        self.wfile.write(sig + out)

    def send_data_rows(self, query_result: QueryResult, limit: int = 0) -> int:
        """The result's DataRow messages onto the socket, ``limit`` rows of them (0: all), written
        by the Rust encoder (REQ-2194): its Arrow batches with each column's wire type, result
        format and coercers, or a source's own DataRow messages forwarded as they are. No Python
        row is built."""
        # Imported here, not with this module: the code host's front end (provisa/codehost) runs
        # buenavista's modules on their own, and imports them before any result is sent.
        from provisa._encoders import pgwire as encoder

        data = query_result.data()
        write = self.wfile.write
        if data.raw:
            return encoder.forward_raw((row for batch in data.take(limit) for row in batch), write)
        count = query_result.column_count()
        return encoder.encode_stream(
            data.take(limit),
            [wire_type_name(query_result.column(i)[1]) for i in range(count)],
            write,
            list(query_result.result_format) if query_result.result_format else None,
            query_result.coercers(),
        )

    def send_error(self, exception, ctx: Optional[BVContext] = None):
        estr = str(exception)
        logger.error(estr)
        buf = BVBuffer()
        buf.write_byte(b"M")
        buf.write_string(estr)
        buf.write_byte(NULL_BYTE)
        out = buf.get_value()
        err_sig = struct.pack("!ci", ServerResponse.ERROR_RESPONSE, len(out) + 4)
        self.wfile.write(err_sig + out)
        if ctx:
            ctx.mark_error()

    def send_notice(self):
        self.wfile.write(ServerResponse.NOTICE_RESPONSE)

    def send_backend_key_data(self, ctx):
        self.wfile.write(
            struct.pack(
                "!ciII",
                ServerResponse.BACKEND_KEY_DATA,
                12,
                ctx.process_id,
                ctx.secret_key,
            )
        )

    def send_ready_for_query(self, ctx: Optional[BVContext]):
        logger.debug("Sending ready for query")
        status = ctx.transaction_status() if ctx else TransactionStatus.IDLE
        self.wfile.write(struct.pack("!cic", ServerResponse.READY_FOR_QUERY, 5, status))

    def send_parameter_status(self, params: Dict[str, str]):
        for name, value in params.items():
            buf = BVBuffer()
            buf.write_string(name)
            buf.write_string(value)
            out = buf.get_value()
            psig = struct.pack("!ci", ServerResponse.PARAMETER_STATUS, len(out) + 4)
            self.wfile.write(psig + out)

    def send_parse_complete(self):
        self.wfile.write(struct.pack("!ci", ServerResponse.PARSE_COMPLETE, 4))

    def send_bind_complete(self):
        self.wfile.write(struct.pack("!ci", ServerResponse.BIND_COMPLETE, 4))

    def send_close_complete(self):
        self.wfile.write(struct.pack("!ci", ServerResponse.CLOSE_COMPLETE, 4))

    def send_no_data(self):
        self.wfile.write(struct.pack("!ci", ServerResponse.NO_DATA, 4))

    def send_portal_suspend(self):
        self.wfile.write(struct.pack("!ci", ServerResponse.PORTAL_SUSPENDED, 4))

    def send_command_complete(self, tag: str):
        buf = BVBuffer()
        buf.write_bytes(struct.pack("!ci", ServerResponse.COMMAND_COMPLETE, len(tag) + 4))
        buf.write_bytes(tag.encode())
        out = buf.get_value()
        self.wfile.write(out)


class BuenaVistaServer(socketserver.ThreadingTCPServer):
    """A Python socketserver for the Buena Vista Postgres proxy."""

    allow_reuse_address = True

    def __init__(
        self,
        server_address,
        conn: Connection,
        *,
        rewriter: Optional[Rewriter] = None,
        extensions: Optional[List[Extension]] = None,
        auth: Optional[Dict[str, str]] = None,
    ):
        super().__init__(server_address, BuenaVistaHandler)
        self.conn = conn
        self.rewriter = rewriter
        self.extensions = {e.type(): e for e in extensions or []}
        self.ctxts = {}
        self.auth = auth

    def verify_request(self, request, client_address) -> bool:
        del request
        return client_address[0] == "127.0.0.1" or "BUENAVISTA_HOST" in os.environ
