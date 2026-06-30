# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3f1a2e-4c8b-4e9f-a5d2-1b6c3e8f2a4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PackStream codec for Bolt protocol.

PackStream is MessagePack-compatible for scalars/lists/dicts, extended with
Neo4j struct types encoded as msgpack Ext objects.

Struct tags (single byte, written as the Ext type byte):
  Node         0x4E  ('N')
  Relationship 0x52  ('R')
  Path         0x50  ('P')
  Date         0x44  ('D')
  DateTime     0x49  ('I')
  LocalDT      0x64  ('d')
  Duration     0x45  ('E')

All Neo4j Browser query results travel through pack(); unpack() is only
needed for incoming RUN parameters.
"""

from __future__ import annotations

import datetime
import struct
from decimal import Decimal
from typing import Any


# ── Struct tag constants ───────────────────────────────────────────────────────

TAG_NODE = 0x4E
TAG_RELATIONSHIP = 0x52
TAG_UNBOUND_RELATIONSHIP = 0x72
TAG_PATH = 0x50
TAG_DATE = 0x44
TAG_DATETIME_TZ = 0x49
TAG_LOCAL_DATETIME = 0x64
TAG_DURATION = 0x45


# ── Struct builders (produce msgpack Ext objects) ──────────────────────────────


def _node_ext(element_id: int, labels: list[str], props: dict) -> bytes:
    """Pack a Node struct: (elementId, labels, properties, elementId_str)."""
    # Bolt 5.x Node: struct{N}(element_id: Integer, labels: [String], properties: {String→Value},
    #                          element_id_string: String)
    fields = [element_id, labels, props, str(element_id)]
    inner = b"".join(_pack_value(f) for f in fields)
    header = bytes([0xB4, TAG_NODE])  # 0xB4 = tiny struct with 4 fields
    return header + inner


def _rel_ext(
    element_id: int,
    start_id: int,
    end_id: int,
    rel_type: str,
    props: dict,
) -> bytes:
    """Pack a Relationship struct (Bolt 5.x, 8 fields)."""
    fields = [
        element_id,
        start_id,
        end_id,
        rel_type,
        props,
        str(element_id),
        str(start_id),
        str(end_id),
    ]
    inner = b"".join(_pack_value(f) for f in fields)
    header = bytes([0xB8, TAG_RELATIONSHIP])  # 0xB8 = tiny struct with 8 fields
    return header + inner


def _unbound_rel_ext(element_id: int, rel_type: str, props: dict) -> bytes:
    """Pack an UnboundRelationship struct for use inside Path (Bolt 5.x, 4 fields)."""
    fields = [element_id, rel_type, props, str(element_id)]
    inner = b"".join(_pack_value(f) for f in fields)
    header = bytes([0xB4, TAG_UNBOUND_RELATIONSHIP])  # 0xB4 = tiny struct with 4 fields
    return header + inner


def _date_bytes(d: datetime.date) -> bytes:
    epoch = datetime.date(1970, 1, 1)
    days = (d - epoch).days
    header = bytes([0xB1, TAG_DATE])
    return header + _pack_value(days)


def _datetime_bytes(dt: datetime.datetime) -> bytes:
    import calendar

    if dt.tzinfo is not None:
        ts = int(calendar.timegm(dt.utctimetuple()))
        nanos = dt.microsecond * 1000
        tz_offset = int(dt.utcoffset().total_seconds()) if dt.utcoffset() else 0  # type: ignore[union-attr]
        header = bytes([0xB3, TAG_DATETIME_TZ])
        return header + _pack_value(ts) + _pack_value(nanos) + _pack_value(tz_offset)
    else:
        ts = int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())
        nanos = dt.microsecond * 1000
        header = bytes([0xB2, TAG_LOCAL_DATETIME])
        return header + _pack_value(ts) + _pack_value(nanos)


# ── Core pack / unpack ─────────────────────────────────────────────────────────


def _pack_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    n = len(encoded)
    if n <= 15:
        return bytes([0x80 | n]) + encoded
    if n <= 0xFF:
        return struct.pack("!BB", 0xD0, n) + encoded
    if n <= 0xFFFF:
        return struct.pack("!BH", 0xD1, n) + encoded
    return struct.pack("!BI", 0xD2, n) + encoded


def _pack_value(value: Any) -> bytes:
    """Encode a single Python value to PackStream bytes."""
    if value is None:
        return b"\xc0"
    if isinstance(value, bool):
        return b"\xc3" if value else b"\xc2"
    if isinstance(value, int):
        # PackStream uses signed integer encoding only — never msgpack unsigned (0xCC-0xCF).
        if -16 <= value <= 127:
            return struct.pack("!b", value)
        if -128 <= value <= 127:
            return struct.pack("!Bb", 0xC8, value)
        if -32768 <= value <= 32767:
            return struct.pack("!Bh", 0xC9, value)
        if -2147483648 <= value <= 2147483647:
            return struct.pack("!Bi", 0xCA, value)
        return struct.pack("!Bq", 0xCB, value)
    if isinstance(value, float):
        return struct.pack("!Bd", 0xC1, value)
    if isinstance(value, Decimal):
        return struct.pack("!Bd", 0xC1, float(value))
    if isinstance(value, str):
        return _pack_string(value)
    if isinstance(value, bytes):
        n = len(value)
        if n <= 0xFF:
            return struct.pack("!BB", 0xCC, n) + value
        if n <= 0xFFFF:
            return struct.pack("!BH", 0xCD, n) + value
        return struct.pack("!BI", 0xCE, n) + value
    if isinstance(value, datetime.datetime):
        return _datetime_bytes(value)
    if isinstance(value, datetime.date):
        return _date_bytes(value)
    if isinstance(value, list):
        header = _list_header(len(value))
        return header + b"".join(_pack_value(v) for v in value)
    if isinstance(value, dict):
        # Check if this is a serialized node or relationship from to_serializable()
        if _is_node_dict(value):
            return _pack_node_dict(value)
        if _is_rel_dict(value):
            return _pack_rel_dict(value)
        if _is_path_dict(value):
            return _pack_path_dict(value)
        header = _map_header(len(value))
        body = b"".join(_pack_value(k) + _pack_value(v) for k, v in value.items())
        return header + body
    # Fallback: stringify
    return _pack_string(str(value))


def _list_header(n: int) -> bytes:
    if n <= 15:
        return bytes([0x90 | n])
    if n <= 0xFF:
        return struct.pack("!BB", 0xD4, n)  # list8
    if n <= 0xFFFF:
        return struct.pack("!BH", 0xD5, n)  # list16
    return struct.pack("!BI", 0xD6, n)  # list32


def _map_header(n: int) -> bytes:
    if n <= 15:
        return bytes([0xA0 | n])
    if n <= 0xFF:
        return struct.pack("!BB", 0xD8, n)  # map8
    if n <= 0xFFFF:
        return struct.pack("!BH", 0xD9, n)  # map16
    return struct.pack("!BI", 0xDA, n)  # map32


# ── Dict shape detectors ───────────────────────────────────────────────────────


def _is_node_dict(d: dict) -> bool:
    return "id" in d and "label" in d and "properties" in d and "startNode" not in d


def _is_rel_dict(d: dict) -> bool:
    return "type" in d and "startNode" in d and "endNode" in d


def _is_path_dict(d: dict) -> bool:
    return "nodes" in d and "edges" in d


def _pack_node_dict(d: dict) -> bytes:
    element_id = d["id"] if isinstance(d["id"], int) else 0
    label = d.get("label", "")
    # "Domain:Table" → two labels [Table, Domain] so both appear in the graph;
    # both are also listed by db.labels().
    if ":" in label:
        domain, table = label.split(":", 1)
        labels = [table, domain]
    else:
        labels = [label] if label else []
    props = {k: v for k, v in (d.get("properties") or {}).items() if v is not None}
    return _node_ext(element_id, labels, props)


def _pack_rel_dict(d: dict) -> bytes:
    raw_id = d.get("identity", 0)
    # Relationship element_id must be a unique integer; identities are composite strings
    # (e.g. "SUBMITTED_BY:1-1"). Without this, every edge packs as id 0 and the Browser
    # dedups them all to one.
    if isinstance(raw_id, int):
        element_id = raw_id
    elif raw_id:
        element_id = abs(hash(raw_id)) & 0x7FFFFFFF
    else:
        element_id = 0
    start = d.get("startNode") or {}
    end = d.get("endNode") or {}
    start_id = start.get("id", 0) if isinstance(start, dict) else 0
    end_id = end.get("id", 0) if isinstance(end, dict) else 0
    if not isinstance(start_id, int):
        start_id = 0
    if not isinstance(end_id, int):
        end_id = 0
    props = {k: v for k, v in (d.get("properties") or {}).items() if v is not None}
    return _rel_ext(element_id, start_id, end_id, d.get("type", ""), props)


def _pack_unbound_rel(d: dict) -> bytes:
    """Pack an edge dict as UnboundRelationship for use inside a Path struct."""
    raw_id = d.get("identity", "")
    # Derive a stable integer ID from the identity string.
    element_id = abs(hash(raw_id)) & 0x7FFFFFFF if raw_id else 0
    props = {k: v for k, v in (d.get("properties") or {}).items() if v is not None}
    return _unbound_rel_ext(element_id, d.get("type", ""), props)


def _pack_path_dict(d: dict) -> bytes:
    nodes = d.get("nodes", [])
    edges = d.get("edges", [])
    # Path struct: (nodes: [Node], rels: [UnboundRelationship], sequence: [Integer])
    # Sequence encodes alternating rel-index (1-based, negative = reversed) and node-index.
    sequence = []
    for i, _ in enumerate(edges):
        sequence.append(i + 1)  # rel index (1-based)
        sequence.append(i + 1)  # next node index
    packed_nodes = _list_header(len(nodes)) + b"".join(_pack_value(n) for n in nodes)
    # Edges inside a Path must be UnboundRelationship (tag 0x72, 4 fields), not full Relationship.
    packed_rels = _list_header(len(edges)) + b"".join(_pack_unbound_rel(e) for e in edges)
    packed_seq = _list_header(len(sequence)) + b"".join(_pack_value(s) for s in sequence)
    header = bytes([0xB3, TAG_PATH])
    return header + packed_nodes + packed_rels + packed_seq


def pack(value: Any) -> bytes:
    """Top-level pack: encode any Python value to PackStream bytes."""
    return _pack_value(value)


def pack_dict(d: dict) -> bytes:
    """Pack a dict as a PackStream map (used for message fields)."""
    return _pack_value(d)


def _unpack_one(data: bytes, offset: int) -> tuple[Any, int]:
    """Parse one PackStream value; return (value, next_offset)."""
    marker = data[offset]
    offset += 1

    # Tiny positive int (0x00-0x7F)
    if marker <= 0x7F:
        return marker, offset
    # Tiny negative int (0xF0-0xFF)
    if marker >= 0xF0:
        return struct.unpack("!b", bytes([marker]))[0], offset

    # Null
    if marker == 0xC0:
        return None, offset
    # Bool
    if marker == 0xC2:
        return False, offset
    if marker == 0xC3:
        return True, offset
    # Float64
    if marker == 0xC1:
        v = struct.unpack("!d", data[offset : offset + 8])[0]
        return v, offset + 8
    # Signed ints
    if marker == 0xC8:
        return struct.unpack("!b", data[offset : offset + 1])[0], offset + 1
    if marker == 0xC9:
        return struct.unpack("!h", data[offset : offset + 2])[0], offset + 2
    if marker == 0xCA:
        return struct.unpack("!i", data[offset : offset + 4])[0], offset + 4
    if marker == 0xCB:
        return struct.unpack("!q", data[offset : offset + 8])[0], offset + 8
    # Bytes
    if marker == 0xCC:
        n = data[offset]; offset += 1
        return bytes(data[offset : offset + n]), offset + n
    if marker == 0xCD:
        n = struct.unpack("!H", data[offset : offset + 2])[0]; offset += 2
        return bytes(data[offset : offset + n]), offset + n
    if marker == 0xCE:
        n = struct.unpack("!I", data[offset : offset + 4])[0]; offset += 4
        return bytes(data[offset : offset + n]), offset + n
    # String (PackStream: 0x80-0x8F tiny, 0xD0-0xD2 for larger)
    if 0x80 <= marker <= 0x8F:
        n = marker & 0x0F
        return data[offset : offset + n].decode("utf-8"), offset + n
    if marker == 0xD0:
        n = data[offset]; offset += 1
        return data[offset : offset + n].decode("utf-8"), offset + n
    if marker == 0xD1:
        n = struct.unpack("!H", data[offset : offset + 2])[0]; offset += 2
        return data[offset : offset + n].decode("utf-8"), offset + n
    if marker == 0xD2:
        n = struct.unpack("!I", data[offset : offset + 4])[0]; offset += 4
        return data[offset : offset + n].decode("utf-8"), offset + n
    # List (0x90-0x9F tiny, 0xD4-0xD6 for larger)
    if 0x90 <= marker <= 0x9F:
        n = marker & 0x0F
        lst: list = []
        for _ in range(n):
            v, offset = _unpack_one(data, offset)
            lst.append(v)
        return lst, offset
    if marker == 0xD4:
        n = data[offset]; offset += 1
        lst = []
        for _ in range(n):
            v, offset = _unpack_one(data, offset)
            lst.append(v)
        return lst, offset
    if marker == 0xD5:
        n = struct.unpack("!H", data[offset : offset + 2])[0]; offset += 2
        lst = []
        for _ in range(n):
            v, offset = _unpack_one(data, offset)
            lst.append(v)
        return lst, offset
    if marker == 0xD6:
        n = struct.unpack("!I", data[offset : offset + 4])[0]; offset += 4
        lst = []
        for _ in range(n):
            v, offset = _unpack_one(data, offset)
            lst.append(v)
        return lst, offset
    # Dictionary/Map (0xA0-0xAF tiny, 0xD8-0xDA for larger)
    if 0xA0 <= marker <= 0xAF:
        n = marker & 0x0F
        d: dict = {}
        for _ in range(n):
            k, offset = _unpack_one(data, offset)
            v, offset = _unpack_one(data, offset)
            d[k] = v
        return d, offset
    if marker == 0xD8:
        n = data[offset]; offset += 1
        d = {}
        for _ in range(n):
            k, offset = _unpack_one(data, offset)
            v, offset = _unpack_one(data, offset)
            d[k] = v
        return d, offset
    if marker == 0xD9:
        n = struct.unpack("!H", data[offset : offset + 2])[0]; offset += 2
        d = {}
        for _ in range(n):
            k, offset = _unpack_one(data, offset)
            v, offset = _unpack_one(data, offset)
            d[k] = v
        return d, offset
    if marker == 0xDA:
        n = struct.unpack("!I", data[offset : offset + 4])[0]; offset += 4
        d = {}
        for _ in range(n):
            k, offset = _unpack_one(data, offset)
            v, offset = _unpack_one(data, offset)
            d[k] = v
        return d, offset
    # Struct (0xB0-0xBF tiny struct)
    if 0xB0 <= marker <= 0xBF:
        n_fields = marker & 0x0F
        tag_byte = data[offset]; offset += 1
        struct_fields: list = []
        for _ in range(n_fields):
            v, offset = _unpack_one(data, offset)
            struct_fields.append(v)
        return {"_tag": tag_byte, "_fields": struct_fields}, offset

    raise ValueError(f"Unknown PackStream marker 0x{marker:02X} at offset {offset - 1}")


def unpack(data: bytes) -> Any:
    """Decode PackStream bytes to Python."""
    value, _ = _unpack_one(data, 0)
    return value


def unpack_fields(data: bytes) -> list[Any]:
    """Decode all PackStream values from data (used for Bolt message fields)."""
    results = []
    offset = 0
    while offset < len(data):
        value, offset = _unpack_one(data, offset)
        results.append(value)
    return results


def pack_message(tag: int, *fields: Any) -> bytes:
    """Encode a complete Bolt message: tiny-struct header + tag + packed fields."""
    n = len(fields)
    header = bytes([0xB0 | n, tag])
    body = b"".join(_pack_value(f) for f in fields)
    return header + body
