# Copyright (c) 2026 Kenneth Stott
# Canary: 4502bf59-b180-4deb-887f-57a47cc9ca23
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDB `airport` extension wire protocol codec (REQ-1106).

Byte-exact reimplementation of the msgpack/zstd envelopes the airport-go
reference server (github.com/hugr-lab/airport-go v0.2.1) emits, so the real
DuckDB airport C++ extension can ATTACH and SELECT against Provisa.

Protocol facts (from airport-go flight/doaction.go, flight/doaction_metadata.go):
  * DoAction bodies are MessagePack. Structs with `msgpack:"tag"` → maps with
    string keys; `MSGPACK_DEFINE` (not _MAP) tuples → arrays.
  * Compressed catalog payloads are wrapped ``[uint32 uncompressed_len, str data]``
    where ``data`` is a raw zstd frame carried in a msgpack **str** (Go
    ``string(compressedBytes)``), NOT a msgpack bin. Python's high-level packer
    cannot emit arbitrary bytes as a str, so the encoders below build the byte
    stream directly (:class:`_Str` marks a byte payload for str framing).
  * FlightInfo / FlightEndpoint / FlightDescriptor are Arrow Flight protobufs;
    pyarrow.flight.*.serialize() produces the identical wire format proto.Marshal
    does, so the protobuf layer is delegated to pyarrow.
"""

from __future__ import annotations

import hashlib
import struct
from typing import Any

import msgpack
import zstandard


class _Str:
    """Marks a byte payload to be framed as a msgpack ``str`` (not ``bin``).

    airport-go carries zstd frames as Go ``string`` values; the C++ decoder reads
    them as msgpack str. Emitting them as bin would change the type byte and the
    extension would reject the catalog.
    """

    __slots__ = ("data",)

    def __init__(self, data: bytes) -> None:
        self.data = data


def _packb(obj: Any, **kw: Any) -> bytes:
    data = msgpack.packb(obj, **kw)
    assert data is not None  # msgpack.packb only returns None for a None packer sink
    return data


def _emit_str(data: bytes) -> bytes:
    n = len(data)
    if n <= 0x1F:
        hdr = bytes([0xA0 | n])
    elif n <= 0xFF:
        hdr = bytes([0xD9, n])
    elif n <= 0xFFFF:
        hdr = b"\xda" + struct.pack(">H", n)
    else:
        hdr = b"\xdb" + struct.pack(">I", n)
    return hdr + data


def _encode(obj: Any) -> bytes:
    """Minimal msgpack encoder with full control over str-vs-bin framing.

    Handles exactly the shapes the airport catalog needs: dict→map(str keys),
    list/tuple→array, ``_Str``→str-bytes, str→str, bytes→bin, bool, None, int.
    """
    if obj is None:
        return b"\xc0"
    if isinstance(obj, bool):
        return b"\xc3" if obj else b"\xc2"
    if isinstance(obj, _Str):
        return _emit_str(obj.data)
    if isinstance(obj, str):
        return _emit_str(obj.encode("utf-8"))
    if isinstance(obj, bytes):
        return _packb(obj, use_bin_type=True)  # bin framing
    if isinstance(obj, int):
        return _packb(obj)  # minimal int framing; C++ reads as integer
    if isinstance(obj, dict):
        n = len(obj)
        if n <= 0xF:
            out = bytes([0x80 | n])
        elif n <= 0xFFFF:
            out = b"\xde" + struct.pack(">H", n)
        else:
            out = b"\xdf" + struct.pack(">I", n)
        for k, v in obj.items():
            out += _emit_str(k.encode("utf-8")) + _encode(v)
        return out
    if isinstance(obj, (list, tuple)):
        n = len(obj)
        if n <= 0xF:
            out = bytes([0x90 | n])
        elif n <= 0xFFFF:
            out = b"\xdc" + struct.pack(">H", n)
        else:
            out = b"\xdd" + struct.pack(">I", n)
        for v in obj:
            out += _encode(v)
        return out
    raise TypeError(f"airport wire: unencodable {type(obj)!r}")


def _zstd(data: bytes) -> bytes:
    return zstandard.ZstdCompressor(level=3).compress(data)


def _compressed_content(uncompressed: bytes) -> bytes:
    """``AirportSerializedCompressedContent`` = msgpack ``[uint32 len, str zstd]``.

    The length MUST be a full msgpack uint32 (0xce): airport-go emits ``uint32(len)`` and the
    C++ decoder's ``MSGPACK_DEFINE(length, data)`` reads a uint32 — a minimal uint16 misaligns
    its parse and the extension silently sees an empty catalog.
    """
    # array(2) header + forced-uint32 length + str-framed zstd frame.
    return (
        b"\x92"
        + b"\xce"
        + struct.pack(">I", len(uncompressed))
        + _emit_str(_zstd(uncompressed))
    )


def serialize_schema_contents(flight_info_protos: list[bytes]) -> tuple[bytes, str]:
    """Per-schema ``contents.serialized`` + its sha256 hex.

    Mirrors airport-go serializeSchemaContents: msgpack array of FlightInfo
    protobuf bytes (each a msgpack **bin**) → zstd → ``[len, str]`` envelope.
    Returns (serialized_envelope_bytes, sha256_hex_of_that_envelope).
    """
    inner = _encode([bytes(b) for b in flight_info_protos])  # array of bin
    serialized = _compressed_content(inner)
    return serialized, hashlib.sha256(serialized).hexdigest()


def build_list_schemas_response(
    schemas: list[dict[str, Any]], catalog_version: int, is_fixed: bool
) -> bytes:
    """DoAction ``list_schemas`` result body.

    ``schemas`` items: ``{"name", "description", "serialized" (bytes envelope),
    "sha256" (hex), "is_default" (bool)}``. Outer = ``[len, str zstd(msgpack(root))]``.
    """
    schema_objs: list[dict[str, Any]] = []
    for s in schemas:
        schema_objs.append(
            {
                "name": s["name"],
                "description": s.get("description", ""),
                "tags": {},
                "contents": {
                    "sha256": s["sha256"],
                    "url": None,
                    "serialized": _Str(s["serialized"]),
                },
                "is_default": s["is_default"],
            }
        )
    root = {
        "contents": {"sha256": "0" * 64, "url": None, "serialized": None},
        "schemas": schema_objs,
        "version_info": {"catalog_version": catalog_version, "is_fixed": is_fixed},
    }
    return _compressed_content(_encode(root))


def build_catalog_version_response(catalog_version: int, is_fixed: bool) -> bytes:
    """DoAction ``catalog_version`` result body (plain msgpack map)."""
    return _encode({"catalog_version": catalog_version, "is_fixed": is_fixed})


def build_endpoints_response(endpoint_protos: list[bytes]) -> bytes:
    """DoAction ``endpoints`` result body: msgpack array of str(FlightEndpoint proto)."""
    return _encode([_Str(bytes(p)) for p in endpoint_protos])


def decode_action_request(body: bytes) -> dict[str, Any]:
    """Decode a msgpack DoAction request body with map keys as ``str``.

    Unpacked ``raw=True`` so binary values (descriptor protobuf, json filters)
    stay ``bytes``; only the map keys — always ascii field names — are decoded.
    """
    raw = msgpack.unpackb(body, raw=True, strict_map_key=False)
    return _asciify_keys(raw)


def _asciify_keys(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {
            (k.decode("ascii") if isinstance(k, bytes) else k): _asciify_keys(v)
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_asciify_keys(v) for v in obj]
    return obj
