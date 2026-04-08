# Copyright (c) 2026 Kenneth Stott
# Canary: a769886d-ad5d-42e6-8fbe-eb7ceed0349e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Load and parse .proto files for the gRPC Remote Schema Connector (Phase AR).

Two distinct operations:
  parse_proto_text(text)     — pure text parsing, no I/O, no external deps
  load_proto(path_or_url)    — fetch file or URL, return parsed dict
  compile_proto_stubs(...)   — compile proto → Python stubs via grpc_tools.protoc
"""
from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path


# ---------------------------------------------------------------------------
# Text-based proto parser (pure, no external deps)
# ---------------------------------------------------------------------------

def _strip_comments(text: str) -> str:
    text = re.sub(r"//[^\n]*", "", text)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    return text


def _extract_blocks(text: str, keyword: str) -> list[tuple[str, str]]:
    """Extract named top-level blocks (message/service/enum), handling nested braces."""
    results = []
    pattern = re.compile(rf"\b{keyword}\s+(\w+)\s*\{{")
    for m in pattern.finditer(text):
        name = m.group(1)
        start = m.end()
        depth = 1
        i = start
        while i < len(text) and depth > 0:
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
            i += 1
        body = text[start : i - 1]
        results.append((name, body))
    return results


def _parse_message_fields(body: str) -> list[dict]:
    """Parse field declarations from a message body."""
    fields = []
    pattern = re.compile(r"(repeated\s+)?(\w[\w.]*)\s+(\w+)\s*=\s*\d+")
    skip = {"option", "reserved", "extensions", "oneof", "map"}
    for m in pattern.finditer(body):
        field_type = m.group(2)
        if field_type in skip:
            continue
        fields.append(
            {
                "name": m.group(3),
                "type": field_type,
                "repeated": bool(m.group(1)),
            }
        )
    return fields


def _parse_service_methods(body: str) -> list[dict]:
    """Parse rpc declarations from a service body."""
    methods = []
    pattern = re.compile(
        r"rpc\s+(\w+)\s*\(\s*(stream\s+)?(\w+)\s*\)\s+returns\s*\(\s*(stream\s+)?(\w+)\s*\)"
    )
    for m in pattern.finditer(body):
        methods.append(
            {
                "name": m.group(1),
                "client_streaming": bool(m.group(2)),
                "input_type": m.group(3),
                "server_streaming": bool(m.group(4)),
                "output_type": m.group(5),
            }
        )
    return methods


def _extract_package(text: str) -> str:
    m = re.search(r"\bpackage\s+([\w.]+)\s*;", text)
    return m.group(1) if m else ""


def parse_proto_text(proto_text: str) -> dict:
    """Parse proto3 text into an intermediate schema dict.

    Returns:
        {
            "package": str,
            "services": [{"name": str, "methods": [...]}],
            "messages": {name: [{"name", "type", "repeated"}]},
            "enums": [str],
        }
    """
    text = _strip_comments(proto_text)
    package = _extract_package(text)

    messages: dict[str, list[dict]] = {}
    for name, body in _extract_blocks(text, "message"):
        messages[name] = _parse_message_fields(body)

    enums: list[str] = [name for name, _ in _extract_blocks(text, "enum")]

    services: list[dict] = []
    for name, body in _extract_blocks(text, "service"):
        services.append({"name": name, "methods": _parse_service_methods(body)})

    return {
        "package": package,
        "services": services,
        "messages": messages,
        "enums": enums,
    }


# ---------------------------------------------------------------------------
# File / URL loader
# ---------------------------------------------------------------------------

async def load_proto(
    path_or_url: str,
    import_paths: list[str] | None = None,
) -> dict:
    """Load and parse a .proto file from a local path or http/https URL.

    Returns the same intermediate dict as parse_proto_text().

    Raises:
        FileNotFoundError — missing local path
        httpx.HTTPError   — HTTP fetch failure
        ValueError        — proto parse error
    """
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        import httpx
        r = httpx.get(path_or_url, timeout=30, follow_redirects=True)
        r.raise_for_status()
        text = r.text
    else:
        p = Path(path_or_url)
        if not p.exists():
            raise FileNotFoundError(f"Proto file not found: {path_or_url!r}")
        text = p.read_text()

    return parse_proto_text(text)


# ---------------------------------------------------------------------------
# Stub compiler (requires grpcio-tools)
# ---------------------------------------------------------------------------

def compile_proto_stubs(
    proto_text: str,
    proto_name: str = "remote",
    import_paths: list[str] | None = None,
    out_dir: str | None = None,
) -> tuple[str, str]:
    """Compile proto text to Python stubs using grpc_tools.protoc.

    Returns:
        (pb2_path, pb2_grpc_path) — absolute paths to the generated modules.

    Raises:
        ValueError — if protoc compilation fails.
    """
    from grpc_tools import protoc  # type: ignore[import]
    import pkg_resources

    tmp = out_dir or tempfile.mkdtemp(prefix="grpc_remote_stubs_")
    proto_file = os.path.join(tmp, f"{proto_name}.proto")
    Path(proto_file).write_text(proto_text)

    try:
        well_known = pkg_resources.resource_filename("grpc_tools", "_proto")
    except Exception:
        well_known = ""

    include_flags: list[str] = [f"-I{tmp}"]
    if well_known:
        include_flags.append(f"-I{well_known}")
    for ip in (import_paths or []):
        include_flags.append(f"-I{ip}")

    args = [
        "grpc_tools.protoc",
        *include_flags,
        f"--python_out={tmp}",
        f"--grpc_python_out={tmp}",
        proto_file,
    ]
    ret = protoc.main(args)
    if ret != 0:
        raise ValueError(f"protoc compilation failed (exit {ret}) for {proto_name!r}")

    pb2_path = os.path.join(tmp, f"{proto_name}_pb2.py")
    pb2_grpc_path = os.path.join(tmp, f"{proto_name}_pb2_grpc.py")
    if not os.path.exists(pb2_path):
        raise ValueError(f"protoc did not generate {pb2_path}")

    return pb2_path, pb2_grpc_path
