# Copyright (c) 2026 Kenneth Stott
# Canary: 4d8a2f6c-3e1b-4b9a-9c7d-0a5e6f2b8c31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Test-side gRPC client schema loading WITHOUT server reflection.

gRPC reflection is optional (REQ-1904: off unless auth is active or explicitly opted in), so
clients must not depend on it. A role's ``.proto`` is published at ``GET /data/proto/{role}``
(REQ-525); this compiles it with ``grpc_tools.protoc`` into a descriptor pool the test builds
message classes from."""

# Requirements: REQ-525, REQ-1904

from __future__ import annotations

import tempfile
from importlib import resources
from pathlib import Path

import httpx
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor import ServiceDescriptor
from google.protobuf.descriptor_pool import DescriptorPool


def compile_proto(proto_text: str) -> DescriptorPool:
    """Compile ``.proto`` source into a fresh descriptor pool (with its well-known imports)."""
    from grpc_tools import protoc

    well_known = str(resources.files("grpc_tools") / "_proto")
    with tempfile.TemporaryDirectory() as d:
        src = Path(d) / "provisa.proto"
        src.write_text(proto_text)
        out = Path(d) / "provisa.pb"
        rc = protoc.main(
            [
                "protoc",
                f"-I{d}",
                f"-I{well_known}",
                "--include_imports",
                f"--descriptor_set_out={out}",
                str(src),
            ]
        )
        if rc != 0:
            raise RuntimeError(f"protoc failed ({rc}) compiling the served .proto")
        fds = descriptor_pb2.FileDescriptorSet.FromString(out.read_bytes())
    pool = DescriptorPool()
    for fdp in fds.file:
        pool.Add(fdp)
    return pool


def role_descriptor_pool(base_url: str, role: str) -> tuple[DescriptorPool, ServiceDescriptor]:
    """Fetch ``role``'s published ``.proto`` and return its pool and data service descriptor."""
    resp = httpx.get(f"{base_url}/data/proto/{role}", headers={"X-Provisa-Role": role}, timeout=30)
    resp.raise_for_status()
    pool = compile_proto(resp.text)
    fdp = descriptor_pb2.FileDescriptorProto()
    pool.FindFileByName("provisa.proto").CopyToProto(fdp)
    service_name = next(s.name for s in fdp.service if s.name.endswith("Service"))
    return pool, pool.FindServiceByName(f"{fdp.package}.{service_name}")
