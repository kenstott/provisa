# Copyright (c) 2025 Kenneth Stott
# Canary: 81719129-a916-49ab-a373-0bbc355c3f0d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Compile .proto content to Python stubs using grpc_tools.protoc."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from grpc_tools import protoc


def compile_proto(proto_content: str, output_dir: str) -> tuple[str, str]:
    """Compile a .proto string to Python gRPC stubs.

    Args:
        proto_content: The .proto file content string.
        output_dir: Directory to write generated _pb2.py and _pb2_grpc.py.

    Returns:
        Tuple of (pb2_path, pb2_grpc_path).
    """
    os.makedirs(output_dir, exist_ok=True)

    # Write proto to a temp dir so protoc can find it
    with tempfile.TemporaryDirectory() as tmpdir:
        proto_path = Path(tmpdir) / "provisa_service.proto"
        proto_path.write_text(proto_content)

        # Locate google protobuf includes
        import grpc_tools
        well_known = str(Path(grpc_tools.__file__).parent / "_proto")

        result = protoc.main([
            "grpc_tools.protoc",
            f"--proto_path={tmpdir}",
            f"--proto_path={well_known}",
            f"--python_out={output_dir}",
            f"--grpc_python_out={output_dir}",
            "provisa_service.proto",
        ])

        if result != 0:
            raise RuntimeError(f"protoc compilation failed with code {result}")

    pb2_path = str(Path(output_dir) / "provisa_service_pb2.py")
    pb2_grpc_path = str(Path(output_dir) / "provisa_service_pb2_grpc.py")

    if not Path(pb2_path).exists():
        raise RuntimeError(f"Expected output not found: {pb2_path}")
    if not Path(pb2_grpc_path).exists():
        raise RuntimeError(f"Expected output not found: {pb2_grpc_path}")

    return pb2_path, pb2_grpc_path
