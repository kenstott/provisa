# Copyright (c) 2026 Kenneth Stott
# Canary: 1718b32a-b7a4-4421-8174-cc7436db001c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generate the Python bindings of Arrow's FlightSql.proto (REQ-2175).

provisa/proto/FlightSql.proto is Apache Arrow's format/FlightSql.proto (Apache-2.0)
at the pyarrow release Provisa pins; FlightSql_pb2.py beside it is generated from it with
grpcio-tools and checked in. tests/unit/test_flightsql_proto_generated.py fails when the checked-in
bindings are not what this generates.

    PYTHONPATH=. .venv/bin/python3 scripts/gen_flightsql_proto.py
"""

from __future__ import annotations

import tempfile
from importlib import resources
from pathlib import Path

from grpc_tools import protoc

PROTO_DIR = Path(__file__).resolve().parents[1] / "provisa/proto"
PROTO = PROTO_DIR / "FlightSql.proto"
OUT = PROTO_DIR / "FlightSql_pb2.py"
# The type stub protoc writes beside it, for the type checker (the module builds its classes at run
# time, so their names are not otherwise visible).
STUB = PROTO_DIR / "FlightSql_pb2.pyi"


# The file header the repository's canary stamp completes (scripts/canary_stamp.py adds its
# ``# Canary:`` line after the first); the check ignores that line.
HEADER = """\
# Copyright (c) 2026 Kenneth Stott
#
# Generated from FlightSql.proto (Apache Arrow, Apache-2.0) by scripts/gen_flightsql_proto.py.

"""


def render() -> dict[Path, str]:
    """The bindings and their stub, as grpcio-tools generates them from FlightSql.proto, under
    the file header."""
    include = str(resources.files("grpc_tools") / "_proto")
    with tempfile.TemporaryDirectory() as out:
        code = protoc.main(
            [
                "grpc_tools.protoc",
                f"-I{PROTO_DIR}",
                f"-I{include}",
                f"--python_out={out}",
                f"--pyi_out={out}",
                PROTO.name,
            ]
        )
        if code != 0:
            raise RuntimeError(f"protoc failed on {PROTO} with exit code {code}")
        return {
            path: HEADER + (Path(out) / path.name).read_text(encoding="utf-8")
            for path in (OUT, STUB)
        }


if __name__ == "__main__":
    for path, text in render().items():
        path.write_text(text, encoding="utf-8")
        print(f"wrote {path}")
