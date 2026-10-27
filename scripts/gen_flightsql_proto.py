# Copyright (c) 2026 Kenneth Stott
# Canary: 1718b32a-b7a4-4421-8174-cc7436db001c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generate the Python bindings of Arrow's FlightSql.proto and Flight.proto (REQ-2175).

provisa/proto/FlightSql.proto and Flight.proto are Apache Arrow's format/FlightSql.proto and
format/Flight.proto (Apache-2.0) at the pyarrow release Provisa pins; the ``_pb2.py`` beside each
is generated from it with grpcio-tools and checked in. Flight.proto's are for its session option
messages, which pyarrow's Python API does not expose. tests/unit/test_flightsql_proto_generated.py fails when the checked-in
bindings are not what this generates.

    PYTHONPATH=. .venv/bin/python3 scripts/gen_flightsql_proto.py
"""

from __future__ import annotations

import tempfile
from importlib import resources
from pathlib import Path

from grpc_tools import protoc

PROTO_DIR = Path(__file__).resolve().parents[1] / "provisa/proto"
#: Each proto, as its bindings and the type stub protoc writes beside them, for the type checker
#: (the modules build their classes at run time, so their names are not otherwise visible).
PROTOS = ("FlightSql.proto", "Flight.proto")


# The file header the repository's canary stamp completes (scripts/canary_stamp.py adds its
# ``# Canary:`` line after the first); the check ignores that line.
def _header(proto: str) -> str:
    return (
        "# Copyright (c) 2026 Kenneth Stott\n#\n"
        f"# Generated from {proto} (Apache Arrow, Apache-2.0) by scripts/gen_flightsql_proto.py.\n\n"
    )


def render() -> dict[Path, str]:
    """Each proto's bindings and their stub, as grpcio-tools generates them, under the file
    header."""
    include = str(resources.files("grpc_tools") / "_proto")
    out: dict[Path, str] = {}
    for proto in PROTOS:
        stem = proto.removesuffix(".proto")
        with tempfile.TemporaryDirectory() as tmp:
            code = protoc.main(
                [
                    "grpc_tools.protoc",
                    f"-I{PROTO_DIR}",
                    f"-I{include}",
                    f"--python_out={tmp}",
                    f"--pyi_out={tmp}",
                    proto,
                ]
            )
            if code != 0:
                raise RuntimeError(f"protoc failed on {PROTO_DIR / proto} with exit code {code}")
            for name in (f"{stem}_pb2.py", f"{stem}_pb2.pyi"):
                text = (Path(tmp) / name).read_text(encoding="utf-8")
                out[PROTO_DIR / name] = _header(proto) + text
    return out


if __name__ == "__main__":
    for path, text in render().items():
        path.write_text(text, encoding="utf-8")
        print(f"wrote {path}")
