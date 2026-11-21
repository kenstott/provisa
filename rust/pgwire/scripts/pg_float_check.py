# Copyright (c) 2026 Kenneth Stott
# Canary: 5b7e2c90-41d3-4f6a-8a1e-c3d92f0b6a47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The encoder's float8 and float4 text against a live PostgreSQL's float8out and float4out, byte for
byte, over random bit patterns and edge values. The server is a throwaway cluster in a scratch
directory, on a Unix socket only, stopped at the end. PostgreSQL 18's src/common/d2s.c, f2s.c,
ryu_common.h and float8out_internal/float4out differ from 17's in their copyright lines only.

    PYTHONPATH=.:vendor/buenavista \\
        .venv/bin/python rust/pgwire/scripts/pg_float_check.py <pg bin dir> <scratch dir>
"""

from __future__ import annotations

import random
import struct
import subprocess
import sys
from pathlib import Path

import pyarrow as pa

from provisa._encoders import pgwire as pgwire_encoder

N = 300_000
EDGES = [0.0, -0.0, float("nan"), float("inf"), float("-inf"), 1.0, 0.1, 1e15, 1e14, 1e-4, 1e-5]
EDGES += [5e-324, 2.2250738585072014e-308, 1.7976931348623157e308, -738171652909232.25, 1e100]


def _cells(b: bytes) -> list[bytes]:
    out, at = [], 0
    while at < len(b):
        (n,) = struct.unpack_from("!i", b, at + 7)
        out.append(b[at + 11 : at + 11 + n])
        at += 11 + n
    return out


def _postgres_text(psql: list[str], sql_type: str, values: list[float]) -> list[bytes]:
    # repr reads back as the same double; a float4 from a float's widened repr is that float.
    data = "".join(f"{v!r}\n".replace("inf", "Infinity").replace("nan", "NaN") for v in values)
    script = (
        f"CREATE TEMP TABLE t (k serial, x {sql_type});\n"
        "COPY t (x) FROM STDIN;\n" + data + "\\.\n"
        "COPY (SELECT x::text FROM t ORDER BY k) TO STDOUT;\n"
    )
    done = subprocess.run(psql, input=script.encode(), capture_output=True, check=True)
    return done.stdout.splitlines()


def main() -> None:
    bindir, scratch = Path(sys.argv[1]), Path(sys.argv[2])
    data = scratch / "pgdata"
    subprocess.run(
        [bindir / "initdb", "-D", data, "-A", "trust", "-U", "postgres", "--no-sync"],
        check=True,
        capture_output=True,
    )
    opts = f"-k {scratch} -p 54917 -c listen_addresses=''"
    subprocess.run([bindir / "pg_ctl", "-D", data, "-o", opts, "-w", "start"], check=True)
    try:
        psql = [str(bindir / "psql"), "-h", str(scratch), "-p", "54917", "-U", "postgres"]
        psql += ["-X", "-q", "-v", "ON_ERROR_STOP=1", "postgres"]
        version = subprocess.run(psql + ["-At", "-c", "SHOW server_version"], capture_output=True)
        print("server", version.stdout.decode().strip())
        rng = random.Random(17)
        doubles = EDGES + [
            struct.unpack("!d", rng.getrandbits(64).to_bytes(8, "big"))[0] for _ in range(N)
        ]
        floats = [
            struct.unpack("!f", struct.pack("!I", rng.getrandbits(32)))[0] for _ in range(N)
        ] + [struct.unpack("!f", struct.pack("!f", v))[0] for v in EDGES if abs(v) < 3e38]
        for sql_type, bvtype, arrow_type, values in (
            ("float8", "FLOAT", pa.float64(), doubles),
            ("float4", "REAL", pa.float32(), floats),
        ):
            pg = _postgres_text(psql, sql_type, values)
            batch = pa.RecordBatch.from_arrays([pa.array(values, arrow_type)], names=["x"])
            rs = _cells(pgwire_encoder.encode_batch(batch, [bvtype], [0]))
            differ = [(v, a, b) for v, a, b in zip(values, pg, rs, strict=True) if a != b]
            print(f"{sql_type}: {len(values)} values, {len(differ)} differ", differ[:5])
            assert not differ
    finally:
        subprocess.run([bindir / "pg_ctl", "-D", data, "-m", "fast", "-w", "stop"], check=True)


if __name__ == "__main__":
    main()
