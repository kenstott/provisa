# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's pgwire connect, SCRAM sign-in and one simple statement, timed against PostgreSQL's
on the same runner.

A driver's pool and timing tests count a connection and its first statement as part of what
they time: psycopg's test_queue[NullConnectionPool] runs six workers on two connections, each
``select pg_sleep(0.2)``, and wants the first done within 0.240 s, connect included. Provisa
spent ~20 ms there per connection on a normal runner and ~40 ms on a slow one, where PostgreSQL
spends ~5 ms, and the test missed its window.

A runner's speed varies by tens of percent from run to run, so a wall-clock budget either flakes
or is too loose to mean anything. The budget is relative instead: each round connects once to
each server, signs in with SCRAM over TCP, runs ``select 1`` and closes, the two interleaved so a
burst of load lands on both, and the median of Provisa's rounds may be at most ``--ratio`` times
PostgreSQL's.

    connect_latency.py --provisa <dsn> --postgres <dsn> [--rounds N] [--ratio R] [--summary FILE]

Exit status 0 when within the budget, 1 when over it.
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time

import psycopg

WARMUP = 5


def round_trip(dsn: str) -> float:
    """Seconds to connect, sign in, run ``select 1``, read its row and close."""
    start = time.perf_counter()
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("select 1").fetchall()
    return time.perf_counter() - start


def measure(provisa: str, postgres: str, rounds: int) -> tuple[list[float], list[float]]:
    for _ in range(WARMUP):
        round_trip(provisa)
        round_trip(postgres)
    ours: list[float] = []
    theirs: list[float] = []
    for _ in range(rounds):
        ours.append(round_trip(provisa))
        theirs.append(round_trip(postgres))
    return ours, theirs


def verdict(ours: list[float], theirs: list[float], ratio: float) -> tuple[bool, str]:
    """Whether Provisa's median round is within ``ratio`` times PostgreSQL's, and the line that
    says so."""
    mine, reference = statistics.median(ours), statistics.median(theirs)
    observed = mine / reference
    within = observed <= ratio
    line = (
        f"pgwire connect + SCRAM + select 1: Provisa {mine * 1000:.1f} ms, PostgreSQL "
        f"{reference * 1000:.1f} ms (median of {len(ours)}): {observed:.2f}x, budget {ratio:.2f}x"
        f" -- {'within' if within else 'OVER'}"
    )
    return within, line


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="pgwire connect + SCRAM + select 1, Provisa against PostgreSQL"
    )
    parser.add_argument("--provisa", required=True)
    parser.add_argument("--postgres", required=True)
    parser.add_argument("--rounds", type=int, default=60)
    parser.add_argument("--ratio", type=float, required=True)
    parser.add_argument("--summary", help="a file the verdict line is appended to")
    args = parser.parse_args(argv[1:])
    ours, theirs = measure(args.provisa, args.postgres, args.rounds)
    within, line = verdict(ours, theirs, args.ratio)
    print(line)
    if args.summary:
        with open(args.summary, "a") as out:
            out.write(line + "\n")
    return 0 if within else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
