# Copyright (c) 2026 Kenneth Stott
# Canary: fa1dde1d-884a-43c5-bf3e-ef5211a30324
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
"""A downstream team's consumer: told on Kafka that a dataset is ready, it pulls the dataset over
Arrow Flight as of the version time the notice names, and records the pull.

Each notice on the topic `dataset-ready` names a dataset, the consumer it is for and its version
time. For each notice addressed to one of CONSUMERS, the consumer pulls the whole dataset as that
consumer's role -- what its row rules and masks let it see -- as of the version time, so a refresh
landing while it reads cannot mix two versions. It counts the rows as the batches stream in, and
writes one row to the warehouse's `pulls` table: when it was told, when it had the rows, how many
and how fast, or why it was refused.

Any Arrow Flight client can do the same: a ticket is a JSON object naming the query, the role and
the as-of time.

``--notices N`` pulls for the first N notices addressed to CONSUMERS and exits, under a consumer
group of its own (the demo's setup runs it once, setup_pulls.py). Environment:

  FEEDS_BOOTSTRAP   the Kafka broker the notices are on
  PROVISA_FLIGHT    Provisa's Arrow Flight address, grpc://host:port
  CONSUMERS         the consumers this process pulls for, comma-separated (their roles)
  PULLS_DSN         the warehouse database the pulls are recorded in
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg
import pyarrow.flight as flight
from aiokafka import AIOKafkaConsumer

BOOTSTRAP = os.environ["FEEDS_BOOTSTRAP"]
FLIGHT = os.environ["PROVISA_FLIGHT"]
CONSUMERS = [c.strip() for c in os.environ["CONSUMERS"].split(",") if c.strip()]
PULLS_DSN = os.environ["PULLS_DSN"]
# Its name ends with the demo's suffix where the broker is shared (the sandbox tier's node).
TOPIC = "dataset-ready" + os.environ["TOPIC_SUFFIX"]


def pull(dataset: str, role: str, as_of: str) -> tuple[int, float]:
    """Pull the whole dataset as ``role``, as of ``as_of``: the rows it read and the seconds it
    took, from the request to the last batch."""
    ticket = {
        "query": f'SELECT * FROM "provisioning"."view_{dataset}"',
        "role": role,
        "as_of": as_of,
    }
    started = time.perf_counter()
    reader = flight.connect(FLIGHT).do_get(flight.Ticket(json.dumps(ticket).encode()))
    rows = sum(chunk.data.num_rows for chunk in reader)
    return rows, time.perf_counter() - started


def record(notice: dict, rows: int | None, seconds: float | None, refused: str | None) -> None:
    with psycopg.connect(PULLS_DSN, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO pulls (notice_id, consumer, dataset, version_time, notified_at, "
            "pulled_at, rows, seconds, rows_per_sec, refused) "
            "VALUES (%s, %s, %s, %s, %s, now(), %s, %s, %s, %s)",
            (
                notice["notice_id"],
                notice["consumer"],
                notice["dataset"],
                notice["version_time"],
                notice["notified_at"],
                rows,
                seconds,
                rows / seconds if rows is not None and seconds else None,
                refused,
            ),
        )


async def main(notices: int | None) -> None:
    group = f"provisioning-consumers-{'-'.join(CONSUMERS)}"
    if notices is not None:
        group = f"{group}-{uuid.uuid4().hex[:8]}"  # a run of its own: every notice, from the start
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=group,
        auto_offset_reset="earliest",
    )
    handled = 0
    await consumer.start()
    print(f"pulling for {', '.join(CONSUMERS)} from {FLIGHT} on each notice", flush=True)
    try:
        async for message in consumer:
            if message.value is None:
                continue  # a tombstone: the notices table deletes nothing, so none carries a notice
            notice = json.loads(message.value)
            if notice.get("consumer") not in CONSUMERS:
                continue
            notice["notified_at"] = datetime.now(timezone.utc)
            try:
                rows, seconds = await asyncio.to_thread(
                    pull, notice["dataset"], notice["consumer"], notice["version_time"]
                )
            except flight.FlightServerError as exc:
                # Refused by the server (outside the dataset's window, say): recorded. A server
                # that cannot be reached is not a refusal, and stops the consumer.
                record(notice, None, None, str(exc).splitlines()[0])
                print(f"refused: {notice['consumer']} {notice['dataset']}: {exc}", flush=True)
                handled += 1
                if notices is not None and handled >= notices:
                    return
                continue
            record(notice, rows, seconds, None)
            print(
                f"{notice['consumer']} pulled {rows} rows of {notice['dataset']} as of "
                f"{notice['version_time']} in {seconds:.2f}s ({rows / seconds:,.0f} rows/s)",
                flush=True,
            )
            handled += 1
            if notices is not None and handled >= notices:
                return
    finally:
        await consumer.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--notices", type=int, help="pull for this many notices, then exit")
    asyncio.run(main(parser.parse_args().notices))
