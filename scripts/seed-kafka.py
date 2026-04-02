#!/usr/bin/env python3
# Copyright (c) 2025 Kenneth Stott
# Canary: 21b63c0c-c9a3-49e4-91fc-e2d29c42355a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Seed Kafka with support ticket and resolution events.

Two message types on the same topic (support.tickets):
- TicketCreated: customer opened a support ticket
- TicketResolved: support resolved a ticket

Uses customer_id keys from the PG demo dataset (1-20).
"""

import json
import random
import time
from datetime import datetime, timedelta

from confluent_kafka import Producer

TOPIC = "support.tickets"
BOOTSTRAP = "localhost:9092"

CATEGORIES = ["billing", "technical", "shipping", "returns", "account"]
PRIORITIES = ["low", "medium", "high", "critical"]
RESOLUTIONS = ["fixed", "workaround", "duplicate", "wont_fix", "escalated"]

AGENTS = ["agent_alice", "agent_bob", "agent_carol", "agent_dave"]


def delivery_report(err, msg):
    if err:
        print(f"  ERROR: {err}")


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    now = datetime.utcnow()
    ticket_id = 1000

    print(f"Seeding {TOPIC} with ticket and resolution events...")

    for i in range(50):
        customer_id = random.randint(1, 20)
        ticket_id += 1
        created_at = now - timedelta(minutes=random.randint(1, 55))

        # TicketCreated event
        ticket_event = {
            "event_type": "TicketCreated",
            "ticket_id": ticket_id,
            "customer_id": customer_id,
            "category": random.choice(CATEGORIES),
            "priority": random.choice(PRIORITIES),
            "subject": f"Issue with order #{random.randint(100, 999)}",
            "created_at": created_at.isoformat() + "Z",
        }
        producer.produce(
            TOPIC,
            key=str(customer_id).encode(),
            value=json.dumps(ticket_event).encode(),
            callback=delivery_report,
        )

        # ~70% of tickets get resolved
        if random.random() < 0.7:
            resolved_at = created_at + timedelta(minutes=random.randint(5, 120))
            resolution_event = {
                "event_type": "TicketResolved",
                "ticket_id": ticket_id,
                "customer_id": customer_id,
                "agent": random.choice(AGENTS),
                "resolution": random.choice(RESOLUTIONS),
                "resolution_time_minutes": (resolved_at - created_at).seconds // 60,
                "resolved_at": resolved_at.isoformat() + "Z",
            }
            producer.produce(
                TOPIC,
                key=str(customer_id).encode(),
                value=json.dumps(resolution_event).encode(),
                callback=delivery_report,
            )

    producer.flush()
    print(f"Done. Seeded ~85 events (50 tickets + ~35 resolutions).")


if __name__ == "__main__":
    main()
