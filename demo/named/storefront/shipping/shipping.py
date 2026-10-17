# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The storefront's internal shipping service: each order's shipment, over REST, described by its
OpenAPI document (/openapi.json). Provisa reads it live as a source. Every shipment is a function
of its order's id, with the order dates db/init.sql gives them, so it holds the same rows at every
start."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ORDERS = 3000
FIRST_PLACED = datetime(2026, 9, 30, 12, 0)
CARRIERS = ("Parcelo", "SwiftShip", "NordPost", "CargoRun")
# Of every 20 shipments, how many each carrier delivers after the date it promised.
LATE_PER_20 = (1, 5, 2, 8)

SPEC = {
    "openapi": "3.0.3",
    "info": {"title": "Storefront shipping", "version": "1.0.0"},
    "servers": [{"url": "/"}],
    "paths": {
        "/shipments": {
            "get": {
                "operationId": "listShipments",
                "summary": "Every order's shipment",
                "responses": {
                    "200": {
                        "description": "The shipments",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/Shipment"},
                                }
                            }
                        },
                    }
                },
            }
        }
    },
    "components": {
        "schemas": {
            "Shipment": {
                "type": "object",
                "properties": {
                    "order_id": {"type": "integer"},
                    "carrier": {"type": "string"},
                    "tracking": {"type": "string"},
                    "status": {"type": "string"},
                    "shipped_at": {"type": "string", "format": "date-time"},
                    "promised_at": {"type": "string", "format": "date-time"},
                    "delivered_at": {"type": "string", "format": "date-time", "nullable": True},
                },
            }
        }
    },
}


def shipment(order_id: int) -> dict | None:
    """The order's shipment; None for an order not shipped (processing, or cancelled)."""
    if order_id < 60 or order_id % 41 == 0:
        return None
    placed = FIRST_PLACED - timedelta(minutes=40 * order_id)
    carrier = order_id % len(CARRIERS)
    shipped = placed + timedelta(hours=6 + order_id % 18)
    promised = placed + timedelta(days=2 + order_id % 3)
    late = ((order_id // len(CARRIERS)) * 7) % 20 < LATE_PER_20[carrier]
    delivered = (
        promised + timedelta(days=1 + order_id % 3)
        if late
        else promised - timedelta(hours=order_id % 30)
    )
    return {
        "order_id": order_id,
        "carrier": CARRIERS[carrier],
        "tracking": f"{CARRIERS[carrier][:2].upper()}{order_id * 7919:09d}",
        "status": "delivered",
        "shipped_at": shipped.isoformat(),
        "promised_at": promised.isoformat(),
        "delivered_at": delivered.isoformat(),
    }


SHIPMENTS = [s for s in (shipment(n) for n in range(1, ORDERS + 1)) if s is not None]


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 -- BaseHTTPRequestHandler's name
        path = self.path.split("?", 1)[0]
        if path == "/openapi.json":
            self._send(200, SPEC)
        elif path == "/shipments":
            self._send(200, SHIPMENTS)
        elif path == "/health":
            self._send(200, {"ok": True})
        else:
            self._send(404, {"error": f"no such path {path}"})

    def _send(self, status: int, body: object) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 -- the base's name
        return  # one line a request would bury the store's own log


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
