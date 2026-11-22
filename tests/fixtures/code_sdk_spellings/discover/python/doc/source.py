# Copyright (c) 2026 Kenneth Stott
# Canary: 07968dc9-9508-4ec6-8ee9-0f0f48802d27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass

from provisa_services import Discovered, DiscoveredQuery, DiscoveredTable


@dataclass
class Row:
    id: int
    name: str


@dataclass
class ByName:
    name: str


def discover() -> Discovered:
    """@discover"""
    return Discovered(
        tables=[DiscoveredTable("rows", row=Row)],
        queries=[DiscoveredQuery("rows_by_name", table="rows", parameters=ByName)],
        table_handler=serve_table,
        query_handler=serve_query,
    )


def serve_table(name, inputs, context):
    return []


def serve_query(name, arguments):
    return []
