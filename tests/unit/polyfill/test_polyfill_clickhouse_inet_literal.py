# Copyright (c) 2026 Kenneth Stott
# Canary: 4c9e2a71-8d3b-4e56-b0f7-1a6d5c8e3b92
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's rows of the inet and cidr functions of literal addresses (REQ-2236; REQ-2171):
abbrev, broadcast, cidr, family, host, hostmask, masklen, netmask, network, set_masklen and the
network_* comparisons, each refused by name (or refused for an IPv6 address) on ClickHouse before:
the functions measure's own calls and the edges the rows read, answered on the embedded engine
(chdb) against the embedded PostgreSQL's recorded answer."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import pytest

from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile

ANSWERED = [
    ('SELECT pg_catalog."abbrev"(NULL::cidr) AS v', [None]),
    ("SELECT pg_catalog.\"abbrev\"('192.168.0.0/24'::cidr) AS v", ["192.168.0/24"]),
    ("SELECT pg_catalog.\"abbrev\"('10.0.0.0/8'::cidr) AS v", ["10/8"]),
    ("SELECT pg_catalog.\"abbrev\"('::1/128'::cidr) AS v", ["::1/128"]),
    ("SELECT pg_catalog.\"abbrev\"('0.0.0.0/0'::cidr) AS v LIMIT 1001", ["0/0"]),
    ("SELECT pg_catalog.\"abbrev\"('::/0'::cidr) AS v LIMIT 1001", ["::/0"]),
    ('SELECT pg_catalog."abbrev"(NULL::inet) AS v', [None]),
    ("SELECT pg_catalog.\"abbrev\"('192.168.0.1/24'::inet) AS v", ["192.168.0.1/24"]),
    ("SELECT pg_catalog.\"abbrev\"('10.1.2.3'::inet) AS v", ["10.1.2.3"]),
    ("SELECT pg_catalog.\"abbrev\"('::1'::inet) AS v", ["::1"]),
    ("SELECT pg_catalog.\"abbrev\"('0.0.0.0/0'::inet) AS v LIMIT 1001", ["0.0.0.0/0"]),
    ("SELECT pg_catalog.\"abbrev\"('255.255.255.255'::inet) AS v LIMIT 1001", ["255.255.255.255"]),
    ("SELECT pg_catalog.\"abbrev\"('::/0'::inet) AS v LIMIT 1001", ["::/0"]),
    (
        "SELECT pg_catalog.\"abbrev\"('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'::inet) AS v LIMIT 1001",
        ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],
    ),
    ('SELECT pg_catalog."broadcast"(NULL::inet) AS v', [None]),
    ("SELECT pg_catalog.\"broadcast\"('192.168.0.1/24'::inet) AS v", ["192.168.0.255/24"]),
    ("SELECT pg_catalog.\"broadcast\"('10.1.2.3'::inet) AS v", ["10.1.2.3"]),
    ("SELECT pg_catalog.\"broadcast\"('::1'::inet) AS v", ["::1"]),
    ("SELECT pg_catalog.\"broadcast\"('0.0.0.0/0'::inet) AS v LIMIT 1001", ["255.255.255.255/0"]),
    (
        "SELECT pg_catalog.\"broadcast\"('255.255.255.255'::inet) AS v LIMIT 1001",
        ["255.255.255.255"],
    ),
    (
        "SELECT pg_catalog.\"broadcast\"('::/0'::inet) AS v LIMIT 1001",
        ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/0"],
    ),
    (
        "SELECT pg_catalog.\"broadcast\"('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'::inet) AS v LIMIT 1001",
        ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],
    ),
    ('SELECT pg_catalog."cidr"(NULL::inet) AS v', [None]),
    ("SELECT pg_catalog.\"cidr\"('192.168.0.1/24'::inet) AS v", ["192.168.0.0/24"]),
    ("SELECT pg_catalog.\"cidr\"('10.1.2.3'::inet) AS v", ["10.1.2.3/32"]),
    ("SELECT pg_catalog.\"cidr\"('::1'::inet) AS v", ["::1/128"]),
    ("SELECT pg_catalog.\"cidr\"('0.0.0.0/0'::inet) AS v LIMIT 1001", ["0.0.0.0/0"]),
    ("SELECT pg_catalog.\"cidr\"('255.255.255.255'::inet) AS v LIMIT 1001", ["255.255.255.255/32"]),
    ("SELECT pg_catalog.\"cidr\"('::/0'::inet) AS v LIMIT 1001", ["::/0"]),
    (
        "SELECT pg_catalog.\"cidr\"('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'::inet) AS v LIMIT 1001",
        ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128"],
    ),
    ('SELECT pg_catalog."family"(NULL::inet) AS v', [None]),
    ("SELECT pg_catalog.\"family\"('192.168.0.1/24'::inet) AS v", ["4"]),
    ("SELECT pg_catalog.\"family\"('10.1.2.3'::inet) AS v", ["4"]),
    ("SELECT pg_catalog.\"family\"('::1'::inet) AS v", ["6"]),
    ("SELECT pg_catalog.\"family\"('0.0.0.0/0'::inet) AS v LIMIT 1001", ["4"]),
    ("SELECT pg_catalog.\"family\"('255.255.255.255'::inet) AS v LIMIT 1001", ["4"]),
    ("SELECT pg_catalog.\"family\"('::/0'::inet) AS v LIMIT 1001", ["6"]),
    (
        "SELECT pg_catalog.\"family\"('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'::inet) AS v LIMIT 1001",
        ["6"],
    ),
    ('SELECT pg_catalog."host"(NULL::inet) AS v', [None]),
    ("SELECT pg_catalog.\"host\"('192.168.0.1/24'::inet) AS v", ["192.168.0.1"]),
    ("SELECT pg_catalog.\"host\"('10.1.2.3'::inet) AS v", ["10.1.2.3"]),
    ("SELECT pg_catalog.\"host\"('::1'::inet) AS v", ["::1"]),
    ("SELECT pg_catalog.\"host\"('0.0.0.0/0'::inet) AS v LIMIT 1001", ["0.0.0.0"]),
    ("SELECT pg_catalog.\"host\"('255.255.255.255'::inet) AS v LIMIT 1001", ["255.255.255.255"]),
    ("SELECT pg_catalog.\"host\"('::/0'::inet) AS v LIMIT 1001", ["::"]),
    (
        "SELECT pg_catalog.\"host\"('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'::inet) AS v LIMIT 1001",
        ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],
    ),
    ('SELECT pg_catalog."hostmask"(NULL::inet) AS v', [None]),
    ("SELECT pg_catalog.\"hostmask\"('192.168.0.1/24'::inet) AS v", ["0.0.0.255"]),
    ("SELECT pg_catalog.\"hostmask\"('10.1.2.3'::inet) AS v", ["0.0.0.0"]),
    ("SELECT pg_catalog.\"hostmask\"('::1'::inet) AS v", ["::"]),
    ("SELECT pg_catalog.\"hostmask\"('0.0.0.0/0'::inet) AS v LIMIT 1001", ["255.255.255.255"]),
    ("SELECT pg_catalog.\"hostmask\"('255.255.255.255'::inet) AS v LIMIT 1001", ["0.0.0.0"]),
    (
        "SELECT pg_catalog.\"hostmask\"('::/0'::inet) AS v LIMIT 1001",
        ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],
    ),
    (
        "SELECT pg_catalog.\"hostmask\"('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'::inet) AS v LIMIT 1001",
        ["::"],
    ),
    ('SELECT pg_catalog."masklen"(NULL::inet) AS v', [None]),
    ("SELECT pg_catalog.\"masklen\"('192.168.0.1/24'::inet) AS v", ["24"]),
    ("SELECT pg_catalog.\"masklen\"('10.1.2.3'::inet) AS v", ["32"]),
    ("SELECT pg_catalog.\"masklen\"('::1'::inet) AS v", ["128"]),
    ("SELECT pg_catalog.\"masklen\"('0.0.0.0/0'::inet) AS v LIMIT 1001", ["0"]),
    ("SELECT pg_catalog.\"masklen\"('255.255.255.255'::inet) AS v LIMIT 1001", ["32"]),
    ("SELECT pg_catalog.\"masklen\"('::/0'::inet) AS v LIMIT 1001", ["0"]),
    (
        "SELECT pg_catalog.\"masklen\"('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'::inet) AS v LIMIT 1001",
        ["128"],
    ),
    ('SELECT pg_catalog."netmask"(NULL::inet) AS v', [None]),
    ("SELECT pg_catalog.\"netmask\"('192.168.0.1/24'::inet) AS v", ["255.255.255.0"]),
    ("SELECT pg_catalog.\"netmask\"('10.1.2.3'::inet) AS v", ["255.255.255.255"]),
    (
        "SELECT pg_catalog.\"netmask\"('::1'::inet) AS v",
        ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],
    ),
    ("SELECT pg_catalog.\"netmask\"('0.0.0.0/0'::inet) AS v LIMIT 1001", ["0.0.0.0"]),
    ("SELECT pg_catalog.\"netmask\"('255.255.255.255'::inet) AS v LIMIT 1001", ["255.255.255.255"]),
    ("SELECT pg_catalog.\"netmask\"('::/0'::inet) AS v LIMIT 1001", ["::"]),
    (
        "SELECT pg_catalog.\"netmask\"('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'::inet) AS v LIMIT 1001",
        ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],
    ),
    ('SELECT pg_catalog."network"(NULL::inet) AS v', [None]),
    ("SELECT pg_catalog.\"network\"('192.168.0.1/24'::inet) AS v", ["192.168.0.0/24"]),
    ("SELECT pg_catalog.\"network\"('10.1.2.3'::inet) AS v", ["10.1.2.3/32"]),
    ("SELECT pg_catalog.\"network\"('::1'::inet) AS v", ["::1/128"]),
    ("SELECT pg_catalog.\"network\"('0.0.0.0/0'::inet) AS v LIMIT 1001", ["0.0.0.0/0"]),
    (
        "SELECT pg_catalog.\"network\"('255.255.255.255'::inet) AS v LIMIT 1001",
        ["255.255.255.255/32"],
    ),
    ("SELECT pg_catalog.\"network\"('::/0'::inet) AS v LIMIT 1001", ["::/0"]),
    (
        "SELECT pg_catalog.\"network\"('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'::inet) AS v LIMIT 1001",
        ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128"],
    ),
    ('SELECT pg_catalog."network_eq"(NULL::inet, NULL::inet) AS v', [None]),
    (
        "SELECT pg_catalog.\"network_eq\"('192.168.0.1/24'::inet, '192.168.0.1/24'::inet) AS v",
        ["t"],
    ),
    ("SELECT pg_catalog.\"network_eq\"('10.1.2.3'::inet, '10.1.2.3'::inet) AS v", ["t"]),
    ("SELECT pg_catalog.\"network_eq\"('::1'::inet, '::1'::inet) AS v", ["t"]),
    (
        "SELECT pg_catalog.\"network_eq\"(NULL::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_eq\"('192.168.0.1/24'::inet, NULL::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_eq\"('0.0.0.0/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_eq\"('192.168.0.1/24'::inet, '0.0.0.0/0'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_eq\"('255.255.255.255'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_eq\"('192.168.0.1/24'::inet, '255.255.255.255'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_eq\"('::/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_eq\"('192.168.0.1/24'::inet, '::/0'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    ('SELECT pg_catalog."network_ge"(NULL::inet, NULL::inet) AS v', [None]),
    (
        "SELECT pg_catalog.\"network_ge\"('192.168.0.1/24'::inet, '192.168.0.1/24'::inet) AS v",
        ["t"],
    ),
    ("SELECT pg_catalog.\"network_ge\"('10.1.2.3'::inet, '10.1.2.3'::inet) AS v", ["t"]),
    ("SELECT pg_catalog.\"network_ge\"('::1'::inet, '::1'::inet) AS v", ["t"]),
    (
        "SELECT pg_catalog.\"network_ge\"(NULL::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_ge\"('192.168.0.1/24'::inet, NULL::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_ge\"('0.0.0.0/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_ge\"('192.168.0.1/24'::inet, '0.0.0.0/0'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_ge\"('255.255.255.255'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_ge\"('192.168.0.1/24'::inet, '255.255.255.255'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_ge\"('::/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_ge\"('192.168.0.1/24'::inet, '::/0'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    ('SELECT pg_catalog."network_gt"(NULL::inet, NULL::inet) AS v', [None]),
    (
        "SELECT pg_catalog.\"network_gt\"('192.168.0.1/24'::inet, '192.168.0.1/24'::inet) AS v",
        ["f"],
    ),
    ("SELECT pg_catalog.\"network_gt\"('10.1.2.3'::inet, '10.1.2.3'::inet) AS v", ["f"]),
    ("SELECT pg_catalog.\"network_gt\"('::1'::inet, '::1'::inet) AS v", ["f"]),
    (
        "SELECT pg_catalog.\"network_gt\"(NULL::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_gt\"('192.168.0.1/24'::inet, NULL::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_gt\"('0.0.0.0/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_gt\"('192.168.0.1/24'::inet, '0.0.0.0/0'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_gt\"('255.255.255.255'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_gt\"('192.168.0.1/24'::inet, '255.255.255.255'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_gt\"('::/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_gt\"('192.168.0.1/24'::inet, '::/0'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    ('SELECT pg_catalog."network_larger"(NULL::inet, NULL::inet) AS v', [None]),
    (
        "SELECT pg_catalog.\"network_larger\"('192.168.0.1/24'::inet, '192.168.0.1/24'::inet) AS v",
        ["192.168.0.1/24"],
    ),
    ("SELECT pg_catalog.\"network_larger\"('10.1.2.3'::inet, '10.1.2.3'::inet) AS v", ["10.1.2.3"]),
    ("SELECT pg_catalog.\"network_larger\"('::1'::inet, '::1'::inet) AS v", ["::1"]),
    (
        "SELECT pg_catalog.\"network_larger\"(NULL::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_larger\"('192.168.0.1/24'::inet, NULL::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_larger\"('0.0.0.0/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["192.168.0.1/24"],
    ),
    (
        "SELECT pg_catalog.\"network_larger\"('192.168.0.1/24'::inet, '0.0.0.0/0'::inet) AS v LIMIT 1001",
        ["192.168.0.1/24"],
    ),
    (
        "SELECT pg_catalog.\"network_larger\"('255.255.255.255'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["255.255.255.255"],
    ),
    (
        "SELECT pg_catalog.\"network_larger\"('192.168.0.1/24'::inet, '255.255.255.255'::inet) AS v LIMIT 1001",
        ["255.255.255.255"],
    ),
    (
        "SELECT pg_catalog.\"network_larger\"('::/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["::/0"],
    ),
    (
        "SELECT pg_catalog.\"network_larger\"('192.168.0.1/24'::inet, '::/0'::inet) AS v LIMIT 1001",
        ["::/0"],
    ),
    ('SELECT pg_catalog."network_le"(NULL::inet, NULL::inet) AS v', [None]),
    (
        "SELECT pg_catalog.\"network_le\"('192.168.0.1/24'::inet, '192.168.0.1/24'::inet) AS v",
        ["t"],
    ),
    ("SELECT pg_catalog.\"network_le\"('10.1.2.3'::inet, '10.1.2.3'::inet) AS v", ["t"]),
    ("SELECT pg_catalog.\"network_le\"('::1'::inet, '::1'::inet) AS v", ["t"]),
    (
        "SELECT pg_catalog.\"network_le\"(NULL::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_le\"('192.168.0.1/24'::inet, NULL::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_le\"('0.0.0.0/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_le\"('192.168.0.1/24'::inet, '0.0.0.0/0'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_le\"('255.255.255.255'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_le\"('192.168.0.1/24'::inet, '255.255.255.255'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_le\"('::/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_le\"('192.168.0.1/24'::inet, '::/0'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    ('SELECT pg_catalog."network_lt"(NULL::inet, NULL::inet) AS v', [None]),
    (
        "SELECT pg_catalog.\"network_lt\"('192.168.0.1/24'::inet, '192.168.0.1/24'::inet) AS v",
        ["f"],
    ),
    ("SELECT pg_catalog.\"network_lt\"('10.1.2.3'::inet, '10.1.2.3'::inet) AS v", ["f"]),
    ("SELECT pg_catalog.\"network_lt\"('::1'::inet, '::1'::inet) AS v", ["f"]),
    (
        "SELECT pg_catalog.\"network_lt\"(NULL::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_lt\"('192.168.0.1/24'::inet, NULL::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_lt\"('0.0.0.0/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_lt\"('192.168.0.1/24'::inet, '0.0.0.0/0'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_lt\"('255.255.255.255'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_lt\"('192.168.0.1/24'::inet, '255.255.255.255'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_lt\"('::/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_lt\"('192.168.0.1/24'::inet, '::/0'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    ('SELECT pg_catalog."network_ne"(NULL::inet, NULL::inet) AS v', [None]),
    (
        "SELECT pg_catalog.\"network_ne\"('192.168.0.1/24'::inet, '192.168.0.1/24'::inet) AS v",
        ["f"],
    ),
    ("SELECT pg_catalog.\"network_ne\"('10.1.2.3'::inet, '10.1.2.3'::inet) AS v", ["f"]),
    ("SELECT pg_catalog.\"network_ne\"('::1'::inet, '::1'::inet) AS v", ["f"]),
    (
        "SELECT pg_catalog.\"network_ne\"(NULL::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_ne\"('192.168.0.1/24'::inet, NULL::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_ne\"('0.0.0.0/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_ne\"('192.168.0.1/24'::inet, '0.0.0.0/0'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_ne\"('255.255.255.255'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_ne\"('192.168.0.1/24'::inet, '255.255.255.255'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_ne\"('::/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_ne\"('192.168.0.1/24'::inet, '::/0'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    ('SELECT pg_catalog."network_overlap"(NULL::inet, NULL::inet) AS v', [None]),
    (
        "SELECT pg_catalog.\"network_overlap\"('192.168.0.1/24'::inet, '192.168.0.1/24'::inet) AS v",
        ["t"],
    ),
    ("SELECT pg_catalog.\"network_overlap\"('10.1.2.3'::inet, '10.1.2.3'::inet) AS v", ["t"]),
    ("SELECT pg_catalog.\"network_overlap\"('::1'::inet, '::1'::inet) AS v", ["t"]),
    (
        "SELECT pg_catalog.\"network_overlap\"(NULL::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_overlap\"('192.168.0.1/24'::inet, NULL::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_overlap\"('0.0.0.0/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_overlap\"('192.168.0.1/24'::inet, '0.0.0.0/0'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_overlap\"('255.255.255.255'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_overlap\"('192.168.0.1/24'::inet, '255.255.255.255'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_overlap\"('::/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_overlap\"('192.168.0.1/24'::inet, '::/0'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    ('SELECT pg_catalog."network_smaller"(NULL::inet, NULL::inet) AS v', [None]),
    (
        "SELECT pg_catalog.\"network_smaller\"('192.168.0.1/24'::inet, '192.168.0.1/24'::inet) AS v",
        ["192.168.0.1/24"],
    ),
    (
        "SELECT pg_catalog.\"network_smaller\"('10.1.2.3'::inet, '10.1.2.3'::inet) AS v",
        ["10.1.2.3"],
    ),
    ("SELECT pg_catalog.\"network_smaller\"('::1'::inet, '::1'::inet) AS v", ["::1"]),
    (
        "SELECT pg_catalog.\"network_smaller\"(NULL::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_smaller\"('192.168.0.1/24'::inet, NULL::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_smaller\"('0.0.0.0/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["0.0.0.0/0"],
    ),
    (
        "SELECT pg_catalog.\"network_smaller\"('192.168.0.1/24'::inet, '0.0.0.0/0'::inet) AS v LIMIT 1001",
        ["0.0.0.0/0"],
    ),
    (
        "SELECT pg_catalog.\"network_smaller\"('255.255.255.255'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["192.168.0.1/24"],
    ),
    (
        "SELECT pg_catalog.\"network_smaller\"('192.168.0.1/24'::inet, '255.255.255.255'::inet) AS v LIMIT 1001",
        ["192.168.0.1/24"],
    ),
    (
        "SELECT pg_catalog.\"network_smaller\"('::/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["192.168.0.1/24"],
    ),
    (
        "SELECT pg_catalog.\"network_smaller\"('192.168.0.1/24'::inet, '::/0'::inet) AS v LIMIT 1001",
        ["192.168.0.1/24"],
    ),
    ('SELECT pg_catalog."network_sub"(NULL::inet, NULL::inet) AS v', [None]),
    (
        "SELECT pg_catalog.\"network_sub\"('192.168.0.1/24'::inet, '192.168.0.1/24'::inet) AS v",
        ["f"],
    ),
    ("SELECT pg_catalog.\"network_sub\"('10.1.2.3'::inet, '10.1.2.3'::inet) AS v", ["f"]),
    ("SELECT pg_catalog.\"network_sub\"('::1'::inet, '::1'::inet) AS v", ["f"]),
    (
        "SELECT pg_catalog.\"network_sub\"(NULL::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_sub\"('192.168.0.1/24'::inet, NULL::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_sub\"('0.0.0.0/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_sub\"('192.168.0.1/24'::inet, '0.0.0.0/0'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_sub\"('255.255.255.255'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_sub\"('192.168.0.1/24'::inet, '255.255.255.255'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_sub\"('::/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_sub\"('192.168.0.1/24'::inet, '::/0'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    ('SELECT pg_catalog."network_subeq"(NULL::inet, NULL::inet) AS v', [None]),
    (
        "SELECT pg_catalog.\"network_subeq\"('192.168.0.1/24'::inet, '192.168.0.1/24'::inet) AS v",
        ["t"],
    ),
    ("SELECT pg_catalog.\"network_subeq\"('10.1.2.3'::inet, '10.1.2.3'::inet) AS v", ["t"]),
    ("SELECT pg_catalog.\"network_subeq\"('::1'::inet, '::1'::inet) AS v", ["t"]),
    (
        "SELECT pg_catalog.\"network_subeq\"(NULL::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_subeq\"('192.168.0.1/24'::inet, NULL::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_subeq\"('0.0.0.0/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_subeq\"('192.168.0.1/24'::inet, '0.0.0.0/0'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_subeq\"('255.255.255.255'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_subeq\"('192.168.0.1/24'::inet, '255.255.255.255'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_subeq\"('::/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_subeq\"('192.168.0.1/24'::inet, '::/0'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    ('SELECT pg_catalog."network_sup"(NULL::inet, NULL::inet) AS v', [None]),
    (
        "SELECT pg_catalog.\"network_sup\"('192.168.0.1/24'::inet, '192.168.0.1/24'::inet) AS v",
        ["f"],
    ),
    ("SELECT pg_catalog.\"network_sup\"('10.1.2.3'::inet, '10.1.2.3'::inet) AS v", ["f"]),
    ("SELECT pg_catalog.\"network_sup\"('::1'::inet, '::1'::inet) AS v", ["f"]),
    (
        "SELECT pg_catalog.\"network_sup\"(NULL::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_sup\"('192.168.0.1/24'::inet, NULL::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_sup\"('0.0.0.0/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_sup\"('192.168.0.1/24'::inet, '0.0.0.0/0'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_sup\"('255.255.255.255'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_sup\"('192.168.0.1/24'::inet, '255.255.255.255'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_sup\"('::/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_sup\"('192.168.0.1/24'::inet, '::/0'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    ('SELECT pg_catalog."network_supeq"(NULL::inet, NULL::inet) AS v', [None]),
    (
        "SELECT pg_catalog.\"network_supeq\"('192.168.0.1/24'::inet, '192.168.0.1/24'::inet) AS v",
        ["t"],
    ),
    ("SELECT pg_catalog.\"network_supeq\"('10.1.2.3'::inet, '10.1.2.3'::inet) AS v", ["t"]),
    ("SELECT pg_catalog.\"network_supeq\"('::1'::inet, '::1'::inet) AS v", ["t"]),
    (
        "SELECT pg_catalog.\"network_supeq\"(NULL::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_supeq\"('192.168.0.1/24'::inet, NULL::inet) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"network_supeq\"('0.0.0.0/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"network_supeq\"('192.168.0.1/24'::inet, '0.0.0.0/0'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_supeq\"('255.255.255.255'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_supeq\"('192.168.0.1/24'::inet, '255.255.255.255'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_supeq\"('::/0'::inet, '192.168.0.1/24'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"network_supeq\"('192.168.0.1/24'::inet, '::/0'::inet) AS v LIMIT 1001",
        ["f"],
    ),
    ('SELECT pg_catalog."set_masklen"(NULL::cidr, NULL::integer) AS v', [None]),
    ("SELECT pg_catalog.\"set_masklen\"('192.168.0.0/24'::cidr, 3::integer) AS v", ["192.0.0.0/3"]),
    ("SELECT pg_catalog.\"set_masklen\"('::1/128'::cidr, 42::integer) AS v", ["::/42"]),
    ('SELECT pg_catalog."set_masklen"(NULL::cidr, 3::integer) AS v LIMIT 1001', [None]),
    (
        "SELECT pg_catalog.\"set_masklen\"('192.168.0.0/24'::cidr, NULL::integer) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"set_masklen\"('0.0.0.0/0'::cidr, 3::integer) AS v LIMIT 1001",
        ["0.0.0.0/3"],
    ),
    ("SELECT pg_catalog.\"set_masklen\"('::/0'::cidr, 3::integer) AS v LIMIT 1001", ["::/3"]),
    (
        "SELECT pg_catalog.\"set_masklen\"('192.168.0.0/24'::cidr, '0'::integer) AS v LIMIT 1001",
        ["0.0.0.0/0"],
    ),
    ('SELECT pg_catalog."set_masklen"(NULL::inet, NULL::integer) AS v', [None]),
    (
        "SELECT pg_catalog.\"set_masklen\"('192.168.0.1/24'::inet, 3::integer) AS v",
        ["192.168.0.1/3"],
    ),
    ("SELECT pg_catalog.\"set_masklen\"('::1'::inet, 42::integer) AS v", ["::1/42"]),
    ('SELECT pg_catalog."set_masklen"(NULL::inet, 3::integer) AS v LIMIT 1001', [None]),
    (
        "SELECT pg_catalog.\"set_masklen\"('192.168.0.1/24'::inet, NULL::integer) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"set_masklen\"('0.0.0.0/0'::inet, 3::integer) AS v LIMIT 1001",
        ["0.0.0.0/3"],
    ),
    (
        "SELECT pg_catalog.\"set_masklen\"('255.255.255.255'::inet, 3::integer) AS v LIMIT 1001",
        ["255.255.255.255/3"],
    ),
    ("SELECT pg_catalog.\"set_masklen\"('::/0'::inet, 3::integer) AS v LIMIT 1001", ["::/3"]),
    (
        "SELECT pg_catalog.\"set_masklen\"('192.168.0.1/24'::inet, '0'::integer) AS v LIMIT 1001",
        ["192.168.0.1/0"],
    ),
    ("SELECT abbrev('10.1.0.0/16'::cidr)", ["10.1/16"]),
    ("SELECT abbrev('10.1.2.0/23'::cidr)", ["10.1.2/23"]),
    ("SELECT abbrev('10.0.0.0/9'::cidr)", ["10.0/9"]),
    ("SELECT host('::ffff:1.2.3.4'::inet)", ["::ffff:1.2.3.4"]),
    ("SELECT host('::1.2.3.4'::inet)", ["::1.2.3.4"]),
    ("SELECT host('2001:db8::1:0:0:1'::inet)", ["2001:db8::1:0:0:1"]),
    ("SELECT host('1:0:0:2::'::inet)", ["1:0:0:2::"]),
    ("SELECT set_masklen('192.168.1.5/24'::inet, 16)", ["192.168.1.5/16"]),
    ("SELECT set_masklen('192.168.1.0/24'::cidr, 16)", ["192.168.0.0/16"]),
    ("SELECT set_masklen('192.168.1.5/24'::inet, -1)", ["192.168.1.5"]),
    ("SELECT network_sub('192.168.1.0/24'::inet, '192.168.0.0/16'::inet)", ["t"]),
    ("SELECT network_overlap('10.0.0.0/8'::inet, '10.1.0.0/16'::inet)", ["t"]),
    ("SELECT netmask('::1/64'::inet)", ["ffff:ffff:ffff:ffff::"]),
    ("SELECT broadcast('2001:db8::/32'::inet)", ["2001:db8:ffff:ffff:ffff:ffff:ffff:ffff/32"]),
    ("SELECT cidr('192.168.1.5/24'::inet)", ["192.168.1.0/24"]),
]
RAISED = [
    ("SELECT pg_catalog.\"set_masklen\"('10.0.0.0/8'::cidr, (-7)::integer) AS v", "22023"),
    (
        "SELECT pg_catalog.\"set_masklen\"('192.168.0.0/24'::cidr, '-2147483648'::integer) AS v LIMIT 1001",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"set_masklen\"('192.168.0.0/24'::cidr, '2147483647'::integer) AS v LIMIT 1001",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"set_masklen\"('192.168.0.0/24'::cidr, '2147483648'::integer) AS v LIMIT 1001",
        "22003",
    ),
    ("SELECT pg_catalog.\"set_masklen\"('10.1.2.3'::inet, (-7)::integer) AS v", "22023"),
    (
        "SELECT pg_catalog.\"set_masklen\"('192.168.0.1/24'::inet, '-2147483648'::integer) AS v LIMIT 1001",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"set_masklen\"('192.168.0.1/24'::inet, '2147483647'::integer) AS v LIMIT 1001",
        "22023",
    ),
    ("SELECT set_masklen('192.168.1.5/24'::inet, 33)", "22023"),
]


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "t" if value else "f"
    return str(value)


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_clickhouse_answers_postgresql(runtime, sql, want):
    got = [
        _text(row[0])
        for row in runtime.run_sync(transpile(sql, "clickhouse", generated=False)).iter_rows()
    ]
    assert got == want


@pytest.mark.parametrize(("sql", "sqlstate"), RAISED)
def test_clickhouse_raises_postgresqls_class(runtime, sql, sqlstate):
    with pytest.raises(FunctionRefused) as refused:  # of constants: raised as the statement is read
        transpile(sql, "clickhouse", generated=False)
    assert getattr(refused.value, "sqlstate", "")[:2] == sqlstate[:2]
