# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3f1a2e-4c8b-4e9f-a5d2-1b6c3e8f2a4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Neo4j Bolt protocol server for Provisa."""


def start_bolt_server(host, port, ssl_ctx, loop):
    from provisa.bolt.server import start_bolt_server as _start

    return _start(host, port, ssl_ctx, loop)


__all__ = ["start_bolt_server"]
