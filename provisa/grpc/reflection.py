# Copyright (c) 2025 Kenneth Stott
# Canary: 367d5bac-35af-442c-a932-a9ed22c792af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Enable gRPC server reflection for service discovery."""

from __future__ import annotations

import grpc
from grpc_reflection.v1alpha import reflection


def enable_reflection(server: grpc.aio.Server, service_names: list[str]) -> None:
    """Enable gRPC server reflection for the given service names.

    Args:
        server: The gRPC server instance.
        service_names: Fully qualified service names to expose.
    """
    service_names_with_reflection = list(service_names) + [
        reflection.SERVICE_NAME,
    ]
    reflection.enable_server_reflection(service_names_with_reflection, server)
