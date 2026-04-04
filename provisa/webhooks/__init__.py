# Copyright (c) 2025 Kenneth Stott
# Canary: ae86e30c-7053-4d12-adc2-16847447c0bd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Webhook execution module for Provisa tracked webhook mutations."""

from provisa.webhooks.executor import execute_webhook

__all__ = ["execute_webhook"]
