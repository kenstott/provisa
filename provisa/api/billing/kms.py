# Copyright (c) 2026 Kenneth Stott
# Canary: a99d46b7-feb1-4117-8b1b-8be379b45830
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Thin async wrappers around boto3 KMS and AES-256-GCM helpers."""

# Requirements: REQ-073, REQ-074

from __future__ import annotations

import asyncio
import os

import boto3
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def _kms_client():
    region = os.environ.get("AWS_KMS_REGION", "us-east-1")
    return boto3.client("kms", region_name=region)


async def create_tenant_key(tenant_id: str) -> str:  # REQ-073, REQ-074
    """Create KMS CMK for tenant. Returns KeyArn."""
    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(
        None,
        lambda: _kms_client().create_key(
            Description=f"provisa-tenant-{tenant_id}",
            KeyUsage="ENCRYPT_DECRYPT",
        ),
    )
    return response["KeyMetadata"]["Arn"]


async def generate_data_key(key_arn: str) -> tuple[bytes, bytes]:
    """Returns (plaintext_dek, encrypted_dek). Caller must zero plaintext_dek immediately after use."""
    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(
        None,
        lambda: _kms_client().generate_data_key(KeyId=key_arn, KeySpec="AES_256"),
    )
    return response["Plaintext"], response["CiphertextBlob"]


async def decrypt_data_key(key_arn: str, encrypted_dek: bytes) -> bytes:
    """Decrypt an encrypted DEK. Returns plaintext DEK."""
    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(
        None,
        lambda: _kms_client().decrypt(KeyId=key_arn, CiphertextBlob=encrypted_dek),
    )
    return response["Plaintext"]


def aes_encrypt(plaintext: bytes, dek: bytes) -> tuple[bytes, bytes]:
    """Returns (iv, ciphertext)."""
    iv = os.urandom(12)
    aesgcm = AESGCM(dek)
    ciphertext = aesgcm.encrypt(iv, plaintext, None)
    return iv, ciphertext


def aes_decrypt(iv: bytes, ciphertext: bytes, dek: bytes) -> bytes:
    """Returns plaintext."""
    aesgcm = AESGCM(dek)
    return aesgcm.decrypt(iv, ciphertext, None)
