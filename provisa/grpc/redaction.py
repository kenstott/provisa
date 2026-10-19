# Copyright (c) 2026 Kenneth Stott
# Canary: 3e6c5445-fade-472b-9857-ae60779fabe4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every gRPC status message passes central redaction (REQ-2087): the server's first
interceptor hands each handler a context whose ``abort`` and ``set_details`` redact."""

# Requirements: REQ-2087

from __future__ import annotations

from typing import Any

import grpc

from provisa.security.redaction import redact


class _RedactingContext:
    def __init__(self, context: Any) -> None:
        self._context = context
        self.aborted = False

    def abort(self, code: grpc.StatusCode, details: str) -> None:
        self.aborted = True
        self._context.abort(code, redact(details or ""))

    def set_details(self, details: str) -> None:
        self._context.set_details(redact(details or ""))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._context, name)


def _unanswered(rctx: _RedactingContext, exc: BaseException) -> None:
    """An exception a handler raised without aborting: gRPC would send its text as the status
    message. It is ended the same way (UNKNOWN), with the text redacted."""
    if rctx.aborted:
        raise exc
    rctx.abort(grpc.StatusCode.UNKNOWN, f"Exception calling application: {exc}")


def _unary(inner):
    def call(request_or_iterator, ctx):
        rctx = _RedactingContext(ctx)
        try:
            return inner(request_or_iterator, rctx)
        except Exception as exc:  # noqa: BLE001  # allow-blind-except: whatever a handler raises, its text reaches the client as gRPC's status message; this only redacts that text and re-raises an abort it already made
            _unanswered(rctx, exc)
            raise

    return call


def _streaming(inner):
    def call(request_or_iterator, ctx):
        rctx = _RedactingContext(ctx)
        try:
            yield from inner(request_or_iterator, rctx)
        except Exception as exc:  # noqa: BLE001  # allow-blind-except: as _unary, for a response stream
            _unanswered(rctx, exc)
            raise

    return call


class RedactingInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None
        if handler.unary_unary is not None:
            inner = handler.unary_unary
            return grpc.unary_unary_rpc_method_handler(
                _unary(inner),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        if handler.unary_stream is not None:
            inner_us = handler.unary_stream
            return grpc.unary_stream_rpc_method_handler(
                _streaming(inner_us),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        if handler.stream_unary is not None:
            inner_su = handler.stream_unary
            return grpc.stream_unary_rpc_method_handler(
                _unary(inner_su),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        inner_ss = handler.stream_stream
        return grpc.stream_stream_rpc_method_handler(
            _streaming(inner_ss),
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
