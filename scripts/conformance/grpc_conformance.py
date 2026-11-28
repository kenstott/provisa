# Copyright (c) 2026 Kenneth Stott
# Canary: 7e1f4a28-9c3b-4d5e-a2f8-6b0d9c4e1a73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's gRPC surface against the gRPC protocol's server reflection, health checking and
status codes (REQ-2167).

gRPC publishes interop tests for its own test service, not a conformance suite for a server of
another service, so the scenario set is derived from the protocol's documents, one case per
normative statement a client observes, named for its source:

- server reflection (grpc/grpc doc/server-reflection.md; the reflection.proto services
  grpc.reflection.v1 and v1alpha): the services listed, every service's file descriptors resolving
  into one pool, every method's types found;
- health checking (doc/health-checking.md, grpc.health.v1.Health);
- status codes (doc/statuscodes.md): which code a server answers each condition with;
- a query of the fixture table's RPC (pgwire_catalog_types.FIXTURE_TABLE, the one table of the
  model every conformance instance loads), answered OK with its rows.

Every case is passed, skipped by design, a documented limit or failed (REQ-2167, amended
2026-11-23); the run is
judged by baseline_gate.py against scripts/conformance/expected/grpc.json.

Usage: grpc_conformance.py <host:port> <role> <authorization> <out.json> <out.md>
"""

# Requirements: REQ-2167

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import grpc
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.message_factory import GetMessageClass
from grpc_health.v1 import health_pb2
from grpc_reflection.v1alpha import reflection_pb2 as v1alpha_pb2

from pgwire_catalog_types import FIXTURE_TABLE
from surface_cases import Case, expect, report, run, write

SUITE = "gRPC server reflection, health checking and status code scenarios"
VERSION = f"grpcio {grpc.__version__}"
REFLECTION_V1ALPHA = "grpc.reflection.v1alpha.ServerReflection"
REFLECTION_V1 = "grpc.reflection.v1.ServerReflection"
TIMEOUT = 120


HEALTH_CHECK = "/grpc.health.v1.Health/Check"


def _identity(message: bytes) -> bytes:
    return message


def _serialize(message: Any) -> bytes:
    return message.SerializeToString()


class Grpc:
    """The gRPC surface of one instance, as the job user acting as ``role``."""

    def __init__(self, target: str, role: str, authorization: str) -> None:
        self.channel = grpc.insecure_channel(target)
        self.metadata = (("authorization", authorization), ("x-provisa-role", role))
        self.role = role
        self._pool: DescriptorPool | None = None
        self._services: list[str] | None = None

    def _ask(self, request: Any, service: str = REFLECTION_V1ALPHA) -> Any:
        """One reflection request's answer. v1 and v1alpha carry the same messages."""
        rpc = self.channel.stream_stream(
            f"/{service}/ServerReflectionInfo",
            request_serializer=_serialize,
            response_deserializer=v1alpha_pb2.ServerReflectionResponse.FromString,
        )
        (answer,) = rpc(iter([request]), metadata=self.metadata, timeout=TIMEOUT)
        return answer

    def _reflect(self, request: Any) -> Any:
        answer = self._ask(request)
        expect(
            not answer.HasField("error_response"),
            f"reflection error {answer.error_response.error_code}: "
            f"{answer.error_response.error_message}",
        )
        return answer

    def services(self) -> list[str]:
        if self._services is None:
            answer = self._reflect(v1alpha_pb2.ServerReflectionRequest(list_services=""))
            self._services = [s.name for s in answer.list_services_response.service]
        assert self._services is not None
        return self._services

    def provisa_services(self) -> list[str]:
        found = [s for s in self.services() if not s.startswith("grpc.")]
        expect(bool(found), f"reflection lists no service of the model: {self.services()}")
        return found

    def pool(self) -> DescriptorPool:
        """Every listed service's file descriptors, with their dependencies, in one pool."""
        if self._pool is None:
            protos: dict[str, descriptor_pb2.FileDescriptorProto] = {}
            for service in self.provisa_services():
                answer = self._reflect(
                    v1alpha_pb2.ServerReflectionRequest(file_containing_symbol=service)
                )
                for raw in answer.file_descriptor_response.file_descriptor_proto:
                    proto = descriptor_pb2.FileDescriptorProto.FromString(raw)
                    protos[proto.name] = proto
            pool = DescriptorPool()
            added: set[str] = set()

            def add(name: str) -> None:
                if name in added:
                    return
                proto = protos.get(name)
                expect(proto is not None, f"reflection never sent {name}, a dependency")
                assert proto is not None
                for dep in proto.dependency:
                    add(dep)
                pool.Add(proto)
                added.add(name)

            for name in protos:
                add(name)
            self._pool = pool
        return self._pool

    def query_method(self) -> Any:
        """The fixture table's server-streaming Query<Table> method. The role's service also
        declares the control plane's own tables (data products, commands, ...), which the
        conformance instance leaves empty, so the first Query RPC declared is not the fixture's.
        Matched on the table's name without case or underscores, as the type name is the
        table's in PascalCase."""
        wanted = "query" + FIXTURE_TABLE.replace("_", "").lower()
        declared = []
        for service in self.provisa_services():
            for method in self.pool().FindServiceByName(service).methods:
                declared.append(method.name)
                if method.name.lower() == wanted and method.server_streaming:
                    return method
        raise AssertionError(f"the service declares no RPC for {FIXTURE_TABLE}: {declared}")

    def _path(self, method: Any) -> str:
        return f"/{method.containing_service.full_name}/{method.name}"

    def _query(self, metadata: Any, payload: bytes | None = None) -> list[Any]:
        method = self.query_method()
        out_cls = GetMessageClass(method.output_type)
        rpc = self.channel.unary_stream(
            self._path(method),
            request_serializer=_identity,
            response_deserializer=out_cls.FromString,
        )
        body = _serialize(GetMessageClass(method.input_type)()) if payload is None else payload
        return list(rpc(body, metadata=metadata, timeout=TIMEOUT))

    def _code(self, call: Any) -> grpc.StatusCode:
        try:
            call()
        except grpc.RpcError as err:
            return err.code()  # pyright: ignore[reportAttributeAccessIssue]
        return grpc.StatusCode.OK

    # --- server reflection ---------------------------------------------------------------------

    def lists_services_v1alpha(self) -> None:
        self.provisa_services()

    def lists_services_v1(self) -> None:
        answer = self._ask(v1alpha_pb2.ServerReflectionRequest(list_services=""), REFLECTION_V1)
        names = [s.name for s in answer.list_services_response.service]
        expect(any(not n.startswith("grpc.") for n in names), f"v1 listed {names}")

    def descriptors_resolve(self) -> None:
        for service in self.provisa_services():
            self.pool().FindServiceByName(service)

    def method_types_found(self) -> None:
        missing = []
        for service in self.provisa_services():
            for method in self.pool().FindServiceByName(service).methods:
                for described in (method.input_type, method.output_type):
                    try:
                        GetMessageClass(described)
                    except (TypeError, KeyError) as exc:
                        missing.append(f"{method.full_name}: {exc}")
        expect(not missing, f"methods whose types do not build: {missing[:5]}")

    def unknown_symbol_not_found(self) -> None:
        answer = self._ask(
            v1alpha_pb2.ServerReflectionRequest(file_containing_symbol="no.such.Symbol")
        )
        code = answer.error_response.error_code
        expect(
            answer.HasField("error_response") and code == grpc.StatusCode.NOT_FOUND.value[0],
            f"an unknown symbol answered error code {code}",
        )

    # --- health checking -----------------------------------------------------------------------

    def _health(self, service: str) -> Any:
        rpc = self.channel.unary_unary(
            HEALTH_CHECK,
            request_serializer=_serialize,
            response_deserializer=health_pb2.HealthCheckResponse.FromString,
        )
        return rpc(health_pb2.HealthCheckRequest(service=service), timeout=TIMEOUT)

    def health_serving(self) -> None:
        answer = self._health("")
        expect(
            answer.status == health_pb2.HealthCheckResponse.SERVING,
            f"overall health {health_pb2.HealthCheckResponse.ServingStatus.Name(answer.status)}",
        )

    def health_unknown_service(self) -> None:
        code = self._code(lambda: self._health("no.such.Service"))
        expect(code == grpc.StatusCode.NOT_FOUND, f"answered {code.name}")

    # --- status codes --------------------------------------------------------------------------

    def query_ok(self) -> None:
        rows = self._query(self.metadata)
        expect(bool(rows), "a query of the fixture table answered OK with no row")

    def unauthenticated(self) -> None:
        code = self._code(lambda: self._query((("x-provisa-role", self.role),)))
        expect(code == grpc.StatusCode.UNAUTHENTICATED, f"no credential answered {code.name}")

    def bad_credential(self) -> None:
        metadata = (("authorization", "Bearer not-a-token"), ("x-provisa-role", self.role))
        code = self._code(lambda: self._query(metadata))
        expect(code == grpc.StatusCode.UNAUTHENTICATED, f"a bad token answered {code.name}")

    def role_not_held(self) -> None:
        metadata = (self.metadata[0], ("x-provisa-role", "no_such_role_x"))
        code = self._code(lambda: self._query(metadata))
        expect(code == grpc.StatusCode.PERMISSION_DENIED, f"answered {code.name}")

    def unknown_method(self) -> None:
        service = self.provisa_services()[0]
        rpc = self.channel.unary_unary(f"/{service}/NoSuchMethodX")
        code = self._code(lambda: rpc(b"", metadata=self.metadata, timeout=TIMEOUT))
        expect(code == grpc.StatusCode.UNIMPLEMENTED, f"answered {code.name}")

    def unknown_service(self) -> None:
        rpc = self.channel.unary_unary("/no.such.v1.Service/Method")
        code = self._code(lambda: rpc(b"", metadata=self.metadata, timeout=TIMEOUT))
        expect(code == grpc.StatusCode.UNIMPLEMENTED, f"answered {code.name}")

    def unparsable_request(self) -> None:
        # A field 1 length-delimited with a length past the end: no message parses from it.
        code = self._code(lambda: self._query(self.metadata, payload=b"\x0a\xff\xff\xff\x0f"))
        expect(code == grpc.StatusCode.INTERNAL, f"an unparsable request answered {code.name}")


def cases(g: Grpc) -> list[Case]:
    rf, hc, sc = "Server Reflection", "Health Checking", "Status Codes"
    return [
        Case(rf, "v1alpha ListServices names the model's service", g.lists_services_v1alpha),
        Case(rf, "v1 ListServices names the model's service", g.lists_services_v1),
        Case(rf, "FileContainingSymbol resolves every listed service", g.descriptors_resolve),
        Case(rf, "every method's request and response types build", g.method_types_found),
        Case(rf, "an unknown symbol is answered NOT_FOUND", g.unknown_symbol_not_found),
        Case(hc, "Check of the server (empty service) answers SERVING", g.health_serving),
        Case(hc, "Check of an unknown service answers NOT_FOUND", g.health_unknown_service),
        Case(sc, "OK: a query of the fixture table streams its rows", g.query_ok),
        Case(sc, "UNAUTHENTICATED: no credential", g.unauthenticated),
        Case(sc, "UNAUTHENTICATED: an invalid credential", g.bad_credential),
        Case(sc, "PERMISSION_DENIED: a role the caller does not hold", g.role_not_held),
        Case(sc, "UNIMPLEMENTED: a method the service does not have", g.unknown_method),
        Case(sc, "UNIMPLEMENTED: a service the server does not have", g.unknown_service),
        Case(sc, "INTERNAL: a request message that does not parse", g.unparsable_request),
    ]


def main(argv: list[str]) -> int:
    target, role, authorization, out_json, out_md = argv
    doc = report("grpc", SUITE, VERSION, run(cases(Grpc(target, role, authorization))))
    return write(doc, "gRPC", Path(out_json), Path(out_md))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
