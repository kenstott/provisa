# Copyright (c) 2025 Kenneth Stott
# Canary: f6d0dca9-049e-468c-a1ff-7cf4a354e5df
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Auto-discovery of API endpoints from specs (Phase U)."""

from __future__ import annotations

import httpx

from provisa.api_source.models import (
    ApiColumn,
    ApiColumnType,
    ApiEndpointCandidate,
    ApiSourceType,
)


_OPENAPI_TYPE_MAP: dict[str, ApiColumnType] = {
    "string": ApiColumnType.string,
    "integer": ApiColumnType.integer,
    "number": ApiColumnType.number,
    "boolean": ApiColumnType.boolean,
}


def _infer_column_type(schema: dict) -> ApiColumnType:
    """Map OpenAPI/JSON Schema type to ApiColumnType."""
    s_type = schema.get("type", "")
    if s_type in _OPENAPI_TYPE_MAP:
        return _OPENAPI_TYPE_MAP[s_type]
    return ApiColumnType.jsonb


def _schema_to_columns(schema: dict, definitions: dict) -> list[ApiColumn]:
    """Extract columns from an object schema."""
    if "$ref" in schema:
        ref_name = schema["$ref"].split("/")[-1]
        schema = definitions.get(ref_name, {})

    # Handle array items
    if schema.get("type") == "array":
        items = schema.get("items", {})
        if "$ref" in items:
            ref_name = items["$ref"].split("/")[-1]
            schema = definitions.get(ref_name, {})
        else:
            schema = items

    properties = schema.get("properties", {})
    columns: list[ApiColumn] = []
    for name, prop in properties.items():
        if "$ref" in prop:
            col_type = ApiColumnType.jsonb
        else:
            col_type = _infer_column_type(prop)
        filterable = col_type != ApiColumnType.jsonb
        columns.append(ApiColumn(name=name, type=col_type, filterable=filterable))
    return columns


def _path_to_table_name(path: str) -> str:
    """Convert /api/v1/users/{id}/posts to api_v1_users_posts."""
    parts = [p for p in path.split("/") if p and not p.startswith("{")]
    return "_".join(parts).replace("-", "_")


async def introspect_openapi(spec_url: str) -> list[ApiEndpointCandidate]:
    """Parse OpenAPI spec and generate candidate tables for GET endpoints."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(spec_url)
        resp.raise_for_status()
        spec = resp.json()

    # Support both OpenAPI 3.x and Swagger 2.x definitions
    definitions = spec.get("definitions", {})
    if "components" in spec:
        definitions = spec.get("components", {}).get("schemas", definitions)

    paths = spec.get("paths", {})
    candidates: list[ApiEndpointCandidate] = []

    for path, methods in paths.items():
        if "get" not in methods:
            continue
        get_op = methods["get"]

        # Find response schema
        responses = get_op.get("responses", {})
        ok_resp = responses.get("200", responses.get("default", {}))
        schema: dict = {}

        # OpenAPI 3.x
        content = ok_resp.get("content", {})
        if "application/json" in content:
            schema = content["application/json"].get("schema", {})
        # Swagger 2.x
        elif "schema" in ok_resp:
            schema = ok_resp["schema"]

        if not schema:
            continue

        columns = _schema_to_columns(schema, definitions)
        if not columns:
            continue

        candidates.append(ApiEndpointCandidate(
            source_id="",  # set by caller
            path=path,
            method="GET",
            table_name=_path_to_table_name(path),
            columns=columns,
        ))

    return candidates


_GRAPHQL_SCALAR_MAP: dict[str, ApiColumnType] = {
    "String": ApiColumnType.string,
    "Int": ApiColumnType.integer,
    "Float": ApiColumnType.number,
    "Boolean": ApiColumnType.boolean,
    "ID": ApiColumnType.string,
}

_INTROSPECTION_QUERY = """
{
  __schema {
    queryType { name }
    types {
      name
      kind
      fields {
        name
        type {
          name
          kind
          ofType { name kind ofType { name kind } }
        }
      }
    }
  }
}
"""


def _unwrap_type(type_info: dict) -> tuple[str | None, str]:
    """Unwrap NON_NULL/LIST wrappers to get the base type name and kind."""
    kind = type_info.get("kind", "")
    name = type_info.get("name")
    if kind in ("NON_NULL", "LIST") and "ofType" in type_info and type_info["ofType"]:
        return _unwrap_type(type_info["ofType"])
    return name, kind


async def introspect_graphql(url: str) -> list[ApiEndpointCandidate]:
    """Introspect a GraphQL endpoint via __schema query."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json={"query": _INTROSPECTION_QUERY})
        resp.raise_for_status()
        data = resp.json()

    schema = data.get("data", {}).get("__schema", {})
    query_type_name = schema.get("queryType", {}).get("name", "Query")

    type_map: dict[str, dict] = {}
    for t in schema.get("types", []):
        type_map[t["name"]] = t

    query_type = type_map.get(query_type_name, {})
    candidates: list[ApiEndpointCandidate] = []

    for field in query_type.get("fields", []):
        field_name = field["name"]
        if field_name.startswith("__"):
            continue

        base_name, base_kind = _unwrap_type(field["type"])
        if not base_name or base_name.startswith("__"):
            continue

        # Resolve object type fields to columns
        obj_type = type_map.get(base_name, {})
        columns: list[ApiColumn] = []
        for obj_field in obj_type.get("fields", []):
            fname = obj_field["name"]
            ftype_name, ftype_kind = _unwrap_type(obj_field["type"])
            if ftype_name in _GRAPHQL_SCALAR_MAP:
                col_type = _GRAPHQL_SCALAR_MAP[ftype_name]
                columns.append(ApiColumn(name=fname, type=col_type, filterable=True))
            else:
                columns.append(ApiColumn(name=fname, type=ApiColumnType.jsonb, filterable=False))

        if not columns:
            continue

        candidates.append(ApiEndpointCandidate(
            source_id="",
            path=field_name,
            method="QUERY",
            table_name=field_name.replace("-", "_"),
            columns=columns,
        ))

    return candidates


async def introspect_grpc(host_port: str) -> list[ApiEndpointCandidate]:
    """Introspect a gRPC server via server reflection."""
    import grpc
    from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc

    channel = grpc.insecure_channel(host_port)
    stub = reflection_pb2_grpc.ServerReflectionStub(channel)

    # List services
    req = reflection_pb2.ServerReflectionRequest(list_services="")
    responses = stub.ServerReflectionInfo(iter([req]))

    service_names: list[str] = []
    for resp in responses:
        for svc in resp.list_services_response.service:
            if svc.name and not svc.name.startswith("grpc.reflection"):
                service_names.append(svc.name)

    candidates: list[ApiEndpointCandidate] = []
    for svc_name in service_names:
        # Get service descriptor
        req = reflection_pb2.ServerReflectionRequest(file_containing_symbol=svc_name)
        responses = stub.ServerReflectionInfo(iter([req]))

        for resp in responses:
            for proto_bytes in resp.file_descriptor_response.file_descriptor_proto:
                from google.protobuf import descriptor_pb2
                fd = descriptor_pb2.FileDescriptorProto()
                fd.ParseFromString(proto_bytes)

                for svc in fd.service:
                    for method in svc.method:
                        table_name = f"{svc.name}_{method.name}".lower().replace(".", "_")
                        # Find output message
                        output_name = method.output_type.split(".")[-1]
                        columns = _grpc_message_to_columns(fd, output_name)
                        candidates.append(ApiEndpointCandidate(
                            source_id="",
                            path=f"/{svc.name}/{method.name}",
                            method="RPC",
                            table_name=table_name,
                            columns=columns,
                        ))

    channel.close()
    return candidates


_PROTO_TYPE_MAP: dict[int, ApiColumnType] = {
    1: ApiColumnType.number,    # double
    2: ApiColumnType.number,    # float
    3: ApiColumnType.integer,   # int64
    4: ApiColumnType.integer,   # uint64
    5: ApiColumnType.integer,   # int32
    8: ApiColumnType.boolean,   # bool
    9: ApiColumnType.string,    # string
    13: ApiColumnType.integer,  # uint32
    17: ApiColumnType.integer,  # sint32
    18: ApiColumnType.integer,  # sint64
}


def _grpc_message_to_columns(fd, message_name: str) -> list[ApiColumn]:
    """Convert a protobuf message descriptor to columns."""
    for msg in fd.message_type:
        if msg.name == message_name:
            columns: list[ApiColumn] = []
            for field in msg.field:
                col_type = _PROTO_TYPE_MAP.get(field.type, ApiColumnType.jsonb)
                if field.type == 11:  # MESSAGE type
                    col_type = ApiColumnType.jsonb
                filterable = col_type != ApiColumnType.jsonb
                columns.append(ApiColumn(
                    name=field.name, type=col_type, filterable=filterable,
                ))
            return columns
    return []
