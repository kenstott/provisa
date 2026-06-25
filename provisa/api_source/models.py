# Copyright (c) 2026 Kenneth Stott
# Canary: 31973b3d-4288-4201-a2d0-54b9c46e14de
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pydantic models for API data sources (Phase U)."""

# Requirements: REQ-119, REQ-295, REQ-297, REQ-298, REQ-299, REQ-318

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field

from provisa.core.auth_models import ApiAuth


class ApiSourceType(str, Enum):  # REQ-295, REQ-297, REQ-298
    openapi = "openapi"
    graphql_api = "graphql_api"
    grpc_api = "grpc_api"


class PaginationType(str, Enum):  # REQ-318
    link_header = "link_header"
    cursor = "cursor"
    offset = "offset"
    page_number = "page_number"


class ParamType(str, Enum):  # REQ-299, REQ-599
    query = "query"
    path = "path"
    body = "body"
    header = "header"
    variable = "variable"  # GraphQL variable


class ApiColumnType(str, Enum):  # REQ-119, REQ-299
    string = "string"
    integer = "integer"
    number = "number"
    boolean = "boolean"
    jsonb = "jsonb"


class ApiSource(BaseModel):  # REQ-295, REQ-297, REQ-298
    id: str
    type: ApiSourceType
    base_url: str
    spec_url: str | None = None
    auth: ApiAuth | None = None


class ApiColumn(BaseModel):  # REQ-299, REQ-599
    name: str
    type: ApiColumnType
    filterable: bool = True
    param_type: ParamType | None = None
    param_name: str | None = None
    object_fields: list[dict] = []
    description: str | None = None


class PaginationConfig(BaseModel):  # REQ-318
    type: PaginationType
    cursor_field: str | None = None
    cursor_param: str | None = None
    page_param: str | None = None
    page_size_param: str | None = None
    page_size: int = 100
    max_pages: int = 10


class PromotionConfig(BaseModel):  # REQ-119
    jsonb_column: str
    field: str  # dot-path e.g. "address.city"
    target_column: str
    target_type: str  # integer, numeric, boolean, timestamptz, text


class ApiEndpoint(BaseModel):  # REQ-119, REQ-295, REQ-297, REQ-298, REQ-299, REQ-318
    id: int | None = None
    source_id: str
    path: str
    method: str = "GET"
    table_name: str
    columns: list[ApiColumn]
    ttl: int = 300
    response_root: str | None = None
    error_path: str | None = None
    pk_column: str | None = None
    pagination: PaginationConfig | None = None
    max_concurrency: int | None = None  # caps parallel path-param fetches; None = unlimited
    default_params: dict = Field(
        default_factory=dict
    )  # query params used for initial cache population
    # REQ-119: JSONB field promotions → PG generated columns (registered + filterable).
    promotions: list[PromotionConfig] = Field(default_factory=list)
    # Phase AO: query-API sources (Neo4j, SPARQL)
    body_encoding: Literal["json", "form"] | None = None
    query_template: str | None = None  # Cypher or SPARQL query
    response_normalizer: str | None = None  # e.g. "neo4j_tabular" or "sparql_bindings"


class ApiEndpointCandidate(BaseModel):
    id: int | None = None
    source_id: str
    path: str
    method: str = "GET"
    table_name: str | None = None
    columns: list[ApiColumn]
    status: str = "discovered"
