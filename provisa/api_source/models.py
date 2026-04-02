# Copyright (c) 2025 Kenneth Stott
# Canary: 31973b3d-4288-4201-a2d0-54b9c46e14de
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pydantic models for API data sources (Phase U)."""

from enum import Enum

from pydantic import BaseModel, Field

from provisa.core.auth_models import ApiAuth


class ApiSourceType(str, Enum):
    openapi = "openapi"
    graphql_api = "graphql_api"
    grpc_api = "grpc_api"


class PaginationType(str, Enum):
    link_header = "link_header"
    cursor = "cursor"
    offset = "offset"
    page_number = "page_number"


class ParamType(str, Enum):
    query = "query"
    path = "path"
    body = "body"
    header = "header"
    variable = "variable"  # GraphQL variable


class ApiColumnType(str, Enum):
    string = "string"
    integer = "integer"
    number = "number"
    boolean = "boolean"
    jsonb = "jsonb"


class ApiSource(BaseModel):
    id: str
    type: ApiSourceType
    base_url: str
    spec_url: str | None = None
    auth: ApiAuth | None = None


class ApiColumn(BaseModel):
    name: str
    type: ApiColumnType
    filterable: bool = True
    param_type: ParamType | None = None
    param_name: str | None = None


class PaginationConfig(BaseModel):
    type: PaginationType
    cursor_field: str | None = None
    cursor_param: str | None = None
    page_param: str | None = None
    page_size_param: str | None = None
    page_size: int = 100
    max_pages: int = 10


class ApiEndpoint(BaseModel):
    id: int | None = None
    source_id: str
    path: str
    method: str = "GET"
    table_name: str
    columns: list[ApiColumn]
    ttl: int = 300
    response_root: str | None = None
    pagination: PaginationConfig | None = None


class ApiEndpointCandidate(BaseModel):
    id: int | None = None
    source_id: str
    path: str
    method: str = "GET"
    table_name: str | None = None
    columns: list[ApiColumn]
    status: str = "discovered"


class PromotionConfig(BaseModel):
    jsonb_column: str
    field: str  # dot-path e.g. "address.city"
    target_column: str
    target_type: str  # integer, numeric, boolean, timestamptz, text
