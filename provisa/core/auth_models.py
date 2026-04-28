# Copyright (c) 2026 Kenneth Stott
# Canary: cdbdbf87-4193-4218-8a32-3cccf1ea62df
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Typed authentication models for data source connections.

All secret-bearing string fields support the ${provider:reference} pattern
(e.g. ${env:API_TOKEN}) and are resolved at call time via resolve_secrets().
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field


# ── API Source Auth ──────────────────────────────────────────────


class ApiKeyLocation(str, Enum):
    header = "header"
    query = "query"


class ApiAuthNone(BaseModel):
    type: Literal["none"] = "none"


class ApiAuthBearer(BaseModel):
    type: Literal["bearer"] = "bearer"
    token: str


class ApiAuthBasic(BaseModel):
    type: Literal["basic"] = "basic"
    username: str
    password: str


class ApiAuthApiKey(BaseModel):
    type: Literal["api_key"] = "api_key"
    key: str
    name: str  # header name or query param name
    location: ApiKeyLocation = ApiKeyLocation.header


class ApiAuthOAuth2ClientCredentials(BaseModel):
    type: Literal["oauth2_client_credentials"] = "oauth2_client_credentials"
    client_id: str
    client_secret: str
    token_url: str
    scope: str | None = None


class ApiAuthCustomHeaders(BaseModel):
    type: Literal["custom_headers"] = "custom_headers"
    headers: dict[str, str]


ApiAuth = Annotated[
    Union[
        ApiAuthNone,
        ApiAuthBearer,
        ApiAuthBasic,
        ApiAuthApiKey,
        ApiAuthOAuth2ClientCredentials,
        ApiAuthCustomHeaders,
    ],
    Field(discriminator="type"),
]


# ── Kafka Source Auth ────────────────────────────────────────────


class KafkaAuthNone(BaseModel):
    type: Literal["none"] = "none"


class KafkaAuthSaslPlain(BaseModel):
    type: Literal["sasl_plain"] = "sasl_plain"
    username: str
    password: str


class KafkaAuthSaslScram256(BaseModel):
    type: Literal["sasl_scram_256"] = "sasl_scram_256"
    username: str
    password: str


class KafkaAuthSaslScram512(BaseModel):
    type: Literal["sasl_scram_512"] = "sasl_scram_512"
    username: str
    password: str


KafkaAuth = Annotated[
    Union[
        KafkaAuthNone,
        KafkaAuthSaslPlain,
        KafkaAuthSaslScram256,
        KafkaAuthSaslScram512,
    ],
    Field(discriminator="type"),
]


# ── Cloud DW Auth ────────────────────────────────────────────────


class SnowflakeAuthPassword(BaseModel):
    type: Literal["password"] = "password"
    username: str
    password: str


class SnowflakeAuthKeyPair(BaseModel):
    type: Literal["key_pair"] = "key_pair"
    username: str
    private_key_path: str
    private_key_passphrase: str | None = None


class SnowflakeAuthOAuth(BaseModel):
    type: Literal["oauth"] = "oauth"
    token: str


SnowflakeAuth = Annotated[
    Union[
        SnowflakeAuthPassword,
        SnowflakeAuthKeyPair,
        SnowflakeAuthOAuth,
    ],
    Field(discriminator="type"),
]


class BigQueryAuthServiceAccount(BaseModel):
    type: Literal["service_account"] = "service_account"
    credentials_json: str  # path or ${env:GOOGLE_CREDENTIALS}


class BigQueryAuthDefault(BaseModel):
    type: Literal["application_default"] = "application_default"


BigQueryAuth = Annotated[
    Union[
        BigQueryAuthServiceAccount,
        BigQueryAuthDefault,
    ],
    Field(discriminator="type"),
]


class DatabricksAuthToken(BaseModel):
    type: Literal["token"] = "token"
    access_token: str  # personal access token or ${env:...}


class DatabricksAuthOAuth(BaseModel):
    type: Literal["oauth"] = "oauth"
    client_id: str
    client_secret: str
    token_url: str


DatabricksAuth = Annotated[
    Union[
        DatabricksAuthToken,
        DatabricksAuthOAuth,
    ],
    Field(discriminator="type"),
]


# ── Redshift Auth ────────────────────────────────────────────────


class RedshiftAuthPassword(BaseModel):
    type: Literal["password"] = "password"
    username: str
    password: str


class RedshiftAuthIAM(BaseModel):
    type: Literal["iam"] = "iam"
    access_key_id: str
    secret_access_key: str
    region: str = "us-east-1"


RedshiftAuth = Annotated[
    Union[RedshiftAuthPassword, RedshiftAuthIAM],
    Field(discriminator="type"),
]


# ── Data Lake Auth (S3/ADLS/GCS backends) ────────────────────────


class DataLakeAuthNone(BaseModel):
    type: Literal["none"] = "none"


class DataLakeAuthAWS(BaseModel):
    type: Literal["aws"] = "aws"
    access_key_id: str
    secret_access_key: str
    region: str = "us-east-1"
    endpoint: str | None = None  # for MinIO/S3-compatible


class DataLakeAuthAzure(BaseModel):
    type: Literal["azure"] = "azure"
    storage_account: str
    access_key: str | None = None  # shared key
    sas_token: str | None = None  # SAS token alternative


class DataLakeAuthGCS(BaseModel):
    type: Literal["gcs"] = "gcs"
    credentials_json: str  # path or ${env:GOOGLE_CREDENTIALS}


DataLakeAuth = Annotated[
    Union[
        DataLakeAuthNone,
        DataLakeAuthAWS,
        DataLakeAuthAzure,
        DataLakeAuthGCS,
    ],
    Field(discriminator="type"),
]


# ── Elasticsearch Auth ───────────────────────────────────────────


class ElasticsearchAuthNone(BaseModel):
    type: Literal["none"] = "none"


class ElasticsearchAuthBasic(BaseModel):
    type: Literal["basic"] = "basic"
    username: str
    password: str


class ElasticsearchAuthApiKey(BaseModel):
    type: Literal["api_key"] = "api_key"
    api_key: str  # base64 encoded id:api_key


class ElasticsearchAuthBearer(BaseModel):
    type: Literal["bearer"] = "bearer"
    token: str


ElasticsearchAuth = Annotated[
    Union[
        ElasticsearchAuthNone,
        ElasticsearchAuthBasic,
        ElasticsearchAuthApiKey,
        ElasticsearchAuthBearer,
    ],
    Field(discriminator="type"),
]


# ── Prometheus Auth ──────────────────────────────────────────────


class PrometheusAuthNone(BaseModel):
    type: Literal["none"] = "none"


class PrometheusAuthBearer(BaseModel):
    type: Literal["bearer"] = "bearer"
    token: str


class PrometheusAuthBasic(BaseModel):
    type: Literal["basic"] = "basic"
    username: str
    password: str


PrometheusAuth = Annotated[
    Union[PrometheusAuthNone, PrometheusAuthBearer, PrometheusAuthBasic],
    Field(discriminator="type"),
]
