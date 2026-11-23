# Generated from provisa/python/services/provisa_services/__init__.py by
# provisa-sdk/python/gen_services_stubs.py. Do not edit.

import base64
import contextvars
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from collections.abc import Callable, Sequence
from typing import Any, Generic, TypeVar
__all__ = ['Discovered', 'DiscoveredCommand', 'DiscoveredQuery', 'DiscoveredTable', 'Filter', 'Order', 'ReadContext', 'TableConnection', 'TableRef', 'ServiceError', 'audit', 'cache', 'dq', 'http', 'llm', 'log', 'metric', 'secrets', 'settings', 'vectors']

class ServiceError(Exception):
    """A service refused or failed: ``service`` names it, ``status`` is its HTTP status (0 when
    the services are not reachable at all), and the message says why."""
    service: str
    status: int

    def __init__(self, service: str, status: int, message: str) -> None:
        ...

def setup_value(name: str) -> Any:
    """REQ-2225: the setup value ``name`` of a connector that serves a script; KeyError naming it
    when the host has not been given it."""
    ...

class secrets:

    @staticmethod
    def get(name: str) -> str:
        """The declared secret ``name``, read from the org's vault now."""
        ...

class settings:

    @staticmethod
    def get(name: str, default: Any=...) -> Any:
        """The source's setting ``name``; ``default`` when it declares none (else raised)."""
        ...

    @staticmethod
    def all() -> dict:
        ...

class cache:

    @staticmethod
    def get(key: str, default: Any=...) -> Any:
        """The cached value of ``key``, or ``default`` when none is cached."""
        ...

    @staticmethod
    def set(key: str, value: Any, ttl: float | None=...) -> None:
        """Cache ``value`` (JSON) under ``key`` for every worker of this source, for ``ttl``
        seconds (None: until deleted or replaced)."""
        ...

    @staticmethod
    def delete(key: str) -> None:
        ...

class vectors:
    """The source's own vector collection."""

    @staticmethod
    def embed(texts: list[str], model: str) -> list[list[float]]:
        ...

    @staticmethod
    def upsert(items: list[dict], model: str | None=...) -> int:
        """Add or replace ``items`` -- each ``{id, text | vector, metadata}``; a text is embedded
        by ``model``, one of the org's vector models."""
        ...

    @staticmethod
    def search(query: str | list[float], k: int=..., model: str | None=...) -> list[dict]:
        """The ``k`` items nearest ``query`` (a text, embedded by ``model``, or a vector):
        ``{id, score, text, metadata}``, nearest first."""
        ...

    @staticmethod
    def delete(ids: list[str]) -> None:
        ...

class Response:
    """What a fetch through Provisa answered."""
    status: int
    headers: dict
    content: bytes

    def __init__(self, answered: dict) -> None:
        ...

    @property
    def text(self) -> str:
        ...

    def json(self) -> Any:
        ...

class http:
    """A fetch Provisa makes for the code: to a host on the egress allow-list, recorded."""

    @staticmethod
    def get(url: str, headers: dict | None=..., timeout: float | None=...) -> Response:
        ...

    @staticmethod
    def post(url: str, body: str | bytes | None=..., json_body: Any=..., headers: dict | None=..., timeout: float | None=...) -> Response:
        ...

class llm:
    """Provisa's OpenAI-compatible LLM service; any OpenAI SDK also works at :meth:`base_url`."""

    @staticmethod
    def base_url() -> str:
        ...

    @staticmethod
    def chat(model: str, messages: list[dict], **params: Any) -> dict:
        """A chat completion: ``model`` is ``provisa:<role>`` or a model the org assigns."""
        ...

    @staticmethod
    def embeddings(model: str, input: str | list[str]) -> dict:
        ...

class dq:
    """Everything about the data quality of a dataset or a data product, as the meta domain shows it
    to the caller (REQ-2133)."""

    @staticmethod
    def dataset(name: str | int) -> dict:
        """A dataset's checker tables -- each one's description, contract, latest run (its outcome,
        when it ran, its failing checks) and the product it is assigned to -- and its profiles,
        with each one's latest run. ``name`` is the dataset's ``domain.table``, or its table id."""
        ...

    @staticmethod
    def product(product_id: str) -> dict:
        """A data product's promises, expectations, partner DQ and orphans, each with its latest
        run (REQ-1443)."""
        ...

def log(message: str, level: str=..., **fields: Any) -> None:
    """A log line, recorded against the request this call serves."""
    ...

def metric(name: str, value: float, kind: str=..., **attributes: Any) -> None:
    """A metric point (``counter``, ``gauge`` or ``histogram``), named ``code_source.<name>``."""
    ...

def audit(event: str, /, **fields: Any) -> None:
    """A structured event written to the org's trail, attached to the request this call serves:
    ``event`` names it (lower-case letters, digits, ``_`` and ``.``), ``fields`` are JSON values,
    at most 64 KB. A secret value does not belong in it, and is redacted if it is there."""
    ...

@dataclass(frozen=True)
class Filter:
    """A condition the read puts on a return column: ``column op value``. ``op`` is one of eq, ne,
    gt, gte, lt, lte, in (``value`` a list) and like (a SQL pattern)."""
    column: str
    op: str
    value: Any

@dataclass(frozen=True)
class Order:
    """A key the read orders its rows by: a return column, descending or not, its nulls first or
    last."""
    column: str
    descending: bool = ...
    nulls_first: bool = ...
_Row = TypeVar('_Row')

@dataclass(frozen=True)
class TableConnection:
    """REQ-2226: how a function reaches its table: a PostgreSQL connection, ``password`` a token
    good for this call only. ``sslmode`` is the TLS the listener requires of it."""
    host: str
    port: int
    user: str
    password: str
    database: str
    sslmode: str

@dataclass(frozen=True)
class TableRef(Generic[_Row]):
    """REQ-2226: a governed Provisa table given to a ``@command`` or ``@query``, ``TableRef[Row]``
    with ``Row`` the input columns it declares. The function reads the table as the calling role,
    so what the role cannot see it cannot read: run ``sql`` on a PostgreSQL connection made with
    ``connection``."""
    ref: str
    columns: tuple[str, ...]
    connection: TableConnection
    sql: str

@dataclass(frozen=True)
class ReadContext:
    """What a read asks of a ``@table`` or ``@stream`` function's rows, every part a hint
    Provisa applies again (see the module's docstring): its ``filters`` (each must hold), its
    ``order_by``, its ``limit`` (None: every row; its offset folded in), the ``columns`` it reads,
    and for a ``@loader`` the join's ``keys``, a hash of sparse keys (``{hash: {input: value}}``)
    to answer with ``{hash: [rows]}``."""
    filters: tuple[Filter, ...] = ...
    order_by: tuple[Order, ...] = ...
    limit: int | None = ...
    columns: tuple[str, ...] = ...
    keys: dict[str, dict[str, Any]] | None = ...

@dataclass(frozen=True)
class DiscoveredTable:
    """A generated table: its ``row`` type, whose fields are its columns, and its ``inputs``
    type, whose fields are its inputs (None: it takes none). ``loader`` (REQ-2100): its handler
    is given a join's keys in its read context and answers each key's rows; ``loader_batch`` the
    largest batch it takes (it implies ``loader``)."""
    name: str
    row: type
    inputs: type | None = ...
    description: str = ...
    loader: bool = ...
    loader_batch: int | None = ...

@dataclass(frozen=True)
class DiscoveredQuery:
    """A generated query: the ``table`` whose rows it returns (generated or hand-written), and
    its ``parameters`` type, whose fields are its parameters (None: it takes none)."""
    name: str
    table: str
    parameters: type | None = ...
    description: str = ...

@dataclass(frozen=True)
class DiscoveredCommand:
    """A generated command: its ``parameters`` type, and what it returns -- the rows of a
    ``table``, one value of the type ``returns``, or nothing when it names neither."""
    name: str
    parameters: type | None = ...
    returns: Any = ...
    table: str | None = ...
    description: str = ...

@dataclass(frozen=True)
class Discovered:
    """What ``@discover`` returns: its generated items, and the handler of each kind it
    generates."""
    tables: Sequence[DiscoveredTable] = ...
    queries: Sequence[DiscoveredQuery] = ...
    commands: Sequence[DiscoveredCommand] = ...
    table_handler: Callable[..., Any] | None = ...
    query_handler: Callable[..., Any] | None = ...
    command_handler: Callable[..., Any] | None = ...
