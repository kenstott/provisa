# Copyright (c) 2026 Kenneth Stott
# Canary: 238a8269-fca6-4088-8003-f5b301b921c4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Amazon Athena direct source driver (REQ-1981).

Athena is a named source reached through its own API (PyAthena, over boto3): a statement on its
tables alone runs whole in Athena, its filters, columns and limits with it, so the bytes Athena
scans and bills are the statement's own. Every other engine reads a replica it lands.

The connection is the source's own (``database`` = the Glue database queried by default;
``federation_hints`` carries ``region``, ``workgroup``, ``s3_staging_dir``, ``catalog`` and
``role_arn``). The access key pair is the source's ``username``/``password``, each resolved at use;
with none, boto3's own credential chain is used (an instance role, the environment, a profile), as
for the mail service's SES (``core/mail.py``). A role ARN, when given, is assumed for every call.

A result is read as Arrow (REQ-2194) through PyAthena's Arrow cursor, which reads the result file
Athena wrote to the query output location: the source's credentials must read that location, so
the connection that validates a source at setup runs one statement and reads its result, and a
source whose credentials cannot is refused naming the location. A statement whose result metadata
has no column (DDL, DML) has no result set and its file is not read; a result with columns and no
row is an empty Arrow table of those columns, each typed as its values would be.
"""

# Requirements: REQ-1981, REQ-2194

from __future__ import annotations

import asyncio
from typing import Any, cast

import pyarrow as pa
import pyathena  # the driver's dependency: imported here, so its absence means no driver
from pyathena.arrow.converter import DefaultArrowTypeConverter
from pyathena.arrow.cursor import ArrowCursor
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.converter import _to_binary, _to_datetime_with_tz  # pyright: ignore[reportPrivateUsage]
from pyathena.error import OperationalError
from pyathena.model import AthenaQueryExecution

from provisa.executor.drivers.arrow_result import source_arrow_result
from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult

#: The hints a source may set, beside its region.
_OPTIONAL = ("workgroup", "s3_staging_dir", "catalog", "role_arn")


def _to_pyformat(sql: str, params: list | None) -> tuple[str, dict[str, Any] | None]:
    """``$N`` placeholders as PyAthena's ``%(pN)s`` (highest index first, so ``$1`` does not
    corrupt ``$10``); a literal ``%`` is doubled, since the driver formats the statement."""
    if not params:
        return sql, None
    out = sql.replace("%", "%%")
    bind: dict[str, Any] = {}
    for i in range(len(params), 0, -1):
        out = out.replace(f"${i}", f"%(p{i})s")
        bind[f"p{i}"] = params[i - 1]
    return out, bind


#: Athena types the result file's text is parsed as natively, so each value is the one PyAthena's
#: row cursor gave (``date``, a naive ``datetime``, ``time``). PyAthena's own Arrow converter reads
#: a date as a timestamp and a time and a decimal as text. A composite (array, map, row) and json
#: stay text, as the IR types them (``core.ir_types``: Athena composites are text).
_NATIVE: dict[str, pa.DataType] = {
    "date": pa.date32(),
    "timestamp": pa.timestamp("us"),
    "time": pa.time64("us"),
    # Kept as its text for _FROM_TEXT, which reads its zone; never inferred by the CSV reader.
    "timestamp with time zone": pa.string(),
}


#: Athena types whose text in the result file Arrow's CSV reader cannot parse: each such column
#: is converted from its text by PyAthena's own converter for the type, as its rows were.
def _zoned(text: str | None) -> Any:  # noqa: ANN401 — an aware datetime or None
    """A ``timestamp with time zone`` from its text. PyAthena writes a null text column as ``""``
    when the result has a varbinary column (``AthenaArrowResultSet._read_csv``); no timestamp's
    text is empty, so an empty text is that null."""
    return _to_datetime_with_tz(text) if text else None


_FROM_TEXT: dict[str, tuple[Any, pa.DataType]] = {
    "varbinary": (_to_binary, pa.binary()),
    "timestamp with time zone": (_zoned, pa.timestamp("us", tz="UTC")),
}


class _ArrowTypes(DefaultArrowTypeConverter):
    """The Arrow type each Athena column's text is read as: a decimal at its own precision and
    scale, the types in ``_NATIVE`` as themselves, the rest as PyAthena reads them."""

    def get_dtype(self, type_: str, precision: int = 0, scale: int = 0) -> Any:  # noqa: ANN401
        if type_ == "decimal":
            return pa.decimal128(precision, scale)
        return _NATIVE.get(type_) or super().get_dtype(type_, precision, scale)


class _ArrowResultSet(AthenaArrowResultSet):
    """PyAthena's Arrow result set, reading the result file only for a statement with a result set.

    A statement whose result metadata has no column has none: Athena writes an Iceberg
    ``DROP TABLE``'s result file as one empty line, which Arrow's CSV reader cannot read. A result
    of no row (a SELECT's header alone, an empty file, a listing with no entry) is an empty table
    of the metadata's columns, each of the type its values are read as."""

    def _read_csv(self) -> pa.Table:
        description = self.description
        if not description:
            return pa.table({})
        table = super()._read_csv()
        if table.num_rows:
            return table
        types = self.column_types
        # A column with no Arrow type in the converter is read from its text, so with no value
        # to infer from, it is that text (REQ-2194: composites and json are text in the IR).
        schema = pa.schema([(d[0], types.get(d[0], pa.string())) for d in description])
        return schema.empty_table()


class _ArrowCursor(ArrowCursor):
    """PyAthena's Arrow cursor, its result read by :class:`_ArrowResultSet`."""

    def execute(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, operation: str, parameters: dict[str, Any] | None = None
    ) -> _ArrowCursor:
        self._reset_state()
        self.query_id = self._execute(operation, parameters=parameters)
        execution = cast(
            AthenaQueryExecution, self._poll(self.query_id)
        )  # a query, not a calculation
        if execution.state != AthenaQueryExecution.STATE_SUCCEEDED:
            raise OperationalError(execution.state_change_reason)
        self.result_set = _ArrowResultSet(
            connection=self._connection,
            converter=self._converter,
            query_execution=execution,
            arraysize=self.arraysize,
            retry_config=self._retry_config,
            connect_timeout=self._connect_timeout,
            request_timeout=self._request_timeout,
        )
        return self


def _arrow_table(cursor: Any) -> pa.Table:  # noqa: ANN401 — a PyAthena ArrowCursor
    """The cursor's result as Arrow, each ``_FROM_TEXT`` column converted from its text."""
    table = cursor.as_arrow()
    for name, type_, *_ in cursor.description:
        if type_ in _FROM_TEXT:
            convert, arrow_type = _FROM_TEXT[type_]
            i = table.schema.get_field_index(name)
            values = [convert(v) for v in table.column(i).to_pylist()]
            table = table.set_column(i, name, pa.array(values, arrow_type))
    return table


def _check_output_location(conn: Any) -> None:  # noqa: ANN401 — a PyAthena connection
    """Refuse a source whose credentials cannot read its query output location (REQ-2194): one
    statement runs and its result file is read, as every result is."""
    cursor = conn.cursor(_ArrowCursor, converter=_ArrowTypes())
    try:
        cursor.execute("SELECT 1")
    except OperationalError as exc:
        if cursor.query_id is None:
            raise
        execution = conn.client.get_query_execution(QueryExecutionId=cursor.query_id)
        execution = execution["QueryExecution"]
        if execution["Status"]["State"] != "SUCCEEDED":
            raise
        location = execution["ResultConfiguration"]["OutputLocation"]
        raise PermissionError(
            f"athena source: its credentials cannot read the query output location "
            f"{location}, where Athena writes each result: {exc}"
        ) from exc
    finally:
        cursor.close()


class AthenaDriver(DirectDriver):
    def __init__(self) -> None:
        self._conn: Any = None
        self._extra: dict[str, str] = {}

    def configure(self, extra: dict[str, str]) -> None:
        """The source's ``region`` (required), and its optional ``workgroup``,
        ``s3_staging_dir`` (where Athena writes results, unless the workgroup sets it),
        ``catalog`` (default ``AwsDataCatalog``) and ``role_arn``."""
        self._extra = dict(extra)

    async def connect(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        host: str,  # pyright: ignore[reportUnusedParameter]
        port: int,  # pyright: ignore[reportUnusedParameter]
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,  # pyright: ignore[reportUnusedParameter]
        max_pool: int = 5,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        region = self._extra.get("region")
        if not region:
            raise ValueError("athena source requires its region (federation hint `region`)")
        if bool(user) != bool(password):
            raise ValueError(
                "athena source: an access key ID and its secret are given together, or neither "
                "(for the AWS credential chain)"
            )
        kwargs: dict[str, Any] = {"region_name": region, "schema_name": database or "default"}
        for key, arg in zip(
            _OPTIONAL, ("work_group", "s3_staging_dir", "catalog_name", "role_arn")
        ):
            if self._extra.get(key):
                kwargs[arg] = self._extra[key]
        if user:
            kwargs["aws_access_key_id"] = user
            kwargs["aws_secret_access_key"] = password

        def _open() -> Any:
            conn = pyathena.connect(**kwargs)
            try:
                _check_output_location(conn)
            except Exception:
                conn.close()
                raise
            return conn

        self._conn = await asyncio.to_thread(_open)

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        statement, bind = _to_pyformat(sql, params)

        def _run() -> QueryResult:
            cursor = self._conn.cursor(_ArrowCursor, converter=_ArrowTypes())
            try:
                cursor.execute(statement, bind)
                if not cursor.description:  # no result set (DDL, DML)
                    count = cursor.rowcount
                    return QueryResult(
                        rows=[], column_names=[], rowcount=count if count >= 0 else None
                    )
                return source_arrow_result(_arrow_table(cursor))
            finally:
                cursor.close()

        return await asyncio.to_thread(_run)

    async def close(self) -> None:
        if self._conn is not None:
            await asyncio.to_thread(self._conn.close)
            self._conn = None

    @property
    def is_connected(self) -> bool:
        return self._conn is not None
