# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Every Trino ``file`` catalog names its ``base-directory`` (REQ-1992, REQ-1730).

The pinned trino-file plugin (calcite engine-v0.131.1, tests/trino_plugins.py) refuses a catalog
without one: "Invalid configuration property base-directory: must not be null". A Parquet, CSV or
files source then had no catalog, and every read of it on a Trino host answered "Catalog ... not
found" (release 514's install-host smoke, test_source_parquet). The directory is the catalog's own,
under the engine's node data directory, so two catalogs never share the adapter's caches."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from provisa.core.catalog import create_catalog
from provisa.core.models import Source, SourceType
from provisa.federation.trino_connectors import TRINO_FILE_BASE_ROOT


def _source(stype: str) -> Source:
    path = "/var/lib/provisa/files/products.parquet" if stype != "files" else "/data/lake/**"
    return Source(id="smoke-parquet", type=SourceType(stype), path=path)


def _create_statement(source: Source, catalog_name: str | None = None) -> str:
    conn = MagicMock()
    create_catalog(conn, source, "", catalog_name=catalog_name)
    statements = [c.args[0] for c in conn.cursor.return_value.execute.call_args_list]
    create = [s for s in statements if s.startswith("CREATE CATALOG")]
    assert len(create) == 1, statements
    return create[0]


@pytest.mark.parametrize("stype", ["parquet", "csv", "files"])
def test_file_catalog_names_its_base_directory(stype):
    create = _create_statement(_source(stype))
    assert create.startswith("CREATE CATALOG smoke_parquet USING file WITH (")
    assert f"\"base-directory\" = '{TRINO_FILE_BASE_ROOT}/smoke_parquet'" in create


def test_each_catalog_has_its_own_base_directory():
    source = _source("parquet")
    one = _create_statement(source, catalog_name="org_a__smoke_parquet")
    two = _create_statement(source, catalog_name="org_b__smoke_parquet")
    assert f"\"base-directory\" = '{TRINO_FILE_BASE_ROOT}/org_a__smoke_parquet'" in one
    assert f"\"base-directory\" = '{TRINO_FILE_BASE_ROOT}/org_b__smoke_parquet'" in two


def test_base_directory_is_under_the_engine_node_data_dir():
    # node.data-dir=/data/trino on every Trino Provisa runs: trino/etc/node.properties (compose),
    # install-host.sh's rendered node.properties, k8s_provisioner.py's engine shard. The trinodb
    # image gives that directory to the uid Trino runs as, so the adapter can write under it.
    assert TRINO_FILE_BASE_ROOT.startswith("/data/trino/")
