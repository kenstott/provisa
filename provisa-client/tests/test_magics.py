# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""%%sql, %%graphql and %%cypher answer the cell as a DataFrame, bind it when named, and act as
the role --role names."""

import pandas as pd
import pytest

IPython = pytest.importorskip("IPython")  # the notebook extra; [dev] installs it

from IPython.core.interactiveshell import InteractiveShell  # noqa: E402

from provisa_client import magics  # noqa: E402


@pytest.fixture
def shell(monkeypatch):
    read: list = []

    def query(text, *, role=None, variables=None):
        read.append((text, role))
        return pd.DataFrame({"n": [len(read)]})

    monkeypatch.setattr(magics, "query", query)
    ip = InteractiveShell.instance()
    ip.run_line_magic("load_ext", "provisa_client.magics")
    ip.user_ns.pop("orders", None)
    return ip, read


@pytest.mark.parametrize("magic", ["sql", "graphql", "cypher"])
def test_a_cell_is_read_as_a_dataframe_and_bound_when_named(shell, magic):
    ip, read = shell
    df = ip.run_cell_magic(magic, "orders --role analyst_eu", "  SELECT 1\n")
    assert read == [("  SELECT 1\n", "analyst_eu")]
    assert ip.user_ns["orders"] is df
    assert df["n"].tolist() == [1]


def test_a_cell_with_no_name_or_role_is_its_own_result_as_the_connections_role(shell):
    ip, read = shell
    df = ip.run_cell_magic("sql", "", "SELECT 2")
    assert read == [("SELECT 2", None)]
    assert isinstance(df, pd.DataFrame) and "orders" not in ip.user_ns


def test_the_package_loads_the_magics_too(shell):
    ip, _ = shell
    ip.run_line_magic("load_ext", "provisa_client")
    assert "graphql" in ip.magics_manager.magics["cell"]
