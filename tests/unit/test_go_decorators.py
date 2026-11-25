# Copyright (c) 2026 Kenneth Stott
# Canary: c1f9296a-2a1f-418e-8b25-43dc1bb89700
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every tag a Python code source takes, read from a Go source's doc comments (REQ-2200,
REQ-1984): one test per tag, against one package that carries them all, and the host serving the
read context and a @loader's keyed call. The reader and host are compiled here by the pinned Go
toolchain; the sandboxed runs are tests/integration/test_go_decorators.py's."""

# Requirements: REQ-2200, REQ-2100, REQ-2221, REQ-2249, REQ-2226, REQ-2267
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from provisa.codesource import worker as w
from provisa.codesource.arrow_rows import row_schema, schema_text
from provisa.codesource.loader import KEY
from provisa.codesource.manifest import (
    Change,
    ManifestRefused,
    Manifest,
    Named,
    Requirement,
    Scalar,
    TableOf,
)
from provisa.golang.build import build_host
from provisa.golang.reader import ensure_reader, read_code
from provisa.runtime_deps.code_runtimes import RUNTIMES, CodeRuntimeResolver

pytestmark = pytest.mark.unit

PROXY = "https://proxy.golang.org"
EMPTY = {"filters": [], "order_by": [], "limit": None, "columns": [], "keys": None}

#: One package carrying every tag but @discover, each on a function of its own.
SOURCE = """
package tags

import (
    "context"
    "iter"
    "sort"
    "strings"

    ps "provisa_services"
)

type TableRef[R any] struct {
    Ref     string   `json:"ref"`
    Columns []string `json:"columns"`
}

type Order struct {
    ID     int64  `json:"id"`
    Region string `json:"region"`
}

type OrderInputs struct {
    Region *string `json:"region"`
}

type Score struct {
    CustomerID string  `json:"customer_id"`
    Score      float64 `json:"score"`
}

type ScoreInputs struct {
    CustomerID *string `json:"customer_id"`
}

type Seen struct {
    Columns string `json:"columns"`
    Filters string `json:"filters"`
    Order   string `json:"order"`
    Limit   *int64 `json:"limit"`
}

type Address struct {
    Street string `json:"street"`
}

type Changed struct {
    ID      int64 `json:"id"`
    Removed bool  `json:"removed"`
    Seq     int64 `json:"seq"`
}

// Orders of one region.
//
// @table
func Orders(ctx context.Context, in OrderInputs) ([]Order, error) {
    region := "eu"
    if in.Region != nil {
        region = *in.Region
    }
    return []Order{{ID: 1, Region: region}}, nil
}

// Placed streams placed orders.
//
// @stream
func Placed(ctx context.Context) iter.Seq2[Order, error] {
    return func(yield func(Order, error) bool) { yield(Order{ID: 1, Region: "eu"}, nil) }
}

// Cancel cancels one order.
//
// @command
func Cancel(id int64) (bool, error) { return true, nil }

// AddOrders inserts orders.
//
// @command
// @insert orders
func AddOrders(rows []Order) (int, error) { return len(rows), nil }

// SetOrders updates orders.
//
// @command
// @update orders
func SetOrders(rows []Order) (int, error) { return len(rows), nil }

// DropOrders deletes orders.
//
// @command
// @delete orders
func DropOrders(rows []Order) (int, error) { return len(rows), nil }

// ByRegion reads a governed table.
//
// @query
func ByRegion(orders TableRef[Order]) ([]Order, error) { return nil, nil }

// Clean parses raw addresses.
//
// @columns
func Clean(raw []string) ([]Address, error) {
    out := make([]Address, len(raw))
    for i, r := range raw {
        out[i] = Address{Street: strings.TrimSpace(r)}
    }
    return out, nil
}

// OrderChanges is what changed since the cursor.
//
// @change table=kept_orders
// @change key=id
// @change removed=removed
// @change cursor=seq
func OrderChanges(orders TableRef[Order], since *int64) ([]Changed, error) { return nil, nil }

// Connect.
//
// @on-start
func Connect(ctx context.Context, values map[string]string) error { return nil }

// Disconnect.
//
// @on-stop
func Disconnect() error { return nil }

// Status.
//
// @health
func Status() (string, error) { return "ok", nil }

// Fetch needs a region and a key.
//
// @table
// @requires REGION
// @requires API_KEY:secret
func Fetch(in OrderInputs) ([]Order, error) { return nil, nil }

// ChurnScore scores every key of a batch at once.
//
// @table
// @loader
func ChurnScore(in ScoreInputs, read ps.ReadContext) (map[string][]Score, error) {
    out := map[string][]Score{}
    if read.Keys == nil {
        id := "none"
        if in.CustomerID != nil {
            id = *in.CustomerID
        }
        out[""] = []Score{{CustomerID: id, Score: 1}}
        return out, nil
    }
    for hash, key := range read.Keys {
        id, _ := key["customer_id"].(string)
        out[hash] = []Score{{CustomerID: id, Score: float64(len(id))}, {CustomerID: id, Score: 0}}
    }
    return out, nil
}

// Batched takes its keys fifty at a time.
//
// @table
// @loader 50
func Batched(ctx context.Context, in ScoreInputs, read ps.ReadContext) map[string][]Score {
    return nil
}

// Hinted says what its read asked of it.
//
// @table
func Hinted(read ps.ReadContext) []Seen {
    var filters []string
    for _, f := range read.Filters {
        filters = append(filters, f.Column+" "+f.Op)
    }
    sort.Strings(filters)
    var order []string
    for _, o := range read.OrderBy {
        text := o.Column
        if o.Descending {
            text += " desc"
        }
        order = append(order, text)
    }
    return []Seen{{
        Columns: strings.Join(read.Columns, ","),
        Filters: strings.Join(filters, ","),
        Order:   strings.Join(order, ","),
        Limit:   read.Limit,
    }}
}
"""


@pytest.fixture(scope="module")
def toolchain(tmp_path_factory) -> tuple[Path, Path, Path]:
    """The pinned go, the compiled reader, and a module cache the reads and builds share."""
    go = CodeRuntimeResolver().resolve("go")
    cache = tmp_path_factory.mktemp("cache")
    return go, ensure_reader(go, RUNTIMES["go"].version, cache, PROXY), cache / "gomod"


def _code(root: Path, source: str) -> Path:
    code = root / "code"
    code.mkdir(parents=True)
    (code / "go.mod").write_text("module example.com/tags\n\ngo 1.23\n")
    (code / "tags.go").write_text(textwrap.dedent(source))
    return code


def _read(toolchain, code: Path, scratch: Path) -> Manifest:
    go, reader, gomodcache = toolchain
    return read_code(
        reader=reader, go=go, code=code, package=".", gomodcache=gomodcache, scratch=scratch
    )


@pytest.fixture(scope="module")
def code(tmp_path_factory) -> Path:
    return _code(tmp_path_factory.mktemp("src"), SOURCE)


@pytest.fixture(scope="module")
def manifest(toolchain, code, tmp_path_factory) -> Manifest:
    return _read(toolchain, code, tmp_path_factory.mktemp("read"))


# -- one test per tag ------------------------------------------------------------------------------


def test_table(manifest):
    orders = manifest.export("Orders")
    assert orders.kind == "table"
    assert orders.returns == Named("Order")
    assert orders.takes == ("inputs",)


def test_stream(manifest):
    assert manifest.export("Placed").kind == "stream"


def test_command(manifest):
    cancel = manifest.export("Cancel")
    assert (cancel.kind, cancel.returns, cancel.many) == ("command", Scalar("boolean"), False)


@pytest.mark.parametrize(
    ("export", "operation"),
    [("AddOrders", "insert"), ("SetOrders", "update"), ("DropOrders", "delete")],
)
def test_insert_update_delete(manifest, export, operation):
    found = manifest.export(export)
    assert (found.kind, found.binds) == ("command", ((operation, "orders"),))


def test_query(manifest):
    by_region = manifest.export("ByRegion")
    assert by_region.kind == "query"
    assert by_region.parameters[0].shape == TableOf(Named("Order"))


def test_columns(manifest):
    assert manifest.export("Clean").kind == "columns"


def test_change(manifest):
    changes = manifest.export("OrderChanges")
    assert changes.kind == "change"
    assert changes.change == Change("kept_orders", ("id",), "removed", "seq")


def test_on_start_and_on_stop(manifest):
    assert manifest.lifecycle("on-start").name == "Connect"
    assert manifest.lifecycle("on-stop").name == "Disconnect"


def test_health(manifest):
    assert manifest.lifecycle("health").name == "Status"


def test_requires(manifest):
    assert set(manifest.requires) == {Requirement("REGION"), Requirement("API_KEY", secret=True)}


def test_loader(manifest):
    """REQ-2100: a Go @loader takes the read context its keys come in, and returns each key's
    rows as a map of the key's hash to them: Go has no union of a list and a map."""
    loader = manifest.export("ChurnScore")
    assert (loader.kind, loader.loader, loader.loader_batch) == ("table", True, None)
    assert loader.takes == ("inputs", "context")
    assert loader.returns == Named("Score")


def test_loader_batch_size(manifest):
    batched = manifest.export("Batched")
    assert (batched.loader, batched.loader_batch) == (True, 50)
    assert batched.takes == ("inputs", "context")


def test_read_context(manifest):
    """REQ-2100: a function's ReadContext parameter, from provisa_services, is the read context;
    its own fields are never read as inputs."""
    hinted = manifest.export("Hinted")
    assert hinted.takes == ("context",)
    assert hinted.parameters == ()
    assert "ReadContext" not in manifest.types


def test_discover_is_refused_by_name(toolchain, tmp_path):
    """REQ-2267: @discover is read from a Python code source only in v0; a Go source carrying it
    is refused by name, never ignored."""
    code = _code(
        tmp_path,
        """
        package tags

        type Discovered struct{}

        // @discover
        func Feeds() Discovered { return Discovered{} }
        """,
    )
    with pytest.raises(ManifestRefused) as refused:
        _read(toolchain, code, tmp_path)
    assert refused.value.problems == ["Feeds: @discover is read from a Python code source only"]


def test_a_loader_without_the_read_context_is_refused_by_name(toolchain, tmp_path):
    code = _code(
        tmp_path,
        """
        package tags

        type Score struct {
            ID string `json:"id"`
        }

        // @table
        // @loader
        func Rows() map[string][]Score { return nil }
        """,
    )
    with pytest.raises(ManifestRefused) as refused:
        _read(toolchain, code, tmp_path)
    assert refused.value.problems == [
        "Rows: a @loader is given the join's keys in its read context, and Rows takes no "
        "ReadContext"
    ]


def test_a_table_returning_a_map_without_loader_is_refused(toolchain, tmp_path):
    """A map of each key's rows is a @loader's answer alone."""
    code = _code(
        tmp_path,
        """
        package tags

        type Score struct {
            ID string `json:"id"`
        }

        // @table
        func Rows() map[string][]Score { return nil }
        """,
    )
    with pytest.raises(ManifestRefused) as refused:
        _read(toolchain, code, tmp_path)
    assert refused.value.problems == [
        "Rows: @table returns a single value, not a collection of rows"
    ]


# -- served by the host ------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def worker(toolchain, code, manifest, tmp_path_factory):
    go, _reader, gomodcache = toolchain
    binary = build_host(
        go=go,
        code=code,
        package=".",
        exports=[e.name for e in manifest.exports],
        gomodcache=gomodcache,
        proxy=PROXY,
        dest=tmp_path_factory.mktemp("bin") / "host",
    )
    started = w.Worker(
        "go-tags",
        "h",
        w.HostCommand(
            argv=lambda address: [str(binary), address],
            env=w.private_environment(),
            cwd=binary.parent,
        ),
    )
    started.start()
    yield started
    started.stop()


def test_the_read_context_reaches_the_function(worker, manifest):
    hinted = manifest.export("Hinted")
    context = {
        "filters": [
            {"column": "columns", "op": "eq", "value": "x"},
            {"column": "limit", "op": "gt", "value": 2**60},
        ],
        "order_by": [{"column": "limit", "descending": True, "nulls_first": False}],
        "limit": 7,
        "columns": ["columns", "limit"],
        "keys": None,
    }
    rows = [
        r
        for b in worker.rows(
            "Hinted",
            "table",
            [],
            [],
            schema=schema_text(row_schema(manifest, hinted)),
            takes=hinted.takes,
            context=context,
        )
        for r in b
    ]
    assert rows == [
        {"columns": "columns,limit", "filters": "columns eq,limit gt", "order": "limit desc",
         "limit": 7}
    ]  # fmt: skip


def test_a_loader_call_answers_each_keys_rows_with_its_key(worker, manifest):
    """REQ-2100: a keyed call gives each batch its own context of keys; every row comes back with
    the hash of its key as $key."""
    import pyarrow as pa

    loader = manifest.export("ChurnScore")
    schema = row_schema(manifest, loader).append(pa.field(KEY, pa.utf8()))
    batches = [
        {"h1": {"customer_id": "ada"}, "h2": {"customer_id": "bo"}},
        {"h3": {"customer_id": "cyd"}},
    ]
    rows = [
        r
        for b in worker.rows_each(
            "ChurnScore",
            [([w.ABSENT], ["customer_id"]) for _ in batches],
            schema=schema_text(schema),
            keyed=True,
            takes=loader.takes,
            context=EMPTY,
            contexts=[{**EMPTY, "keys": keys} for keys in batches],
        )
        for r in b
    ]
    assert sorted((r[KEY], r["customer_id"], r["score"]) for r in rows) == [
        ("h1", "ada", 0.0), ("h1", "ada", 3.0), ("h2", "bo", 0.0), ("h2", "bo", 2.0),
        ("h3", "cyd", 0.0), ("h3", "cyd", 3.0),
    ]  # fmt: skip


def test_a_loader_read_with_its_own_inputs_is_its_rows(worker, manifest):
    """A read that is not a join's gives no keys: the loader's rows are every key's it answers."""
    loader = manifest.export("ChurnScore")
    rows = [
        r
        for b in worker.rows(
            "ChurnScore",
            "table",
            ["ada"],
            ["customer_id"],
            schema=schema_text(row_schema(manifest, loader)),
            takes=loader.takes,
            context=EMPTY,
        )
        for r in b
    ]
    assert rows == [{"customer_id": "ada", "score": 1.0}]
