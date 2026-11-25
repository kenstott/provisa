# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A small Go source using the tags Go now shares with Python (REQ-2200, REQ-2100), read by the
reader, compiled by the pinned Go and served inside the worker's sandbox: a @loader answers a
join's keys in batches of its declared size, matched back to each joining set by hash as
provisa/codesource/table.py does; a function is given its read context; the lifecycle functions
run as calls."""

# Requirements: REQ-2200, REQ-2100, REQ-2221
from __future__ import annotations

import textwrap
from pathlib import Path

import pyarrow as pa
import pytest

from provisa.codesource import loader
from provisa.codesource import worker as w
from provisa.codesource.arrow_rows import row_schema, schema_text
from provisa.codesource.manifest import Manifest, Requirement
from provisa.golang.build import build_host
from provisa.golang.reader import ensure_reader, read_code
from provisa.golang.source import host_command
from provisa.runtime_deps.code_runtimes import RUNTIMES, CodeRuntimeResolver

pytestmark = [pytest.mark.integration]

PROXY = "https://proxy.golang.org"
EMPTY = {"filters": [], "order_by": [], "limit": None, "columns": [], "keys": None}

SOURCE = """
package scoring

import (
    "context"
    "fmt"
    "strings"

    ps "provisa_services"
)

type Score struct {
    CustomerID string  `json:"customer_id"`
    Tier       string  `json:"tier"`
    Score      float64 `json:"score"`
    Batch      int64   `json:"batch"`
}

type ScoreInputs struct {
    CustomerID *string `json:"customer_id"`
    Tier       *string `json:"tier"`
}

type Seen struct {
    Asked string `json:"asked"`
}

var calls int64
var region string

// Connect keeps the region it is set up with.
//
// @on-start
// @requires REGION
func Connect(ctx context.Context, values map[string]string) error {
    region = values["REGION"]
    return nil
}

// Status is the code's health.
//
// @health
func Status() (string, error) { return "ok in " + region, nil }

// ChurnScore scores the keys of a batch, two at a time.
//
// @table
// @loader 2
func ChurnScore(ctx context.Context, in ScoreInputs, read ps.ReadContext) (map[string][]Score, error) {
    calls++
    out := map[string][]Score{}
    for hash, key := range read.Keys {
        id, _ := key["customer_id"].(string)
        tier := "basic" // a key's tier left out takes the function's own default
        if given, ok := key["tier"].(string); ok {
            tier = given
        }
        out[hash] = []Score{{CustomerID: id, Tier: tier, Score: float64(len(id)), Batch: calls}}
    }
    return out, nil
}

// Hinted says what its read asked of it.
//
// @table
func Hinted(read ps.ReadContext) []Seen {
    parts := []string{strings.Join(read.Columns, ",")}
    for _, f := range read.Filters {
        parts = append(parts, fmt.Sprintf("%s %s %v", f.Column, f.Op, f.Value))
    }
    if read.Limit != nil {
        parts = append(parts, fmt.Sprintf("limit %d", *read.Limit))
    }
    return []Seen{{Asked: strings.Join(parts, "; ")}}
}
"""


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> tuple[Manifest, Path]:
    cache = tmp_path_factory.mktemp("cache")
    code = tmp_path_factory.mktemp("src") / "code"
    code.mkdir()
    (code / "go.mod").write_text("module example.com/scoring\n\ngo 1.23\n")
    (code / "scoring.go").write_text(textwrap.dedent(SOURCE))
    go = CodeRuntimeResolver(cache_root=cache).resolve("go")
    reader = ensure_reader(go, RUNTIMES["go"].version, cache, PROXY)
    manifest = read_code(
        reader=reader, go=go, code=code, package=".", gomodcache=cache / "mod", scratch=cache
    )
    binary = build_host(
        go=go,
        code=code,
        package=".",
        exports=[e.name for e in manifest.exports],
        gomodcache=cache / "mod",
        proxy=PROXY,
        dest=tmp_path_factory.mktemp("bin") / "host",
    )
    return manifest, binary


@pytest.fixture
def started(built):
    _manifest, binary = built
    running = w.Worker("go-decorators", "h", host_command(binary=binary, egress=[]))
    running.start()
    yield running
    running.stop()


def test_the_source_is_read_with_its_tags(built):
    manifest, _binary = built
    score = manifest.export("ChurnScore")
    assert (score.loader, score.loader_batch, score.takes) == (True, 2, ("inputs", "context"))
    assert manifest.export("Hinted").takes == ("context",)
    assert manifest.requires == (Requirement("REGION"),)
    assert {manifest.lifecycle(t).name for t in ("on-start", "health")} == {"Connect", "Status"}


def test_a_joins_keys_are_loaded_in_batches_and_matched_back_by_hash(built, started):
    """The path of provisa/codesource/table.py's _loaded: sparse keys, de-duplicated by hash, sent
    two to a batch, each joining set given the rows of its own key."""
    manifest, _binary = built
    score = manifest.export("ChurnScore")
    sets = [
        {"customer_id": "ada", "tier": "gold"},
        {"customer_id": "bo", "tier": None},
        {"customer_id": "ada", "tier": "gold"},
        {"customer_id": "cyd", "tier": "silver"},
    ]
    keys = [loader.sparse_key(s) for s in sets]
    hashes = [loader.key_hash(k) for k in keys]
    unique = dict(zip(hashes, keys, strict=True))
    batches = loader.chunks(unique, score.loader_batch)
    assert [len(b) for b in batches] == [2, 1]
    names = [p.name for p in score.parameters]
    schema = row_schema(manifest, score).append(pa.field(loader.KEY, pa.utf8()))
    answered = [
        row
        for batch in started.rows_each(
            "ChurnScore",
            [([w.ABSENT for _ in names], names) for _ in batches],
            schema=schema_text(schema),
            keyed=True,
            takes=score.takes,
            contexts=[{**EMPTY, "keys": b} for b in batches],
        )
        for row in batch
    ]
    rows = loader.assign("churn_score", keys, hashes, answered, set(unique))
    assert [(r["customer_id"], r["tier"], r["score"], r["batch"]) for r in rows] == [
        ("ada", "gold", 3.0, 1),
        ("bo", "basic", 2.0, 1),
        ("ada", "gold", 3.0, 1),
        ("cyd", "silver", 3.0, 2),
    ]


def test_the_read_context_reaches_the_function_in_the_sandbox(built, started):
    manifest, _binary = built
    hinted = manifest.export("Hinted")
    context = {
        "filters": [{"column": "asked", "op": "like", "value": "a%"}],
        "order_by": [],
        "limit": 3,
        "columns": ["asked"],
        "keys": None,
    }
    [[row]] = list(
        started.rows(
            "Hinted",
            "table",
            [],
            [],
            schema=schema_text(row_schema(manifest, hinted)),
            takes=hinted.takes,
            context=context,
        )
    )
    assert row == {"asked": "asked; asked like a%; limit 3"}


def test_the_lifecycle_functions_run_as_calls_in_the_sandbox(started):
    started.command("Connect", [{"REGION": "eu"}], ["values"])
    assert started.command("Status", [], []) == "ok in eu"
