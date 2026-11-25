# Copyright (c) 2026 Kenneth Stott
# Canary: df362483-9f34-4a35-a673-97ff11bbde96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A small Rust source using the tags Rust now shares with Python (REQ-2227, REQ-2100), read by
the reader, compiled with the pinned toolchain into its host and served inside the worker's
sandbox: a @loader answers a join's keys in batches of its declared size, matched back to each
joining set by hash as provisa/codesource/table.py does, and a read that is not a join's gets every
key's rows; a table and a stream are given their read context; the lifecycle functions run as
calls."""

# Requirements: REQ-2227, REQ-2100, REQ-2221
from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from provisa.codesource import loader
from provisa.codesource import worker as w
from provisa.codesource.arrow_rows import row_schema, schema_text
from provisa.codesource.install import DirectoryCode
from provisa.codesource.languages import LANGUAGES
from provisa.codesource.manifest import Requirement
from provisa.codesource.source import CodeConfig, Context, Prepared

pytestmark = [pytest.mark.integration]

#: The serde the fixture crate of tests/fixtures/rust_source pins, whose Cargo.lock this crate's is.
FIXTURE = Path(__file__).parent.parent / "fixtures" / "rust_source"
EMPTY = {"filters": [], "order_by": [], "limit": None, "columns": [], "keys": None}

CARGO_TOML = """[package]
name = "scoring"
version = "0.1.0"
edition = "2021"
publish = false

[lib]
path = "src/lib.rs"

[dependencies]
serde = { version = "=1.0.228", features = ["derive"] }
"""

SOURCE = """
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct Score {
    pub customer_id: String,
    pub tier: String,
    pub score: f64,
    pub batch: i64,
}

#[derive(Deserialize)]
pub struct ScoreInputs {
    pub customer_id: Option<String>,
    pub tier: Option<String>,
}

#[derive(Serialize)]
pub struct Seen {
    pub asked: String,
}

#[derive(Deserialize)]
pub struct Filter {
    pub column: String,
    pub op: String,
    pub value: String,
}

/// The read context, as much of it as this code reads.
#[derive(Deserialize)]
pub struct ReadContext {
    pub filters: Vec<Filter>,
    pub limit: Option<i64>,
    pub columns: Vec<String>,
    pub keys: Option<HashMap<String, HashMap<String, String>>>,
}

static CALLS: AtomicI64 = AtomicI64::new(0);
static REGION: Mutex<String> = Mutex::new(String::new());

/// Keeps the region it is set up with.
///
/// @on-start
/// @requires REGION
pub fn connect(values: HashMap<String, String>) -> Result<(), String> {
    let region = values.get("REGION").ok_or("no REGION is set up")?;
    *REGION.lock().unwrap() = region.clone();
    Ok(())
}

/// The code's health.
///
/// @health
pub fn status() -> Result<String, String> {
    Ok(format!("ok in {}", REGION.lock().unwrap()))
}

/// Scores the keys of a batch, two at a time.
///
/// @table
/// @loader 2
pub fn churn_score(
    inputs: ScoreInputs,
    context: ReadContext,
) -> Result<HashMap<String, Vec<Score>>, String> {
    let batch = CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    let keys = match context.keys {
        Some(keys) => keys,
        // A read that is not a join's: its own inputs, under one key.
        None => {
            let mut given = HashMap::new();
            if let Some(id) = inputs.customer_id {
                given.insert("customer_id".to_string(), id);
            }
            if let Some(tier) = inputs.tier {
                given.insert("tier".to_string(), tier);
            }
            HashMap::from([(String::new(), given)])
        }
    };
    let mut out = HashMap::new();
    for (hash, key) in keys {
        let id = key.get("customer_id").ok_or("a key has no customer_id")?.clone();
        // A key's tier left out takes the function's own default.
        let tier = match key.get("tier") {
            Some(tier) => tier.clone(),
            None => "basic".to_string(),
        };
        let score = id.len() as f64;
        out.insert(hash, vec![Score { customer_id: id, tier, score, batch }]);
    }
    Ok(out)
}

/// Says what its read asked of it.
///
/// @table
pub fn hinted(context: ReadContext) -> Vec<Seen> {
    let mut parts = vec![context.columns.join(",")];
    for f in &context.filters {
        parts.push(format!("{} {} {}", f.column, f.op, f.value));
    }
    if let Some(limit) = context.limit {
        parts.push(format!("limit {limit}"));
    }
    vec![Seen { asked: parts.join("; ") }]
}

#[derive(Deserialize)]
pub struct TickInputs {
    pub prefix: String,
}

/// As many ticks as the read keeps.
///
/// @stream
pub fn ticks(inputs: TickInputs, context: ReadContext) -> impl Iterator<Item = Seen> {
    let count = context.limit.ok_or("a tick stream is read with a limit").unwrap();
    (0..count).map(move |n| Seen { asked: format!("{}{n}", inputs.prefix) })
}
"""


@pytest.fixture(scope="module")
def joined(tmp_path_factory) -> Prepared:
    """The crate joined as a source: the Rust language's own prepare, as a source's save and the
    connector build run it."""
    code = tmp_path_factory.mktemp("src") / "scoring"
    (code / "src").mkdir(parents=True)
    (code / "Cargo.toml").write_text(CARGO_TOML)
    lock = (FIXTURE / "Cargo.lock").read_text()
    assert 'name = "orders"' in lock
    (code / "Cargo.lock").write_text(lock.replace('name = "orders"', 'name = "scoring"'))
    (code / "src" / "lib.rs").write_text(textwrap.dedent(SOURCE))
    cache = tmp_path_factory.mktemp("cache")
    config = CodeConfig("rust", "rust", {"kind": "directory", "path": str(code)}, None, None, None)
    ctx = Context(cache, [], [], cast(Any, None), "org")
    return LANGUAGES["rust"].prepare()("scoring-src", config, DirectoryCode(str(code)), None, ctx)


@pytest.fixture
def started(joined):
    running = joined.worker()
    running.start()
    yield running
    running.stop()


def test_the_source_is_read_with_its_tags(joined):
    manifest = joined.manifest
    score = manifest.export("churn_score")
    assert (score.loader, score.loader_batch, score.takes) == (True, 2, ("inputs", "context"))
    assert manifest.export("hinted").takes == ("context",)
    assert manifest.export("ticks").takes == ("inputs", "context")
    assert manifest.requires == (Requirement("REGION"),)
    assert {manifest.lifecycle(t).name for t in ("on-start", "health")} == {"connect", "status"}


def test_a_joins_keys_are_loaded_in_batches_and_matched_back_by_hash(joined, started):
    """The path of provisa/codesource/table.py's _loaded: sparse keys, de-duplicated by hash, sent
    two to a batch, each joining set given the rows of its own key."""
    manifest = joined.manifest
    score = manifest.export("churn_score")
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
            "churn_score",
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
    # A read of the loader that is not a join's is given no keys, and its rows are every key's.
    [[row]] = list(
        started.rows(
            "churn_score",
            "table",
            ["zed", w.ABSENT],
            names,
            schema=schema_text(row_schema(manifest, score)),
            takes=score.takes,
            context=EMPTY,
        )
    )
    assert row == {"customer_id": "zed", "tier": "basic", "score": 3.0, "batch": 3}


def test_a_keyed_call_of_a_function_answering_rows_fails_by_name(joined, started):
    manifest = joined.manifest
    hinted = manifest.export("hinted")
    schema = row_schema(manifest, hinted).append(pa.field(loader.KEY, pa.utf8()))
    with pytest.raises(w.CodeFailed, match="a loader call is answered with a map"):
        list(
            started.rows_each(
                "hinted",
                [((), ())],
                schema=schema_text(schema),
                keyed=True,
                takes=hinted.takes,
                contexts=[{**EMPTY, "keys": {"h": {}}}],
            )
        )


def test_the_read_context_reaches_a_table_and_a_stream_in_the_sandbox(joined, started):
    manifest = joined.manifest
    hinted = manifest.export("hinted")
    context = {
        "filters": [{"column": "asked", "op": "like", "value": "a%"}],
        "order_by": [],
        "limit": 3,
        "columns": ["asked"],
        "keys": None,
    }
    [[row]] = list(
        started.rows(
            "hinted",
            "table",
            [],
            [],
            schema=schema_text(row_schema(manifest, hinted)),
            takes=hinted.takes,
            context=context,
        )
    )
    assert row == {"asked": "asked; asked like a%; limit 3"}
    ticks = manifest.export("ticks")
    got = [
        r["asked"]
        for batch in started.rows(
            "ticks",
            "stream",
            ["t"],
            ["prefix"],
            schema=schema_text(row_schema(manifest, ticks)),
            takes=ticks.takes,
            context={**EMPTY, "limit": 3},
        )
        for r in batch
    ]
    assert got == ["t0", "t1", "t2"]


def test_the_lifecycle_functions_run_as_calls_in_the_sandbox(started):
    started.command("connect", [{"REGION": "eu"}], ["values"])
    assert started.command("status", [], []) == "ok in eu"
