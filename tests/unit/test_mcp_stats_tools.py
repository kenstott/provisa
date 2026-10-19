# Copyright (c) 2026 Kenneth Stott
# Canary: cddcc529-5f0c-4e05-9858-71e8602df591
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The MCP stats tools (REQ-2090): each one's math against known answers, and how each reads its
data -- through the one pipeline, as the caller's role, the rows a model reads capped and the cap
reported. The pipeline here is a stand-in that runs the SQL it is handed in DuckDB, so the SQL
each tool sends is SQL that runs; tests/integration/test_mcp_stats_governed.py runs the tools on a
real server, where masks and row rules apply."""

# Requirements: REQ-2090

from __future__ import annotations

import math
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from scipy import stats as sps

from provisa.api.mcp import stats, tools

# --- the math, against known answers --------------------------------------------------------------


def _rng():
    return np.random.default_rng(20261019)


def test_correlate_matches_pearson_and_spearman():
    r = _rng()
    x = r.normal(size=200)
    y = 0.6 * x + r.normal(size=200)
    z = r.normal(size=200)
    df = pd.DataFrame({"x": x, "y": y, "z": z})
    out = stats.correlate(df, ["x", "y", "z"], "pearson")
    assert out["n"] == 200
    assert out["matrix"][0][1] == pytest.approx(sps.pearsonr(x, y)[0])
    assert out["matrix"][1][1] == pytest.approx(1.0)
    rho = stats.correlate(df, ["x", "y"], "spearman")["matrix"][0][1]
    assert rho == pytest.approx(sps.spearmanr(x, y)[0])


def test_correlate_leaves_out_incomplete_rows_and_says_n():
    df = pd.DataFrame({"x": [1.0, 2.0, 3.0, None, 5.0], "y": [2.0, 4.0, 6.0, 8.0, None]})
    out = stats.correlate(df, ["x", "y"], "pearson")
    assert out["n"] == 3 and out["matrix"][0][1] == pytest.approx(1.0)


def test_linear_regression_matches_the_closed_form():
    r = _rng()
    x = r.normal(size=300)
    y = 3.0 + 2.0 * x + r.normal(scale=0.5, size=300)
    out = stats.regress(pd.DataFrame({"x": x, "y": y}), "y", ["x"], "linear")
    ref = sps.linregress(x, y)
    coef = {c["term"]: c for c in out["coefficients"]}
    assert out["n"] == 300 and out["method"] == "ordinary least squares"
    assert coef["x"]["estimate"] == pytest.approx(ref.slope)
    assert coef["const"]["estimate"] == pytest.approx(ref.intercept)
    assert coef["x"]["se"] == pytest.approx(ref.stderr)
    assert coef["x"]["p_value"] == pytest.approx(ref.pvalue, abs=1e-12)
    assert out["r_squared"] == pytest.approx(ref.rvalue**2)


def test_a_text_feature_becomes_one_indicator_per_value_but_its_first():
    df = pd.DataFrame({"y": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0], "k": list("aabbcca")})
    terms = [c["term"] for c in stats.regress(df, "y", ["k"], "linear")["coefficients"]]
    assert terms == ["const", "k_b", "k_c"]


def test_logistic_regression_matches_an_unpenalised_fit_and_its_auc():
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score

    r = _rng()
    x = r.normal(size=(400, 2))
    p = 1 / (1 + np.exp(-(0.5 + 1.5 * x[:, 0] - 1.0 * x[:, 1])))
    y = (r.uniform(size=400) < p).astype(int)
    df = pd.DataFrame({"a": x[:, 0], "b": x[:, 1], "won": np.where(y == 1, "yes", "no")})
    out = stats.regress(df, "won", ["a", "b"], "logistic")
    ref = LogisticRegression(penalty=None).fit(x, y)
    coef = {c["term"]: c["estimate"] for c in out["coefficients"]}
    assert out["positive_value"] == "yes" and out["n"] == 400
    assert coef["a"] == pytest.approx(ref.coef_[0][0], rel=1e-3)
    assert coef["b"] == pytest.approx(ref.coef_[0][1], rel=1e-3)
    assert coef["const"] == pytest.approx(ref.intercept_[0], rel=1e-3)
    assert out["auc"] == pytest.approx(roc_auc_score(y, ref.predict_proba(x)[:, 1]), abs=1e-6)


def test_a_logistic_target_has_two_values():
    df = pd.DataFrame({"y": ["a", "b", "c", "a", "b", "c"], "x": [1.0, 2, 3, 4, 5, 6]})
    with pytest.raises(ValueError, match="two values"):
        stats.regress(df, "y", ["x"], "logistic")


def _summary(name, values):
    v = np.asarray(values, dtype=float)
    return {"group": name, "n": len(v), "mean": v.mean(), "var": v.var(ddof=1)}


def test_two_groups_are_compared_by_welchs_t_test():
    r = _rng()
    a, b = r.normal(10, 2, 40), r.normal(11, 3, 55)
    out = stats.compare([_summary("a", a), _summary("b", b)], "v", "g")
    ref = sps.ttest_ind(a, b, equal_var=False)
    assert out["method"] == "Welch's two-sample t-test" and out["n"] == 95
    assert out["statistic"]["t"] == pytest.approx(ref.statistic)
    assert out["p_value"] == pytest.approx(ref.pvalue)
    pooled = math.sqrt((39 * a.var(ddof=1) + 54 * b.var(ddof=1)) / 93)
    assert out["effect_size"]["cohens_d"] == pytest.approx((a.mean() - b.mean()) / pooled)


def test_three_groups_are_compared_by_one_way_anova():
    r = _rng()
    groups = [r.normal(m, 1, 30) for m in (5, 5.5, 6)]
    out = stats.compare([_summary(i, g) for i, g in enumerate(groups)], "v", "g")
    ref = sps.f_oneway(*groups)
    assert out["method"] == "one-way ANOVA" and out["n"] == 90
    assert out["statistic"]["f"] == pytest.approx(ref.statistic)
    assert out["p_value"] == pytest.approx(ref.pvalue)
    every = np.concatenate(groups)
    ss_total = ((every - every.mean()) ** 2).sum()
    ss_between = sum(len(g) * (g.mean() - every.mean()) ** 2 for g in groups)
    assert out["effect_size"]["eta_squared"] == pytest.approx(ss_between / ss_total)


def test_a_group_of_one_is_refused_by_name():
    with pytest.raises(ValueError, match="solo"):
        stats.compare([_summary("solo", [1.0]), _summary("b", [1.0, 2.0, 3.0])], "v", "g")


def test_a_linear_series_is_forecast_along_its_line_with_intervals():
    t = pd.date_range("2026-01-01", periods=36, freq="D")
    y = 5.0 + 2.0 * np.arange(36) + _rng().normal(scale=0.3, size=36)
    out = stats.forecast(pd.DataFrame({"day": t, "v": y}), "day", "v", 3)
    assert out["n"] == 36 and "additive trend" in out["method"] and out["level"] == 0.95
    assert [p["time"] for p in out["forecast"]] == [
        "2026-02-06T00:00:00",
        "2026-02-07T00:00:00",
        "2026-02-08T00:00:00",
    ]
    for i, point in enumerate(out["forecast"]):
        expected = 5.0 + 2.0 * (36 + i)
        assert point["value"] == pytest.approx(expected, abs=1.5)
        assert point["lower"] < point["value"] < point["upper"]


def test_a_seasonal_series_is_forecast_with_its_season():
    season = np.array([10.0, 20.0, 15.0, 5.0])
    y = np.tile(season, 8) + _rng().normal(scale=0.1, size=32)
    out = stats.forecast(pd.DataFrame({"q": np.arange(32), "v": y}), "q", "v", 4, 4)
    assert "seasonality every 4" in out["method"]
    assert [p["time"] for p in out["forecast"]] == [32.0, 33.0, 34.0, 35.0]
    assert [round(p["value"]) for p in out["forecast"]] == [10, 20, 15, 5]


def test_describe_takes_counts_from_the_engine_and_quantiles_from_the_rows():
    df = pd.DataFrame({"x": [1.0, 2.0, 3.0, 4.0], "k": ["a", "a", "b", None]})
    aggregates = {
        "x": {"count": 4, "missing": 0, "distinct": 4, "mean": 2.5, "sd": 1.29, "min": 1, "max": 4},
        "k": {"count": 3, "missing": 1, "distinct": 2},
    }
    x, k = stats.describe_shape(df, aggregates)
    assert x["mean"] == 2.5 and x["quantiles"]["p50"] == 2.5 and x["quantiles"]["p0"] == 1.0
    assert k["missing"] == 1 and k["top_values"][0] == {"value": "a", "count": 2}
    assert "mean" not in k


# --- how each tool reads its data -----------------------------------------------------------------


class _DuckPipeline:
    """The pipeline's stand-in: runs each statement in DuckDB over ``sales`` and records it."""

    def __init__(self, frame: pd.DataFrame) -> None:
        import duckdb

        self.conn = duckdb.connect()
        self.conn.register("sales", frame)
        self.frame = frame
        self.sent: list[tuple[str, str]] = []

    async def __call__(self, sql, role, state, **kw):
        self.sent.append((sql, role))
        cur = self.conn.execute(sql)
        return SimpleNamespace(
            rows=[tuple(r) for r in cur.fetchall()],
            column_names=[d[0] for d in cur.description],
            redirect=None,
        )


_STATE = SimpleNamespace(contexts={"analyst": object()})


@pytest.fixture
def pipeline(monkeypatch):
    r = _rng()
    region = np.where(np.arange(120) % 2 == 0, "east", "west")
    amount = np.where(region == "east", 100.0, 110.0) + r.normal(scale=5, size=120)
    frame = pd.DataFrame(
        {
            "id": np.arange(120),
            "region": region,
            "amount": amount,
            "qty": r.integers(1, 9, size=120).astype(float),
        }
    )
    stand_in = _DuckPipeline(frame)
    monkeypatch.setattr("provisa.pgwire._pipeline.execute_sql_batch", stand_in)
    monkeypatch.setattr(tools, "_stats_max_rows", lambda: 50)
    return stand_in


async def test_every_statement_runs_as_the_callers_role(pipeline):
    await tools.stats_describe(_STATE, "analyst", "SELECT * FROM sales;")
    assert pipeline.sent and {role for _, role in pipeline.sent} == {"analyst"}
    assert all("SELECT * FROM sales" in sql for sql, _ in pipeline.sent)


async def test_describe_counts_every_row_and_reports_the_rows_it_read(pipeline):
    out = await tools.stats_describe(_STATE, "analyst", "SELECT region, amount FROM sales")
    assert out["n"] == 120
    assert out["rows_read"] == 50 and out["row_cap"] == 50 and out["truncated"] is True
    amount = next(c for c in out["columns"] if c["column"] == "amount")
    assert amount["count"] == 120
    assert amount["mean"] == pytest.approx(pipeline.frame["amount"].mean())
    assert amount["sd"] == pytest.approx(pipeline.frame["amount"].std(ddof=1))
    region = next(c for c in out["columns"] if c["column"] == "region")
    assert region["distinct"] == 2 and "mean" not in region


async def test_compare_asks_the_engine_for_each_groups_summary(pipeline):
    out = await tools.stats_compare(_STATE, "analyst", "SELECT * FROM sales", "amount", "region")
    f = pipeline.frame
    east, west = f[f.region == "east"].amount, f[f.region == "west"].amount
    assert out["n"] == 120 and out["groups_read"] == 2
    assert out["p_value"] == pytest.approx(sps.ttest_ind(east, west, equal_var=False).pvalue)
    assert all("GROUP BY" in sql for sql, _ in pipeline.sent)


async def test_a_model_reads_at_most_the_cap_and_says_so(pipeline):
    out = await tools.stats_regress(
        _STATE, "analyst", "SELECT * FROM sales ORDER BY id", "amount", ["qty"], "linear"
    )
    assert out["n"] == 50 and out["truncated"] is True and out["row_cap"] == 50
    assert any("LIMIT 51" in sql for sql, _ in pipeline.sent)


async def test_a_result_within_the_cap_is_not_truncated(pipeline):
    out = await tools.stats_correlate(
        _STATE, "analyst", "SELECT amount, qty FROM sales WHERE id < 30", ["amount", "qty"]
    )
    assert out["n"] == 30 and out["rows_read"] == 30 and out["truncated"] is False


async def test_a_column_not_in_the_result_is_refused_by_name(pipeline):
    with pytest.raises(ValueError, match="'nope'"):
        await tools.stats_correlate(_STATE, "analyst", "SELECT * FROM sales", ["amount", "nope"])


async def test_sql_the_pipeline_refuses_fails_as_run_sql_fails(pipeline):
    import duckdb

    with pytest.raises(duckdb.Error):
        await tools.stats_describe(_STATE, "analyst", "SELEC nothing")


async def test_a_role_the_caller_cannot_act_as_is_refused():
    with pytest.raises(PermissionError):
        await tools.stats_describe(_STATE, "stranger", "SELECT 1")
    with pytest.raises(ValueError, match="role is required"):
        await tools.stats_compare(_STATE, "", "SELECT 1", "a", "b")
