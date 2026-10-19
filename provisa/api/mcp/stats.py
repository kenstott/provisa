# Copyright (c) 2026 Kenneth Stott
# Canary: 13d5cf8e-0f63-496f-935a-b640837f2473
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The statistics the MCP stats tools answer with (REQ-2090), over data already read.

Nothing here reads data: provisa/api/mcp/tools.py runs the caller's SQL through the governed
pipeline as the caller's role and hands the results here -- the rows a role may read, masked as
it reads them, or the aggregates the engine computed over them. Every answer names its method
and its n.
"""

# Requirements: REQ-2090

from __future__ import annotations

import math
from typing import Any, cast

import numpy as np
import pandas as pd

QUANTILES = (0.0, 0.05, 0.25, 0.5, 0.75, 0.95, 1.0)
TOP_VALUES = 5


def _series(df: pd.DataFrame, name: str) -> pd.Series:
    """One column of ``df``; a frame's column names are unique, so it is a Series."""
    return cast(pd.Series, df[name])


def _frame(df: pd.DataFrame, names: list[str]) -> pd.DataFrame:
    return cast(pd.DataFrame, df[names])


def _num(value: Any) -> float | None:
    """A JSON-safe number: None for NaN or infinity."""
    if value is None:
        return None
    f = float(value)
    return None if math.isnan(f) or math.isinf(f) else f


def frame(columns: list[str], rows: list) -> pd.DataFrame:
    """The rows the pipeline returned, as a frame; numbers read as numbers where every non-null
    value of a column is one."""
    df = pd.DataFrame([list(r) for r in rows], columns=pd.Index(columns))
    for col in columns:
        column = _series(df, col)
        if column.dtype == object:
            converted = pd.Series(pd.to_numeric(column, errors="coerce"), index=df.index)
            present = int(column.notna().sum())
            if present and int(converted.notna().sum()) == present:
                df[col] = converted
    return df


def is_numeric(series: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series)


def require_columns(df: pd.DataFrame, names: list[str]) -> None:
    missing = [n for n in names if n not in df.columns]
    if missing:
        raise ValueError(
            f"column(s) {', '.join(repr(m) for m in missing)} are not in the result; "
            f"the result has {', '.join(repr(c) for c in df.columns)}"
        )


def require_numeric(df: pd.DataFrame, names: list[str]) -> None:
    bad = [n for n in names if not is_numeric(_series(df, n))]
    if bad:
        raise ValueError(f"column(s) {', '.join(repr(b) for b in bad)} are not numeric")


# --- describe -------------------------------------------------------------------------------------


def describe_shape(df: pd.DataFrame, aggregates: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Per column: count, missing and distinct (and mean, sd, min, max for a number) from the
    engine's ``aggregates`` over the whole result; quantiles and top values from ``df``."""
    out: list[dict[str, Any]] = []
    for col in [str(c) for c in df.columns]:
        agg = aggregates[col]
        entry: dict[str, Any] = {
            "column": col,
            "count": int(agg["count"]),
            "missing": int(agg["missing"]),
            "distinct": int(agg["distinct"]),
        }
        series = _series(df, col).dropna()
        if is_numeric(_series(df, col)):
            entry.update(
                mean=_num(agg.get("mean")),
                sd=_num(agg.get("sd")),
                min=_num(agg.get("min")),
                max=_num(agg.get("max")),
            )
            if len(series):
                qs = series.astype(float).quantile(list(QUANTILES)).tolist()
                entry["quantiles"] = {f"p{int(q * 100)}": _num(v) for q, v in zip(QUANTILES, qs)}
        counts = series.astype(str).value_counts().head(TOP_VALUES)
        entry["top_values"] = [{"value": v, "count": int(c)} for v, c in counts.items()]
        out.append(entry)
    return out


# --- correlate ------------------------------------------------------------------------------------


def correlate(df: pd.DataFrame, columns: list[str], method: str) -> dict[str, Any]:
    if method not in ("pearson", "spearman"):
        raise ValueError("method is 'pearson' or 'spearman'")
    if len(columns) < 2:
        raise ValueError("correlate needs two or more columns")
    require_columns(df, columns)
    require_numeric(df, columns)
    used = _frame(df, columns).dropna().astype(float)
    if len(used) < 3:
        raise ValueError(f"correlate needs 3 or more complete rows; the result has {len(used)}")
    matrix = used.corr(method=method)
    return {
        "method": f"{method} correlation, rows with a missing value in any column left out",
        "n": len(used),
        "columns": columns,
        "matrix": [[_num(matrix.loc[a, b]) for b in columns] for a in columns],
    }


# --- regress --------------------------------------------------------------------------------------


def _design(df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    """The features as a design matrix: numbers as they are, a text column as one indicator per
    value but its first."""
    parts = []
    for f in features:
        column = _series(df, f)
        if is_numeric(column) or pd.api.types.is_bool_dtype(column):
            parts.append(_frame(df, [f]).astype(float))
        else:
            parts.append(
                pd.get_dummies(column.astype(str), prefix=f, drop_first=True).astype(float)
            )
    return pd.concat(parts, axis=1)


def _auc(y: np.ndarray, score: np.ndarray) -> float:
    """Area under the ROC curve, by the Mann-Whitney rank sum (ties share their mean rank)."""
    from scipy.stats import rankdata

    ranks = rankdata(score)
    pos = y == 1
    n_pos, n_neg = int(pos.sum()), int((~pos).sum())
    return float((ranks[pos].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def regress(df: pd.DataFrame, target: str, features: list[str], kind: str) -> dict[str, Any]:
    import statsmodels.api as sm

    if kind not in ("linear", "logistic"):
        raise ValueError("kind is 'linear' or 'logistic'")
    if not features:
        raise ValueError("regress needs one or more features")
    require_columns(df, [target, *features])
    used = _frame(df, [target, *features]).dropna()
    x = sm.add_constant(_design(used, features), has_constant="add")
    if len(used) <= x.shape[1]:
        raise ValueError(
            f"regress needs more complete rows than coefficients ({x.shape[1]}); "
            f"the result has {len(used)}"
        )
    if kind == "linear":
        require_numeric(df, [target])
        y = _series(used, target).astype(float)
        fit = sm.OLS(y, x).fit()
        quality = {"r_squared": _num(fit.rsquared), "adj_r_squared": _num(fit.rsquared_adj)}
        method = "ordinary least squares"
    else:
        values = sorted(_series(used, target).unique().tolist(), key=str)
        if len(values) != 2:
            raise ValueError(f"a logistic target has two values; {target!r} has {len(values)}")
        y = (_series(used, target) == values[1]).astype(float)
        fit = sm.Logit(y, x).fit(disp=0)
        quality = {
            "pseudo_r_squared": _num(fit.prsquared),
            "auc": _num(_auc(y.to_numpy(), np.asarray(fit.predict(x)))),
            "positive_value": values[1]
            if not isinstance(values[1], np.generic)
            else values[1].item(),
        }
        method = "logistic regression (maximum likelihood)"
    return {
        "method": method,
        "n": int(fit.nobs),
        "target": target,
        "coefficients": [
            {
                "term": term,
                "estimate": _num(fit.params[term]),
                "se": _num(fit.bse[term]),
                "p_value": _num(fit.pvalues[term]),
            }
            for term in fit.params.index
        ],
        **quality,
    }


# --- compare --------------------------------------------------------------------------------------


def compare(groups: list[dict[str, Any]], value: str, group: str) -> dict[str, Any]:
    """From each group's n, mean and sample variance (computed by the engine): Welch's t-test with
    Cohen's d for two groups, one-way ANOVA with eta squared for more."""
    from scipy import stats

    groups = [g for g in groups if g["n"] and g["n"] > 0]
    if len(groups) < 2:
        raise ValueError(f"compare needs two or more groups of {group!r} with a {value!r}")
    if any(g["n"] < 2 for g in groups):
        small = [str(g["group"]) for g in groups if g["n"] < 2]
        raise ValueError(f"every group needs 2 or more values; {', '.join(small)} has fewer")
    shown = [
        {
            "group": g["group"],
            "n": int(g["n"]),
            "mean": _num(g["mean"]),
            "sd": _num(math.sqrt(g["var"])),
        }
        for g in groups
    ]
    n_total = sum(g["n"] for g in groups)
    if len(groups) == 2:
        a, b = groups
        se2 = a["var"] / a["n"] + b["var"] / b["n"]
        t = (a["mean"] - b["mean"]) / math.sqrt(se2)
        df_ = se2**2 / (
            (a["var"] / a["n"]) ** 2 / (a["n"] - 1) + (b["var"] / b["n"]) ** 2 / (b["n"] - 1)
        )
        p = 2 * stats.t.sf(abs(t), df_)
        pooled = math.sqrt(
            ((a["n"] - 1) * a["var"] + (b["n"] - 1) * b["var"]) / (a["n"] + b["n"] - 2)
        )
        return {
            "method": "Welch's two-sample t-test",
            "n": n_total,
            "groups": shown,
            "statistic": {"t": _num(t), "df": _num(df_)},
            "p_value": _num(p),
            "effect_size": {"cohens_d": _num((a["mean"] - b["mean"]) / pooled)},
        }
    grand = sum(g["n"] * g["mean"] for g in groups) / n_total
    ss_between = sum(g["n"] * (g["mean"] - grand) ** 2 for g in groups)
    ss_within = sum((g["n"] - 1) * g["var"] for g in groups)
    df_b, df_w = len(groups) - 1, n_total - len(groups)
    f = (ss_between / df_b) / (ss_within / df_w)
    return {
        "method": "one-way ANOVA",
        "n": n_total,
        "groups": shown,
        "statistic": {"f": _num(f), "df_between": df_b, "df_within": df_w},
        "p_value": _num(stats.f.sf(f, df_b, df_w)),
        "effect_size": {"eta_squared": _num(ss_between / (ss_between + ss_within))},
    }


# --- forecast -------------------------------------------------------------------------------------


def forecast(
    df: pd.DataFrame,
    time: str,
    value: str,
    horizon: int,
    seasonal_periods: int | None = None,
    level: float = 0.95,
) -> dict[str, Any]:
    from statsmodels.tsa.exponential_smoothing.ets import ETSModel

    if horizon < 1:
        raise ValueError("horizon is 1 or more")
    require_columns(df, [time, value])
    require_numeric(df, [value])
    used = _frame(df, [time, value]).dropna().sort_values(by=time)
    trend = "add" if len(used) >= 10 else None
    seasonal = "add" if seasonal_periods else None
    needed = 2 * seasonal_periods if seasonal_periods else 4
    if len(used) < needed:
        raise ValueError(f"forecast needs {needed} or more points; the result has {len(used)}")
    y = pd.Series(_series(used, value).astype(float).to_numpy())  # ETS predictions are indexed
    # statsmodels types fit() as every shape it can return; this call returns the results.
    fit: Any = ETSModel(
        y, error="add", trend=trend, seasonal=seasonal, seasonal_periods=seasonal_periods
    ).fit(disp=False)
    frame_ = fit.get_prediction(start=len(y), end=len(y) + horizon - 1).summary_frame(
        alpha=1 - level
    )
    times = _future_times(_series(used, time), horizon)
    return {
        "method": (
            "exponential smoothing (ETS), additive error"
            + (", additive trend" if trend else "")
            + (f", additive seasonality every {seasonal_periods}" if seasonal else "")
        ),
        "n": len(used),
        "level": level,
        "forecast": [
            {
                "time": t,
                "value": _num(row["mean"]),
                "lower": _num(row["pi_lower"]),
                "upper": _num(row["pi_upper"]),
            }
            for t, (_, row) in zip(times, frame_.iterrows())
        ],
    }


def _future_times(times: pd.Series, horizon: int) -> list[Any]:
    """The next ``horizon`` times, a step apart, the step being the series' median step; for a
    time that is neither a date nor a number, the steps ahead (1, 2, ...)."""
    if pd.api.types.is_numeric_dtype(times):
        step = float(np.median(np.diff(times.to_numpy(dtype=float))))
        last = float(times.iloc[-1])
        return [_num(last + step * (i + 1)) for i in range(horizon)]
    parsed = pd.to_datetime(times, errors="coerce")
    if parsed.notna().all() and len(parsed) > 1:
        step = pd.Series(parsed.to_numpy()).diff().dropna().median()
        last = parsed.iloc[-1]
        return [(last + step * (i + 1)).isoformat() for i in range(horizon)]
    return [f"+{i + 1}" for i in range(horizon)]
