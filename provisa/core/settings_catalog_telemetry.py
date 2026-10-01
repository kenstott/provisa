# Copyright (c) 2026 Kenneth Stott
# Canary: d0a6e4b3-5f29-4c81-9a7d-12c8b3e5f460
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The telemetry pipeline's operator settings (REQ-1913, REQ-545) — the ``otel`` block.

Part of the settings declarations (``provisa/core/settings_catalog.py`` lists them all). The
defaults are the config model's own (``OtelConfig`` / ``SubsystemTracesConfig``), so the model and
the settings page cannot state different ones.
"""

# Requirements: REQ-1913, REQ-545, REQ-549, REQ-1432

from __future__ import annotations

from typing import Any

from provisa.core.models import OtelConfig, SubsystemTracesConfig
from provisa.core.settings_registry import Setting


def _model_default(field: str) -> Any:
    return OtelConfig.model_fields[field].default


def _otel(field: str, type_: str, effect: str, **more: Any) -> Setting:
    """``otel.<field>``, stated in the config file as ``observability.<field>``."""
    more.setdefault("config_path", ("observability", field))
    if not more.get("nullable"):
        more.setdefault("default", _model_default(field))
    return Setting(
        key=f"otel.{field}", card="telemetry", type=type_, effect=effect, req="REQ-545", **more
    )


# Fixed when the pipeline is set up at start: the sampler, the log handler, the batch processors,
# the compaction job's schedule and sizes.
_AT_START = "restart"

DECLARED: list[Setting] = [
    # Where telemetry is exported. A process attaches exporters for a changed endpoint without a
    # restart (provisa/api/otel_setup.apply_exporter_settings).
    _otel(
        "endpoint",
        "str",
        "live",
        env="OTEL_EXPORTER_OTLP_ENDPOINT",
        nullable=True,  # unset: spans are produced and dropped
        guard="confirm",  # traces can carry query text; this decides where they go
    ),
    _otel(
        "protocol",
        "enum",
        "live",
        env="OTEL_EXPORTER_OTLP_PROTOCOL",
        choices=("grpc", "http/protobuf"),  # REQ-549: declared, never read off the URL
    ),
    _otel("service_name", "str", "live", env="OTEL_SERVICE_NAME"),
    _otel("sample_rate", "float", _AT_START, min=0.0, max=1.0),
    _otel(
        "log_level",
        "enum",
        _AT_START,
        env="OTEL_LOG_LEVEL",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
    ),
    _otel("compact_cron", "str", _AT_START),
    _otel("compact_batch_size", "int", _AT_START, min=1, unit="rows"),
    _otel("compact_file_chunk", "int", _AT_START, min=1, unit="files"),
    _otel("compact_max_files_per_run", "int", _AT_START, min=1, unit="files"),
    _otel("ops_snapshot_retention_hours", "int", _AT_START, min=1, nullable=True, unit="hours"),
    _otel(
        "span_export_delay_millis",
        "int",
        _AT_START,
        env="OTEL_SPAN_EXPORT_DELAY_MILLIS",
        min=1,
        unit="milliseconds",
    ),
    _otel(
        "otlp2parquet_max_age_secs",
        "int",
        _AT_START,
        env="OTLP2PARQUET_MAX_AGE_SECS",
        min=1,
        unit="seconds",
    ),
    _otel("collector_batch_timeout_ms", "int", _AT_START, min=1, unit="milliseconds"),
    _otel("s3_endpoint", "str", _AT_START, env="PROVISA_OTEL_S3_ENDPOINT"),
    _otel(
        "support_endpoint",
        "str",
        _AT_START,
        env="PROVISA_SUPPORT_OTLP_ENDPOINT",
        nullable=True,
        guard="confirm",  # sends a copy of telemetry outside the deployment
    ),
    Setting(
        key="otel.support_redact_sql_literals",
        card="telemetry",
        type="bool",
        effect=_AT_START,
        req="REQ-546",
        config_path=("observability", "support_telemetry_filter", "redact_sql_literals"),
        default=True,  # the copy sent for support carries no query values unless told otherwise
        guard="confirm",
    ),
    Setting(
        key="otel.support_redact_attributes",
        card="telemetry",
        type="list",
        effect=_AT_START,
        req="REQ-546",
        config_path=("observability", "support_telemetry_filter", "redact_attributes"),
        default=(),
    ),
    # REQ-1432: one switch per subsystem; each gates an instrumentor installed at start.
    *(
        Setting(
            key=f"otel.subsystem_traces.{name}",
            card="telemetry",
            type="bool",
            effect=_AT_START,
            req="REQ-1432",
            config_path=("observability", "subsystem_traces", name),
            default=field.default,
        )
        for name, field in SubsystemTracesConfig.model_fields.items()
    ),
]
