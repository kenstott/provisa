# Copyright (c) 2026 Kenneth Stott
# Canary: 7e19b4d2-c853-4a6f-8d01-3f5a2c9e7b16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The declarations of the operator settings (REQ-1913).

One :class:`Setting` per setting, and this is the only place its environment variable, config
path, default, range and effect are stated. A setting is declared here when its reader resolves
through ``provisa.core.settings_registry.value`` — a declaration whose reader still reads the
environment itself would show an editable field that changes nothing.
"""

# Requirements: REQ-1913

from __future__ import annotations

from provisa.core.connection_loop import DEFAULT_BACKGROUND_WORKERS
from provisa.core.limits import REQUEST_TRANSPORTS
from provisa.core.models import ControlPlaneConfig
from provisa.core.settings_registry import Setting
from provisa.otel_compat import TRACE_DETAILS


def _flight_stream_default() -> int:
    """REQ-1905: the host's Flight budget divided among the launch's workers, never below 2 —
    computed in provisa/core/limits.py, not restated here."""
    from provisa.core.limits import flight_stream_default_limit

    return flight_stream_default_limit()


DECLARED: list[Setting] = [
    # --- Server limits ---------------------------------------------------------------------------
    Setting(
        key="limits.default_row_limit",
        card="limits",
        type="int",
        effect="live",
        req="REQ-005",  # the cap on rows returned when the caller supplies no LIMIT; 100
        env="PROVISA_DEFAULT_ROW_LIMIT",
        config_path=("server", "limits", "default_row_limit"),
        default=100,
        min=1,
        unit="rows",
    ),
    Setting(
        key="limits.engine_query_timeout",
        card="limits",
        type="int",
        effect="live",  # read when a federated query is submitted
        req="REQ-1913",
        env="PROVISA_ENGINE_QUERY_TIMEOUT",
        config_path=("server", "limits", "engine_query_timeout"),
        default=120,
        min=1,
        unit="seconds",
    ),
    Setting(
        key="limits.retry_budget_secs",
        card="limits",
        type="float",
        effect="live",  # read when a query is executed; 0 turns retries off
        req="REQ-1913",
        env="PROVISA_RETRY_BUDGET_SECS",
        config_path=("server", "limits", "retry_budget_secs"),
        default=30.0,
        min=0,
        unit="seconds",
    ),
    # REQ-1905: the request timeout is set per transport. `request_timeout` is the default every
    # transport uses unless it has a value of its own in `request_timeouts`.
    Setting(
        key="limits.request_timeout",
        card="limits",
        type="float",
        effect="live",  # read when a request's deadline is bound
        req="REQ-1905",
        env="PROVISA_REQUEST_TIMEOUT",
        config_path=("server", "limits", "request_timeout"),
        default=60.0,
        min=0.001,
        unit="seconds",
    ),
    Setting(
        key="limits.request_timeouts",
        card="limits",
        type="map",
        effect="live",
        req="REQ-1905",
        config_path=("server", "limits", "request_timeouts"),
        map_keys=REQUEST_TRANSPORTS,
        # REQ-1905, THE SHIPPED VALUES — design-mandated defaults, not fallbacks: Flight is
        # expected to carry large data transfers (3600 s) and pgwire carries BI workloads
        # (300 s). Every other transport ships with no value of its own and uses
        # `limits.request_timeout`. An operator can change or clear either like any other.
        default={"flight": 3600.0, "pgwire": 300.0},
        min=0.001,
        unit="seconds",
    ),
    Setting(
        key="sampling.default_sample_size",
        card="limits",
        type="int",
        effect="live",
        req="REQ-165",
        env="PROVISA_SAMPLE_SIZE",
        default=10000,
        min=1,
        unit="rows",
    ),
    Setting(
        key="relationships.auto_track_fk",
        card="engine",
        type="bool",
        effect="live",
        req="REQ-165",
        env="PROVISA_AUTO_TRACK_FK",
        default=True,
    ),
    Setting(
        key="grpc.max_message_bytes",
        card="limits",
        type="int",
        effect="restart",  # a gRPC server option, set when the server is built
        req="REQ-1899",  # 32 MB: covers a 65,536-row batch of a wide table
        env="GRPC_MAX_MESSAGE_BYTES",
        config_path=("server", "grpc_max_message_bytes"),
        default=32 * 1024 * 1024,
        min=1,
        unit="bytes",
    ),
    Setting(
        key="bolt.recv_timeout",
        card="limits",
        type="int",
        effect="live",  # the hint a Bolt client is given at HELLO: how long to wait for a reply
        req="REQ-1913",
        env="PROVISA_BOLT_RECV_TIMEOUT",
        default=120,
        min=1,
        unit="seconds",
    ),
    Setting(
        key="engine.ready_timeout",
        card="engine",
        type="float",
        effect="live",  # how long catalog registration and introspection wait for the engine
        req="REQ-1913",
        env="PROVISA_TRINO_READY_TIMEOUT",
        default=120.0,
        min=1,
        unit="seconds",
    ),
    Setting(
        key="ui.proxy_timeout",
        card="limits",
        type="float",
        # Read by the UI server — its own process, with no control plane — which asks the API for
        # this setting and holds the answer for the snapshot TTL. PROVISA_UI_PROXY_TIMEOUT is that
        # process's variable and is applied there, so it is not an `env` of this declaration.
        effect="live",
        req="REQ-1448",  # 480 s: it must outlast the engine wake a first query may wait on
        default=480.0,
        min=1,
        unit="seconds",
    ),
    # --- Concurrency -----------------------------------------------------------------------------
    Setting(
        key="concurrency.background_workers",
        card="concurrency",
        type="int",
        effect="restart",  # sizes the background pool before its first submission (REQ-1882)
        req="REQ-1882",
        config_path=("server", "background_workers"),
        default=DEFAULT_BACKGROUND_WORKERS,
        min=1,
        unit="threads",
    ),
    # REQ-1882: the per-worker bounds of the request-thread pools. Live: every worker applies a
    # stored change to its pools (provisa/core/request_thread.configure) — raising serves waiting
    # requests at once, lowering takes effect as requests end.
    Setting(
        key="concurrency.request_threads",
        card="concurrency",
        type="int",
        effect="live",
        req="REQ-1882",  # 4: one in-flight request already occupies a worker's core
        env="PROVISA_REQUEST_THREADS",
        default=4,
        min=1,
        unit="threads",
    ),
    Setting(
        key="concurrency.stream_threads",
        card="concurrency",
        type="int",
        effect="live",
        req="REQ-1882",  # 256: a stream is a thread parked on its client, not a core
        env="PROVISA_STREAM_THREADS",
        default=256,
        min=1,
        unit="threads",
    ),
    Setting(
        key="concurrency.control_request_threads",
        card="concurrency",
        type="int",
        effect="live",
        req="REQ-1882",  # 4: health probes and the admin/auth API, apart from data load
        env="PROVISA_CONTROL_REQUEST_THREADS",
        default=4,
        min=1,
        unit="threads",
    ),
    Setting(
        key="concurrency.grpc_max_concurrent_rpcs",
        card="concurrency",
        type="int",
        effect="restart",  # sizes the gRPC server's thread pool and its RPC ceiling, per worker
        req="REQ-1904",
        env="GRPC_MAX_CONCURRENT_RPCS",
        config_path=("server", "grpc_max_concurrent_rpcs"),
        default=200,
        min=1,
        unit="calls",
    ),
    Setting(
        key="concurrency.flight_max_concurrent_streams",
        card="concurrency",
        type="int",
        effect="restart",  # the per-worker slot set is built for the value the worker booted on
        req="REQ-1905",
        env="FLIGHT_MAX_CONCURRENT_STREAMS",
        config_path=("server", "flight_max_concurrent_streams"),
        default_fn=_flight_stream_default,
        min=1,
        unit="streams",
    ),
    # --- Security --------------------------------------------------------------------------------
    Setting(
        key="security.mode",
        card="security",
        type="enum",
        effect="restart",  # decides which listeners start
        req="REQ-693",
        env="PROVISA_SECURITY_MODE",
        config_path=("security", "mode"),
        default="standard",
        choices=("standard", "high"),
        guard="confirm",
    ),
    Setting(
        key="grpc.allow_unsecured_reflection",
        card="security",
        type="bool",
        effect="restart",  # reflection is registered when the gRPC server is built
        req="REQ-1904",
        env="GRPC_ALLOW_UNSECURED_REFLECTION",
        config_path=("server", "grpc_allow_unsecured_reflection"),
        default=False,
        guard="confirm",
    ),
    Setting(
        key="bolt.allowed_origins",
        card="security",
        type="list",
        effect="live",  # read on every browser WebSocket upgrade
        req="REQ-1913",
        env="PROVISA_BOLT_ALLOWED_ORIGINS",
        default=(),  # no site is listed: a browser upgrade is refused
        guard="confirm",
    ),
    Setting(
        key="udf.egress_allowlist",
        card="security",
        type="list",
        effect="restart",  # applied to the function dispatcher when the config is loaded
        req="REQ-885",  # deny by default: a hosted function may call only the hosts listed
        env="PROVISA_UDF_EGRESS_ALLOWLIST",
        config_path=("server", "udf_egress_allowlist"),
        default=(),
        # REQ-885: the environment variable adds hosts to the config file's list. A value stored
        # through the settings page replaces both — it is the whole list.
        env_adds_to_config=True,
        guard="confirm",  # each entry opens an outbound path for hosted functions
    ),
    Setting(
        key="redis.require_tls",
        card="security",
        type="bool",
        effect="restart",  # checked when the cache stores are built
        req="REQ-1913",
        env="PROVISA_REQUIRE_REDIS_TLS",
        default=False,
    ),
    # REQ-125: the break-glass account. A pair stored here replaces the one the config file names
    # (which setup writes as references to PROVISA_SUPERUSER_USERNAME / _PASSWORD). Both halves
    # are write-only: the account's name is not shown either.
    Setting(
        key="security.superuser.username",
        card="security",
        type="str",
        effect="restart",  # resolved once, when the auth middleware is wired
        req="REQ-125",
        nullable=True,
        secret=True,
        guard="confirm",
    ),
    Setting(
        key="security.superuser.password",
        card="security",
        type="str",
        effect="restart",  # resolved once, when the auth middleware is wired
        req="REQ-125",
        nullable=True,
        secret=True,
        guard="confirm",
    ),
    # REQ-1393: the failed-login brake is on without configuration. Five attempts is above any
    # plausible typo count; a fifteen-minute lockout costs a guesser three orders of magnitude.
    Setting(
        key="auth.login_throttle.max_attempts",
        card="security",
        type="int",
        effect="live",  # read whenever an auth provider is built (per connection on the wire)
        req="REQ-1393",
        config_path=("auth", "login_throttle", "max_attempts"),
        default=5,
        min=1,
        unit="attempts",
        guard="confirm",  # loosening it weakens the brake on credential guessing
    ),
    Setting(
        key="auth.login_throttle.window_seconds",
        card="security",
        type="int",
        effect="live",  # read whenever an auth provider is built (per connection on the wire)
        req="REQ-1393",
        config_path=("auth", "login_throttle", "window_seconds"),
        default=300,
        min=1,
        unit="seconds",
        guard="confirm",  # loosening it weakens the brake on credential guessing
    ),
    Setting(
        key="auth.login_throttle.lockout_seconds",
        card="security",
        type="int",
        effect="live",  # read whenever an auth provider is built (per connection on the wire)
        req="REQ-1393",
        config_path=("auth", "login_throttle", "lockout_seconds"),
        default=900,
        min=1,
        unit="seconds",
        guard="confirm",  # loosening it weakens the brake on credential guessing
    ),
    # --- Network: hostname, listeners, TLS -------------------------------------------------------
    # Ports and TLS files are guarded: a wrong one can leave the operator unable to reach the
    # deployment. 0 turns an optional listener off.
    Setting(
        key="server.hostname",
        card="network",
        type="str",
        effect="restart",  # advertised by the servers started at boot
        req="REQ-1913",
        env="PROVISA_HOSTNAME",
        config_path=("server", "hostname"),
        default="localhost",
    ),
    Setting(
        key="server.grpc_port",
        card="network",
        type="int",
        effect="restart",  # a listener is bound when the server starts
        req="REQ-1913",
        env="GRPC_PORT",
        config_path=("server", "grpc_port"),
        default=50051,
        min=1,
        max=65535,
        guard="confirm",
    ),
    Setting(
        key="server.flight_port",
        card="network",
        type="int",
        effect="restart",  # a listener is bound when the server starts
        req="REQ-1913",
        env="FLIGHT_PORT",
        config_path=("server", "flight_port"),
        default=8815,
        min=1,
        max=65535,
        guard="confirm",
    ),
    Setting(
        key="server.pgwire_port",
        card="network",
        type="int",
        effect="restart",  # a listener is bound when the server starts
        req="REQ-1913",
        env="PROVISA_PGWIRE_PORT",
        default=0,
        min=0,
        max=65535,
        guard="confirm",
    ),
    Setting(
        key="server.bolt_port",
        card="network",
        type="int",
        effect="restart",  # a listener is bound when the server starts
        req="REQ-1913",
        env="PROVISA_BOLT_PORT",
        default=0,
        min=0,
        max=65535,
        guard="confirm",
    ),
    Setting(
        key="server.airport_port",
        card="network",
        type="int",
        effect="restart",  # a listener is bound when the server starts
        req="REQ-1913",
        env="PROVISA_AIRPORT_PORT",
        default=0,
        min=0,
        max=65535,
        guard="confirm",
    ),
    # REQ-1226: one certificate pair for the node; a protocol may name its own.
    Setting(
        key="tls.cert",
        card="network",
        type="str",
        effect="restart",
        req="REQ-1226",
        env="PROVISA_TLS_CERT",
        nullable=True,
        guard="confirm",
    ),
    Setting(
        key="tls.key",
        card="network",
        type="str",
        effect="restart",
        req="REQ-1226",
        env="PROVISA_TLS_KEY",
        nullable=True,
        guard="confirm",
    ),
    Setting(
        key="tls.grpc_cert",
        card="network",
        type="str",
        effect="restart",
        req="REQ-1226",
        env="PROVISA_GRPC_CERT",
        nullable=True,
        guard="confirm",
    ),
    Setting(
        key="tls.grpc_key",
        card="network",
        type="str",
        effect="restart",
        req="REQ-1226",
        env="PROVISA_GRPC_KEY",
        nullable=True,
        guard="confirm",
    ),
    Setting(
        key="tls.flight_cert",
        card="network",
        type="str",
        effect="restart",
        req="REQ-1226",
        env="PROVISA_FLIGHT_CERT",
        nullable=True,
        guard="confirm",
    ),
    Setting(
        key="tls.flight_key",
        card="network",
        type="str",
        effect="restart",
        req="REQ-1226",
        env="PROVISA_FLIGHT_KEY",
        nullable=True,
        guard="confirm",
    ),
    Setting(
        key="tls.pgwire_cert",
        card="network",
        type="str",
        effect="restart",
        req="REQ-1226",
        env="PROVISA_PGWIRE_CERT",
        nullable=True,
        guard="confirm",
    ),
    Setting(
        key="tls.pgwire_key",
        card="network",
        type="str",
        effect="restart",
        req="REQ-1226",
        env="PROVISA_PGWIRE_KEY",
        nullable=True,
        guard="confirm",
    ),
    Setting(
        key="tls.bolt_cert",
        card="network",
        type="str",
        effect="restart",
        req="REQ-1226",
        env="PROVISA_BOLT_CERT",
        nullable=True,
        guard="confirm",
    ),
    Setting(
        key="tls.bolt_key",
        card="network",
        type="str",
        effect="restart",
        req="REQ-1226",
        env="PROVISA_BOLT_KEY",
        nullable=True,
        guard="confirm",
    ),
    # --- Cache -----------------------------------------------------------------------------------
    Setting(
        key="cache.enabled",
        card="cache",
        type="bool",
        effect="restart",  # the response-cache store is built at config load
        req="REQ-829",  # on by default: a store always exists (embedded Redis when no URL is set)
        config_path=("cache", "enabled"),
        default=True,
    ),
    Setting(
        key="cache.default_ttl",
        card="cache",
        type="int",
        effect="restart",  # the deployment's default; an org narrows its own live (REQ-1349)
        req="REQ-1913",
        config_path=("cache", "default_ttl"),
        default=300,
        min=0,
        unit="seconds",
    ),
    Setting(
        key="cache.redis_url",
        card="cache",
        type="str",
        effect="restart",  # the cache stores and the rate limiter connect at boot
        req="REQ-829",
        env="REDIS_URL",
        config_path=("cache", "redis_url"),
        nullable=True,  # unset: the embedded in-process Redis
        secret=True,  # the URL may carry a password
        guard="confirm",
    ),
    Setting(
        key="cache.redis_org_password",
        card="cache",
        type="str",
        effect="live",  # read when an org or environment is provisioned
        req="REQ-1913",
        env="PROVISA_REDIS_ORG_PASSWORD",
        nullable=True,
        secret=True,
        guard="confirm",
    ),
    Setting(
        key="apq.ttl",
        card="cache",
        type="int",
        effect="restart",  # handed to the persisted-query cache when it is built
        req="REQ-289",
        env="PROVISA_APQ_TTL",
        config_path=("apq", "ttl"),
        default=86400,
        min=1,
        unit="seconds",
    ),
    Setting(
        key="compiler.compiled_query_cache_ttl",
        card="cache",
        type="int",
        effect="restart",  # read when a worker builds its compiled-query cache
        req="REQ-1877",
        env="PROVISA_COMPILED_QUERY_CACHE_TTL_SECONDS",
        default=60,
        min=1,
        unit="seconds",
    ),
    # --- Telemetry -------------------------------------------------------------------------------
    Setting(
        key="otel.trace_detail",
        card="telemetry",
        type="enum",
        effect="restart",  # the process default, set when tracing is configured (REQ-1910)
        req="REQ-1910",
        env="PROVISA_TRACE_DETAIL",
        config_path=("observability", "trace_detail"),
        default="normal",
        choices=tuple(TRACE_DETAILS),
    ),
    Setting(
        key="otel.trace_sql",
        card="telemetry",
        type="enum",
        effect="live",  # read at every traced stage
        req="REQ-1913",
        env="PROVISA_TRACE_SQL",
        default="off",
        choices=("off", "redacted", "full"),
        guard="confirm",  # "full" writes query text, literals included, into traces
    ),
    Setting(
        key="otel.trace_ast",
        card="telemetry",
        type="bool",
        effect="live",
        req="REQ-1913",
        env="PROVISA_TRACE_AST",
        default=False,
    ),
    Setting(
        key="otel.metric_export_interval",
        card="telemetry",
        type="int",
        effect="restart",  # handed to the metric reader when it is built
        req="REQ-1913",
        env="OTEL_METRIC_EXPORT_INTERVAL",
        default=15000,
        min=1,
        unit="milliseconds",
    ),
    # --- Bootstrap: what the launch itself decides -----------------------------------------------
    # Shown, not editable (REQ-1913): each is consumed before, or decides how, the control plane
    # the stored settings live in is reached.
    Setting(
        key="cache.redis_embedded",
        card="bootstrap",
        type="bool",
        effect="restart",
        req="REQ-829",
        env="PROVISA_REDIS_EMBEDDED",
        default=False,
        editable=False,
        readonly_reason="set_by_desktop_launcher",
    ),
    # The values below are what the launch was given: the environment variable when it is set.
    # Their readers run before the control plane is bound (some before the config is loaded) and
    # read the environment themselves; nothing can be stored for them, so the two cannot disagree.
    Setting(
        key="control_plane.tenant_url",
        card="bootstrap",
        type="str",
        effect="restart",
        req="REQ-1913",
        env="TENANT_DATABASE_URL",
        config_path=("control_plane", "tenant_url"),
        default=ControlPlaneConfig.model_fields["tenant_url"].default,
        secret=True,  # a DSN: it carries the control plane's credentials
        editable=False,
        readonly_reason="locates_control_plane",
    ),
    Setting(
        key="control_plane.platform_url",
        card="bootstrap",
        type="str",
        effect="restart",
        req="REQ-1913",
        env="PLATFORM_DATABASE_URL",
        config_path=("control_plane", "platform_url"),
        nullable=True,
        secret=True,
        editable=False,
        readonly_reason="locates_control_plane",
    ),
    Setting(
        key="control_plane.org_id",
        card="bootstrap",
        type="str",
        effect="restart",
        req="REQ-1913",
        env="ORG_ID",
        config_path=("control_plane", "org_id"),
        default="default",
        editable=False,
        readonly_reason="locates_control_plane",
    ),
    Setting(
        key="control_plane.pool_min",
        card="bootstrap",
        type="int",
        effect="restart",
        req="REQ-1913",
        env=None,
        config_path=("control_plane", "pool_min"),
        default=ControlPlaneConfig.model_fields["pool_min"].default,
        min=0,
        editable=False,
        readonly_reason="locates_control_plane",
    ),
    Setting(
        key="control_plane.pool_max",
        card="bootstrap",
        type="int",
        effect="restart",
        req="REQ-1913",
        env=None,
        config_path=("control_plane", "pool_max"),
        default=ControlPlaneConfig.model_fields["pool_max"].default,
        min=1,
        editable=False,
        readonly_reason="locates_control_plane",
    ),
    Setting(
        key="bootstrap.config_path",
        card="bootstrap",
        type="str",
        effect="restart",
        req="REQ-1913",
        env="PROVISA_CONFIG",
        nullable=True,
        editable=False,
        readonly_reason="read_before_control_plane",
    ),
    Setting(
        key="bootstrap.data_dir",
        card="bootstrap",
        type="str",
        effect="restart",
        req="REQ-1913",
        env="PROVISA_DATA_DIR",
        nullable=True,
        editable=False,
        readonly_reason="read_before_control_plane",
    ),
    Setting(
        key="bootstrap.home",
        card="bootstrap",
        type="str",
        effect="restart",
        req="REQ-1913",
        env="PROVISA_HOME",
        nullable=True,
        editable=False,
        readonly_reason="read_before_control_plane",
    ),
    Setting(
        key="bootstrap.repo_dir",
        card="bootstrap",
        type="str",
        effect="restart",
        req="REQ-1913",
        env="PROVISA_REPO_DIR",
        nullable=True,
        editable=False,
        readonly_reason="read_before_control_plane",
    ),
    Setting(
        key="bootstrap.demo",
        card="bootstrap",
        type="bool",
        effect="restart",
        req="REQ-1913",
        env="PROVISA_DEMO",
        default=False,
        editable=False,
        readonly_reason="set_by_launcher",
    ),
    # --- MCP -------------------------------------------------------------------------------------
    Setting(
        key="mcp.max_rows",
        card="mcp",
        type="int",
        effect="live",  # read on every run_sql call
        req="REQ-1913",
        env="PROVISA_MCP_MAX_ROWS",
        default=1000,
        min=1,
        unit="rows",
    ),
    Setting(
        key="mcp.port",
        card="mcp",
        type="int",
        effect="restart",  # a listener is bound when the server starts
        req="REQ-1008",
        env="PROVISA_MCP_PORT",
        default=0,
        min=0,
        max=65535,
        guard="confirm",
    ),
    Setting(
        key="mcp.host",
        card="mcp",
        type="str",
        effect="restart",
        req="REQ-1101",  # the server tier expects the MCP port reachable off the node
        env="PROVISA_MCP_HOST",
        default="0.0.0.0",  # noqa: S104  # nosec B104
        guard="confirm",
    ),
    Setting(
        key="mcp.tls",
        card="mcp",
        type="bool",
        effect="restart",
        req="REQ-1106",
        env="PROVISA_MCP_TLS",
        default=False,
        guard="confirm",
    ),
    Setting(
        key="mcp.role",
        card="mcp",
        type="str",
        effect="live",  # the role local stdio calls run as; unset, the stdio transport refuses
        req="REQ-1913",
        env="PROVISA_MCP_ROLE",
        nullable=True,
        guard="confirm",
    ),
    Setting(
        key="mcp.external_url",
        card="mcp",
        type="str",
        effect="live",  # the URL shown to clients when the deployment is behind a proxy
        req="REQ-1106",
        env="PROVISA_MCP_EXTERNAL_URL",
        nullable=True,
    ),
    Setting(
        key="mcp.chat_model",
        card="mcp",
        type="str",
        effect="live",  # overrides the org's configured chat model (REQ-1797)
        req="REQ-1797",
        env="PROVISA_MCP_CHAT_MODEL",
        nullable=True,
    ),
]

# The telemetry pipeline's settings are declared beside this module to keep each file readable.
from provisa.core.settings_catalog_telemetry import DECLARED as _TELEMETRY  # noqa: E402

from provisa.core.settings_catalog_engine import DECLARED as _ENGINE  # noqa: E402
from provisa.core.settings_catalog_redirect import DECLARED as _REDIRECT  # noqa: E402
from provisa.core.settings_catalog_tiers import DECLARED as _TIERS  # noqa: E402

DECLARED += _TIERS + _REDIRECT + _TELEMETRY + _ENGINE
