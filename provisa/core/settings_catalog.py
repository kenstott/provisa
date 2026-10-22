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

import socket

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
        key="limits.max_query_complexity",
        card="limits",
        type="int",
        effect="live",  # read when a statement is governed
        req="REQ-1174",  # unset as shipped: an org states its ceiling, none is assumed for it
        env="PROVISA_MAX_QUERY_COMPLEXITY",
        config_path=("server", "limits", "max_query_complexity"),
        default=None,
        nullable=True,
        min=1,
        unit="points",
    ),
    Setting(
        key="logs.preview_lines",
        card="limits",
        type="int",
        effect="live",  # read when a Logs source's columns are worked out
        # REQ-1996: a JSON-lines or logfmt source's columns are the fields its first lines
        # name; 1000 lines sees the fields of a log's usual records without reading it whole.
        req="REQ-1996",
        env="PROVISA_LOGS_PREVIEW_LINES",
        default=1000,
        min=1,
        unit="lines",
    ),
    Setting(
        key="logs.max_line_bytes",
        card="limits",
        type="int",
        effect="live",  # read when a Logs source's files are read
        # REQ-1996: a longer record keeps its first bytes as its raw text and says why it was
        # not parsed; 65536 holds any ordinary log line and a stack trace of some depth.
        req="REQ-1996",
        env="PROVISA_LOGS_MAX_LINE_BYTES",
        default=65536,
        min=256,
        unit="bytes",
    ),
    Setting(
        key="sources.max_upload_bytes",
        card="limits",
        type="int",
        effect="live",  # read when an upload begins
        # REQ-1997: the largest file a file-based source may be given by upload; 1 GiB holds a
        # working SQLite, DuckDB, CSV or Parquet file, and a larger one is better read where it
        # is (an object store) than copied into the org's folder.
        req="REQ-1997",
        env="PROVISA_SOURCES_MAX_UPLOAD_BYTES",
        default=1073741824,
        min=1,
        unit="bytes",
    ),
    # --- A session's temporary tables (REQ-1926) --------------------------------------------------
    # Live: read when a table is made or filled. A temporary table is a real table of the
    # engine's store that a role with no write right may fill, so both are bounded.
    Setting(
        key="temp_tables.max_per_session",
        card="limits",
        type="int",
        effect="live",
        req="REQ-1926",  # 20: enough for a working session; a table past it is refused by name
        env="PROVISA_TEMP_TABLES_MAX_PER_SESSION",
        default=20,
        min=0,
        unit="tables",
    ),
    Setting(
        key="temp_tables.max_rows",
        card="limits",
        type="int",
        effect="live",
        # 1,000,000: a role's row limit bounds one query, not a table filled by several, and a
        # role with full results has none; a table that would pass this is refused, not cut.
        req="REQ-1926",
        env="PROVISA_TEMP_TABLES_MAX_ROWS",
        default=1_000_000,
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
    # --- Replication (REQ-1915) ------------------------------------------------------------------
    # Live: a build runner reads each when it looks for work, so a change applies to the next
    # build it starts; builds already running keep what they hold.
    Setting(
        key="replication.builds_per_node",
        card="concurrency",
        type="int",
        effect="live",
        req="REQ-1915",  # 1: one copy at a time on a host until the operator sizes it up
        env="PROVISA_REPLICATION_BUILDS_PER_NODE",
        default=1,
        min=1,
        unit="builds",
    ),
    Setting(
        key="replication.engine_jobs",
        card="concurrency",
        type="int",
        effect="live",
        # 2: the engine stays mostly free for requests, which are not counted, and one long
        # build does not hold every other replica's refresh behind it.
        req="REQ-1915",
        env="PROVISA_REPLICATION_ENGINE_JOBS",
        default=2,
        min=1,
        unit="jobs",
    ),
    Setting(
        key="kaggle.api_token",
        card="security",
        type="str",
        effect="live",  # read at each Kaggle search and each staging
        req="REQ-1819",
        # The default is a reference in a visible, editable field, not a name hidden in code
        # (maintainer ruling, 2026-10-10: the product resolves what a field holds and makes up
        # no secret names): an operator who already created a secret of this name keeps
        # working, and any other value or reference may be typed over it.
        # reference-convention: REQ-1798 -- the Kaggle token's fixed secret name
        default="${secret:kaggle_api_token}",
        credential=True,
    ),
    Setting(
        key="mail.throttle_wait_seconds",
        card="concurrency",
        type="int",
        effect="live",  # read when a replica's read of a mail source begins
        # 900: a mail service that is throttling says how long to wait (Retry-After), and for a
        # mailbox that has been read hard it asks for minutes. A replica's build is background
        # work: it waits that out and goes on, where failing throws the whole read away. This is
        # the most the waits of ONE call may add up to; a longer one fails the read by name.
        req="REQ-1923",
        env="PROVISA_MAIL_THROTTLE_WAIT_SECONDS",
        default=900,
        min=0,
        unit="seconds",
    ),
    Setting(
        key="mail.mailboxes_at_once",
        card="concurrency",
        type="int",
        effect="live",
        # 4: a source of an organisation's mailboxes reads this many at the same time. Each
        # mailbox is paced on its own by its vendor; more at once shortens a build and asks
        # more of the vendor's limit for the whole application.
        req="REQ-1923",
        env="PROVISA_MAIL_MAILBOXES_AT_ONCE",
        default=4,
        min=1,
        unit="mailboxes",
    ),
    Setting(
        key="replication.retry_interval",
        card="concurrency",
        type="int",
        effect="live",
        req="REQ-1915",  # 60: a failing source is asked again once a minute, not once per read
        env="PROVISA_REPLICATION_RETRY_INTERVAL",
        default=60,
        # At least a second: with no wait at all a build that fails is claimed again at once,
        # and the runner never rests (#203).
        min=1,
        unit="seconds",
    ),
    Setting(
        key="replication.retry_interval_max",
        card="concurrency",
        type="int",
        effect="live",
        # 3600: the wait before a failed build is tried again doubles with each failure in a row
        # and stops growing here -- a source that stays down is asked once an hour, for as long
        # as its table is registered. Not below the interval (settings_guards refuses it).
        req="REQ-1915",
        env="PROVISA_REPLICATION_RETRY_INTERVAL_MAX",
        default=3600,
        min=0,
        unit="seconds",
    ),
    Setting(
        key="replication.spool_max_bytes",
        card="limits",
        type="int",
        effect="live",  # read when a build opens its spool
        # 2 GiB: the most disk a node's spool directory holds for single-document sources being
        # replicated; a build that would pass it fails by name.
        req="REQ-1915",
        env="PROVISA_REPLICATION_SPOOL_MAX_BYTES",
        default=2 * 1024**3,
        min=0,
        unit="bytes",
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
    # REQ-1914: how often each worker reads the control plane's config stamps. A model, governance
    # or settings change made through one worker is in force on every other worker of every
    # instance within this long (provisa/core/config_watch.py). Live: a worker re-reads it on
    # every tick.
    Setting(
        key="config.reload_interval",
        card="concurrency",
        type="float",
        effect="live",
        req="REQ-1914",  # 2 s: one short read per control plane per worker, per interval
        env="PROVISA_CONFIG_RELOAD_INTERVAL",
        config_path=("server", "config_reload_interval"),
        default=2.0,
        min=0.5,
        unit="seconds",
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
        key="commands.allow_host_code",
        card="security",
        type="bool",
        effect="live",  # read when a command is registered, updated or called
        # REQ-2134: python and script commands run code with the server's own access; off by
        # default, so neither an organization nor a promoted model can run code on the host.
        req="REQ-2134",
        env="PROVISA_COMMANDS_ALLOW_HOST_CODE",
        config_path=("server", "commands", "allow_host_code"),
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
        key="sources.download_roots",
        card="security",
        type="list",
        effect="live",  # read when a source's file is downloaded
        # REQ-2113: the directories a CSV or Parquet source's file may be downloaded from by a
        # file mount path, after following symlinks. None by default: each entry lets the
        # server's own files under it be copied into an org's folder. Refused whatever it holds
        # on a server of many organizations.
        req="REQ-2113",
        env="PROVISA_SOURCES_DOWNLOAD_ROOTS",
        default=(),
        guard="confirm",
    ),
    Setting(
        key="logs.allowed_roots",
        card="security",
        type="list",
        effect="live",  # read when a Logs source is saved, previewed and read
        # REQ-1996 (maintainer ruling, 2026-10-12): the directories a Logs source's glob may
        # resolve into, after following symlinks; by default /var/log. Each entry lets the
        # server's own files under it be read into tables.
        req="REQ-1996",
        env="PROVISA_LOGS_ALLOWED_ROOTS",
        default=("/var/log",),
        guard="confirm",
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
    # REQ-1494: the platform key every fake's digest is keyed by. Created by the platform at its
    # first start; secret, and guarded: a different key shows every faked value a new fake.
    Setting(
        key="fakes.key",
        card="security",
        type="str",
        effect="restart",  # held by each process and each engine from its start
        req="REQ-1494",
        nullable=True,
        secret=True,
        guard="confirm",
    ),
    # REQ-2091: the share of a sensitive column's sampled values its fake may leave unchanged before
    # the leak-rate adequacy check fails it.
    Setting(
        key="fakes.leak_rate_threshold",
        card="security",
        type="float",
        effect="live",  # read on every check run
        req="REQ-2091",
        default=0.001,
        min=0.0,
        max=1.0,
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
    # REQ-2076: where a role-probing event is posted, beside the email to the org's administrators.
    # Write-only: a webhook address usually carries its own secret.
    Setting(
        key="security.webhook_url",
        card="security",
        type="str",
        effect="live",  # read when an event is posted
        req="REQ-2076",
        env="PROVISA_SECURITY_WEBHOOK_URL",
        config_path=("security", "webhook_url"),
        default=None,
        nullable=True,
        secret=True,
    ),
    # REQ-2077: how long before a role grant expires, or is removed for lack of use, its holder
    # and the org's admins are told.
    Setting(
        key="auth.role_grant_notice_days",
        card="security",
        type="int",
        effect="live",  # read at each sweep
        req="REQ-2077",
        env="PROVISA_GRANT_NOTICE_DAYS",
        default=7,
        min=1,
        unit="days",
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
        key="server.flight_compression",
        card="network",
        type="enum",
        effect="live",  # read when a Flight stream is created (provisa/api/flight/compression.py)
        req="REQ-1913",
        env="PROVISA_FLIGHT_COMPRESSION",
        config_path=("server", "flight_compression"),
        default="none",  # off: batches are sent as they are
        choices=("none", "lz4", "zstd"),
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
    # --- Service level (REQ-506): the limits each org's last hour is evaluated against ----------
    Setting(
        key="sla.p99_limit_ms",
        card="telemetry",
        type="int",
        effect="live",  # read at every evaluation
        req="REQ-506",
        env="PROVISA_SLA_P99_LIMIT_MS",
        config_path=("sla", "p99_limit_ms"),
        default=5000,
        min=1,
        unit="milliseconds",
    ),
    Setting(
        key="sla.availability_min_pct",
        card="telemetry",
        type="float",
        effect="live",
        req="REQ-506",
        env="PROVISA_SLA_AVAILABILITY_MIN_PCT",
        config_path=("sla", "availability_min_pct"),
        default=99.9,
        min=0,
        max=100,
        unit="percent",
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
    # REQ-2016: the named demos whose setup.yaml has run against this control plane
    # (scripts/run_demo_setup.py adds a demo once every step succeeded). A demo listed here is not
    # set up again; taking it off the list sets it up again on the next start.
    Setting(
        key="demo.setup_completed",
        card="bootstrap",
        type="list",
        effect="live",  # read when a demo's setup is about to run
        req="REQ-2016",
        default=(),
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
    # REQ-1992: the data folder is one mount shared by every node of the deployment, and this
    # node's name among them. Set per node by the launcher (Helm, compose), never stored: a stored
    # value would be the same on every node.
    Setting(
        key="org_files.shared_mount",
        card="bootstrap",
        type="bool",
        effect="restart",
        req="REQ-1992",
        env="PROVISA_ORG_FILES_SHARED_MOUNT",
        default=False,
        editable=False,
        readonly_reason="set_by_launcher",
    ),
    Setting(
        key="org_files.node_id",
        card="bootstrap",
        type="str",
        effect="restart",
        req="REQ-1992",
        env="PROVISA_ORG_FILES_NODE_ID",
        # The pod's or host's own name, which is the node's.
        default_fn=socket.gethostname,
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
        key="mcp.stats_max_rows",
        card="limits",
        type="int",
        effect="live",  # read on every stats tool call
        req="REQ-2090",
        env="PROVISA_MCP_STATS_MAX_ROWS",
        default=100000,
        min=1,
        unit="rows",
    ),
    Setting(
        key="mcp.render_timeout_s",
        card="mcp",
        type="int",
        effect="live",  # read on every render_graph call
        req="REQ-1990",
        env="PROVISA_MCP_RENDER_TIMEOUT_S",
        default=30,
        min=1,
        unit="seconds",
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
    Setting(
        key="polly.model_prices",
        card="mcp",
        type="list",
        effect="live",  # read when a Polly turn reports its usage (REQ-2012)
        req="REQ-2012",
        env="PROVISA_POLLY_MODEL_PRICES",
        columns=("model", "input", "output", "cache_read", "cache_write"),
        # One entry a model: "<model id> <input> <output> <cache read> <cache write>", USD per
        # million tokens, the model id as Polly's vendor names it. From the vendors' published
        # prices, 2026-10-13 (OpenRouter's catalog, which matches Anthropic's own list).
        default=(
            "claude-haiku-5-5 0.1 0.5 0.01 0.125",
            "claude-sonnet-5-5 2.0 10.0 0.1 2.5",
            "claude-opus-5-5 4.0 20.0 0.2 5.0",
            "anthropic/claude-haiku-5.5 0.1 0.5 0.01 0.125",
            "anthropic/claude-sonnet-5.5 2.0 10.0 0.1 2.5",
            "anthropic/claude-opus-5.5 4.0 20.0 0.2 5.0",
            "google/gemini-3.8-flash 0.75 3.75 0.075 0.0417",
            "openai/gpt-5.4-mini 0.75 4.5 0.075 0.75",
            "moonshotai/kimi-k3 0.73 10.0 0.55 0.73",
            "deepseek/deepseek-v4.1-flash 0.0441 0.63 0.0054 0.0441",
            "qwen/qwen3.8-flash 0.15 0.47 0.016 0.15",
        ),
    ),
]

# The telemetry pipeline's settings are declared beside this module to keep each file readable.
from provisa.core.settings_catalog_telemetry import DECLARED as _TELEMETRY  # noqa: E402

from provisa.core.settings_catalog_engine import DECLARED as _ENGINE  # noqa: E402
from provisa.core.settings_catalog_redirect import DECLARED as _REDIRECT  # noqa: E402
from provisa.core.settings_catalog_tiers import DECLARED as _TIERS  # noqa: E402
from provisa.core.settings_catalog_connectors import DECLARED as _CONNECTORS  # noqa: E402
from provisa.core.settings_catalog_sandbox import DECLARED as _SANDBOX  # noqa: E402

DECLARED += _TIERS + _REDIRECT + _TELEMETRY + _ENGINE + _CONNECTORS + _SANDBOX
