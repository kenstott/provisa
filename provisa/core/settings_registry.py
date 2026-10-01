# Copyright (c) 2026 Kenneth Stott
# Canary: c2f81d47-6a35-4e09-b7d2-91e4a0c63f58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The operator settings: declared once, resolved in one order (REQ-1913).

A setting is one :class:`Setting` in ``provisa/core/settings_catalog.py`` — its key, type, range,
environment variable, config path, default, and whether a change is live or waits for a restart.
Nothing else states any of those: a reader asks :func:`value` for its key, the admin API validates
with :func:`parse` and stores with :func:`store`, and the catalog the UI renders is the same
declarations.

RESOLUTION ORDER, for every setting: the value stored in the control plane
(``provisa/core/deployment_settings.py`` — the store every worker and instance reads, REQ-1900),
then the environment variable, then the config file, then the declared default. A value that is
present but cannot be parsed or is out of range is an error naming the setting and the source it
came from; it is never skipped in favour of the next source.

LIVE OR RESTART. A live setting is resolved on every read, so a stored change is in force on every
worker within the store's snapshot TTL. A restart setting is resolved once, by :func:`freeze` at
boot, and :func:`value` keeps returning that for the life of the process; a later stored change is
reported by :func:`pending_restart` until the process starts again. A process that never boots (a
script, a unit test compiling a query) has nothing frozen and reads what it was started with.
"""

# Requirements: REQ-1913, REQ-1900, REQ-1905

from __future__ import annotations

import base64
import logging
import os
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NamedTuple

from provisa.core import deployment_settings

if TYPE_CHECKING:
    from provisa.core.database import Database

log = logging.getLogger(__name__)

# The cards the admin UI groups the settings into, in display order.
CARDS = (
    "limits",
    "concurrency",
    "engine",
    "security",
    "network",
    "cache",
    "redirect",
    "telemetry",
    "mcp",
    "provisioning",
    "bootstrap",
)

# REQ-1913 (design decisions): the operator override for a stored value that stops the server
# booting. Set on a launch, every stored setting is ignored — the launch runs on environment and
# config only — and each ignored setting is logged. It applies only when set; it is not a fallback.
IGNORE_STORED_ENV = "PROVISA_IGNORE_STORED_SETTINGS"

# A secret's stored row is this one-key object holding the base64 of the encrypted value, so the
# control plane never holds the secret itself (the row's column is plain JSON text).
_SEALED = "$enc"

_TRUE = ("1", "true", "yes", "on")
_FALSE = ("0", "false", "no", "off")


@dataclass(frozen=True)
class Setting:
    """One operator setting. ``type`` is int, float, bool, str, enum, list or map."""

    key: str
    card: str
    type: str
    effect: str  # "live" | "restart"
    req: str  # the requirement that mandates the setting and its default
    env: str | None = None
    config_path: tuple[str, ...] | None = None
    default: Any = None
    default_fn: Callable[[], Any] | None = None  # a default derived from the host or the launch
    min: float | None = None
    max: float | None = None
    choices: tuple[str, ...] | None = None
    map_keys: tuple[str, ...] | None = None  # type "map": the keys it holds, each a float or unset
    nullable: bool = False  # the setting may be unset (its value is then None)
    # type "list": the environment ADDS its items to the config file's instead of replacing them.
    # A stored value is still the whole list.
    env_adds_to_config: bool = False
    secret: bool = False
    guard: str | None = None  # "confirm": needs explicit confirmation and an authenticated caller
    editable: bool = True
    readonly_reason: str | None = None
    unit: str | None = None
    # effect "restart": WHAT restarts when it is not the server itself — "engine" for the
    # federation engine's own sizing, whose files are regenerated at once.
    restart_scope: str | None = None


class Resolved(NamedTuple):
    value: Any
    source: str  # "stored" | "env" | "config" | "default"


class UnknownSetting(KeyError):
    def __init__(self, key: str) -> None:
        super().__init__(key)
        self.key = key

    def __str__(self) -> str:
        return f"unknown setting {self.key!r}"


class SettingInvalid(ValueError):
    """A setting's value cannot be used. Names the setting, the source and the reason."""

    def __init__(
        self,
        setting: Setting,
        source: str,
        raw: Any,
        reason: str,
        *,
        key: str | None = None,
    ) -> None:
        self.key = key or setting.key
        self.source = source
        self.reason = reason
        self.min = setting.min
        self.max = setting.max
        self.choices = setting.choices
        where = {
            "env": f"environment variable {setting.env}",
            "config": "config key " + ".".join(setting.config_path or ()),
            "stored": "stored value",
            "default": "declared default",
        }[source]
        shown = "<secret>" if setting.secret else repr(raw)
        super().__init__(f"setting {self.key}: {where} is {shown} — {reason}")


_settings: dict[str, Setting] = {}
_loaded = False
# Request threads reach the registry at the same time, and the first of them loads the catalog.
# Re-entrant: the catalog's own import may read the registry on the thread that is loading it.
_load_lock = threading.RLock()
_loading = False
_config: dict[str, Any] = {}
# The value each restart setting had when this process booted; None until it boots.
_frozen: dict[str, Any] | None = None


def register(setting: Setting) -> None:
    if setting.key in _settings:
        raise ValueError(f"setting {setting.key} is declared twice")
    _settings[setting.key] = setting


def _ensure_loaded() -> None:
    """Register the catalog, once. ``_loaded`` is set only when every setting is registered, so a
    thread that finds it set finds the whole registry; a thread that arrives while another loads
    waits for the lock rather than reading a registry still being filled."""
    global _loaded, _loading
    if _loaded:
        return
    with _load_lock:
        if _loaded or _loading:
            return  # loaded while this thread waited, or this thread is the one loading
        _loading = True
        try:
            from provisa.core import settings_catalog

            for setting in settings_catalog.DECLARED:
                register(setting)
            _loaded = True
        finally:
            _loading = False


def setting(key: str) -> Setting:
    _ensure_loaded()
    found = _settings.get(key)
    if found is None:
        raise UnknownSetting(key)
    return found


def key_for_env(env: str) -> str:
    """The key of the setting an environment variable names."""
    for s in all_settings():
        if s.env == env:
            return s.key
    raise UnknownSetting(env)


def all_settings() -> list[Setting]:
    _ensure_loaded()
    return list(_settings.values())


def bind_config(config: dict[str, Any]) -> None:
    """Name the parsed config file this process runs on (the API layer does this at config load)."""
    global _config
    _config = config


# --- parsing --------------------------------------------------------------------------------------


def _number(s: Setting, raw: Any, source: str, key: str | None, *, integer: bool) -> float | int:
    if isinstance(raw, bool):
        raise SettingInvalid(s, source, raw, "not_a_number", key=key)
    if isinstance(raw, str):
        try:
            num: float | int = int(raw) if integer else float(raw)
        except ValueError:
            raise SettingInvalid(s, source, raw, "not_a_number", key=key) from None
    elif isinstance(raw, int):
        num = raw if integer else float(raw)
    elif isinstance(raw, float):
        if integer:
            if not raw.is_integer():
                raise SettingInvalid(s, source, raw, "not_an_integer", key=key)
            num = int(raw)
        else:
            num = raw
    else:
        raise SettingInvalid(s, source, raw, "not_a_number", key=key)
    if s.min is not None and num < s.min:
        raise SettingInvalid(s, source, raw, "below_min", key=key)
    if s.max is not None and num > s.max:
        raise SettingInvalid(s, source, raw, "above_max", key=key)
    return num


def _boolean(s: Setting, raw: Any, source: str) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        word = raw.strip().lower()
        if word in _TRUE:
            return True
        if word in _FALSE:
            return False
    raise SettingInvalid(s, source, raw, "not_a_boolean")


def _text(s: Setting, raw: Any, source: str) -> str:
    if not isinstance(raw, str):
        raise SettingInvalid(s, source, raw, "not_a_string")
    if not raw.strip():
        raise SettingInvalid(s, source, raw, "empty")
    return raw.strip()


def _choice(s: Setting, raw: Any, source: str) -> str:
    if isinstance(raw, str):
        for choice in s.choices or ():
            if choice.lower() == raw.strip().lower():
                return choice
    raise SettingInvalid(s, source, raw, "not_in_choices")


def _items(s: Setting, raw: Any, source: str) -> list[str]:
    if isinstance(raw, str):
        raw = raw.split(",")
    if not isinstance(raw, (list, tuple)) or not all(isinstance(i, str) for i in raw):
        raise SettingInvalid(s, source, raw, "not_a_list")
    return [i.strip() for i in raw if i.strip()]


def _mapping(s: Setting, raw: Any, source: str) -> dict[str, float | None]:
    """The keys ``raw`` sets. A key set to None is stated as unset by that source."""
    if not isinstance(raw, dict):
        raise SettingInvalid(s, source, raw, "not_a_map")
    out: dict[str, float | None] = {}
    for name, item in raw.items():
        sub_key = f"{s.key}.{name}"
        if name not in (s.map_keys or ()):
            raise SettingInvalid(s, source, item, "unknown_key", key=sub_key)
        out[name] = (
            None if item is None else float(_number(s, item, source, sub_key, integer=False))
        )
    return out


def parse(s: Setting, raw: Any, source: str) -> Any:
    """``raw`` as a value of the setting's type, or :class:`SettingInvalid` naming why not."""
    if s.type == "int":
        return _number(s, raw, source, None, integer=True)
    if s.type == "float":
        return float(_number(s, raw, source, None, integer=False))
    if s.type == "bool":
        return _boolean(s, raw, source)
    if s.type == "str":
        return _text(s, raw, source)
    if s.type == "enum":
        return _choice(s, raw, source)
    if s.type == "list":
        return _items(s, raw, source)
    if s.type == "map":
        return _mapping(s, raw, source)
    raise ValueError(f"setting {s.key} declares unknown type {s.type!r}")


def _declared_default(s: Setting) -> Any:
    return s.default_fn() if s.default_fn is not None else s.default


def parse_default(s: Setting) -> Any:
    """The declared default as a value of the setting's type (None for a nullable setting)."""
    raw = _declared_default(s)
    if raw is None:
        if s.nullable:
            return None
        raise SettingInvalid(s, "default", raw, "no_default")
    if s.type == "list":
        return _items(s, list(raw), "default")
    return parse(s, raw, "default")


# --- the sources ---------------------------------------------------------------------------------


def ignoring_stored() -> bool:
    return os.environ.get(IGNORE_STORED_ENV, "").strip().lower() in _TRUE


def _seal(plaintext: str) -> dict[str, str]:
    from provisa.encryption.runtime import deployment_encryption_service

    blob = deployment_encryption_service().encrypt(plaintext.encode("utf-8"))
    return {_SEALED: base64.b64encode(blob).decode("ascii")}


def _unseal(s: Setting, raw: Any) -> str:
    from provisa.encryption.runtime import deployment_encryption_service

    if not isinstance(raw, dict) or set(raw) != {_SEALED}:
        raise SettingInvalid(s, "stored", raw, "not_encrypted")
    try:
        blob = base64.b64decode(raw[_SEALED], validate=True)
        return deployment_encryption_service().decrypt(blob).decode("utf-8")
    except Exception as exc:  # allow-ble: a provider's own error type; re-raised named
        # Sealed under another key or provider. The provider's own exception type is not known
        # here, and its message is logged rather than carried in an API response.
        log.error("stored setting %s cannot be decrypted: %s", s.key, type(exc).__name__)
        raise SettingInvalid(s, "stored", raw, "cannot_decrypt") from exc


def _stored_raw(s: Setting) -> Any | None:
    if ignoring_stored():
        return None
    raw = deployment_settings.get(s.key)
    if raw is not None and s.secret:
        return _unseal(s, raw)
    return raw


def _env_raw(s: Setting) -> str | None:
    if s.env is None:
        return None
    # An empty variable is one the operator did not set (a compose file passes `${VAR:-}`).
    return os.environ.get(s.env) or None


def _config_raw(s: Setting) -> Any | None:
    if s.config_path is None:
        return None
    node: Any = _config
    for part in s.config_path:
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    # A blank string in the config file states nothing, like an empty environment variable.
    return None if node == "" else node


def _sources(s: Setting) -> list[tuple[str, Any]]:
    """Every source that states a value for ``s``, highest precedence first, unparsed."""
    found = [
        ("stored", _stored_raw(s)),
        ("env", _env_raw(s)),
        ("config", _config_raw(s)),
    ]
    return [(name, raw) for name, raw in found if raw is not None]


def _resolve_map(s: Setting) -> tuple[dict[str, float | None], dict[str, str]]:
    values: dict[str, float | None] = dict.fromkeys(s.map_keys or ())
    sources = dict.fromkeys(s.map_keys or (), "default")
    layers = [("default", _mapping(s, dict(_declared_default(s) or {}), "default"))]
    layers += [(name, _mapping(s, raw, name)) for name, raw in reversed(_sources(s))]
    for name, layer in layers:
        for sub, item in layer.items():
            if item is not None:
                values[sub] = item
                sources[sub] = name
    return values, sources


def resolve(key: str) -> Resolved:
    """What the setting is now and where that comes from, by the resolution order."""
    s = setting(key)
    if s.type == "map":
        values, sources = _resolve_map(s)
        order = ("stored", "env", "config", "default")
        return Resolved(values, min(sources.values(), key=order.index))
    found = _sources(s)
    if s.env_adds_to_config and found and found[0][0] != "stored":
        return _resolve_union(s, found)
    for name, raw in found:
        return Resolved(_dereferenced(s, parse(s, raw, name)), name)
    return Resolved(parse_default(s), "default")


def _resolve_union(s: Setting, found: list[tuple[str, Any]]) -> Resolved:
    """Config's items, then the environment's that config does not already list."""
    items: list[str] = []
    for name, raw in reversed(found):  # config first, then env
        items += [item for item in parse(s, raw, name) if item not in items]
    return Resolved(items, found[0][0])


def _dereferenced(s: Setting, parsed: Any) -> Any:
    """A secret may be a reference to one held in the secrets service (``${secret:NAME}``,
    ``${env:NAME}``): it is kept as the reference and resolved each time it is read, so rotating
    what the reference names rotates what the reader is handed."""
    if not s.secret:
        return parsed
    from provisa.core.secrets import resolve_secrets

    return resolve_secrets(parsed)


def stored_value(key: str) -> Any | None:
    """The value stored for ``key`` through the settings page, or ``None`` when none is. For a
    reader whose environment-and-config layer is a config handed to it rather than the one this
    process is bound to (the engine's config renderer, an auth provider's own block)."""
    s = setting(key)
    raw = _stored_raw(s)
    return None if raw is None else _dereferenced(s, parse(s, raw, "stored"))


def map_sources(key: str) -> dict[str, str]:
    """For a map setting: the source each of its keys takes its value from."""
    return _resolve_map(setting(key))[1]


# --- live or restart-required --------------------------------------------------------------------


def freeze() -> None:
    """Fix the restart settings at their resolved values: this process has booted on them.

    Called once per boot, after the control plane is bound and the config is named. A restart
    setting whose value cannot be used stops the boot here, naming the setting and its source.
    """
    global _frozen
    if ignoring_stored():
        for s in all_settings():
            if deployment_settings.get(s.key) is not None:
                log.warning(
                    "stored setting %s ignored: %s is set (REQ-1913 operator override)",
                    s.key,
                    IGNORE_STORED_ENV,
                )
    _frozen = {s.key: resolve(s.key).value for s in all_settings() if s.effect == "restart"}


def freeze_at_boot() -> None:
    """:func:`freeze`, unless this process has already booted. The config is loaded again on a
    reload, and a reload is not a restart: the restart settings keep what the process booted on."""
    if _frozen is None:
        freeze()


def value(key: str) -> Any:
    """The value in force in this process: the reader's one door."""
    s = setting(key)
    if s.effect == "restart" and _frozen is not None:
        if key not in _frozen:
            raise RuntimeError(f"setting {key} was declared after this process booted")
        return _frozen[key]
    return resolve(key).value


def pending_restart(key: str) -> bool:
    """Whether a restart would change the value this process runs on."""
    s = setting(key)
    if s.effect != "restart" or _frozen is None:
        return False
    return resolve(key).value != _frozen[key]


def pending() -> list[str]:
    """The settings waiting for a restart."""
    return [s.key for s in all_settings() if pending_restart(s.key)]


# --- storing --------------------------------------------------------------------------------------


def _to_store(s: Setting, raw: Any) -> Any:
    if not s.editable:
        raise SettingInvalid(s, "stored", raw, "not_editable")
    if raw is None:
        return None  # clears the stored value: the setting returns to environment and config
    if s.secret:
        return _seal(parse(s, raw, "stored"))
    if s.type != "map":
        return parse(s, raw, "stored")
    merged = dict(deployment_settings.get(s.key) or {})
    for sub, item in _mapping(s, raw, "stored").items():
        if item is None:
            merged.pop(sub, None)
        else:
            merged[sub] = item
    return merged or None


# --- applying a live setting a worker holds as state ----------------------------------------------
#
# Most live settings are read where they are used, so the stored value is in force on the next
# read. A few are STATE a worker built from the value — the bounds of its request-thread pools —
# and a stored change has to be applied to that state in every worker. The owning module registers
# how (:func:`on_change`). REQ-1914: each worker's config watcher reloads its settings snapshot
# when the platform plane's ``settings`` stamp changes and then calls :func:`apply_changes`, so a
# change stored through one worker is applied by every other within the reload interval, with no
# restart.


@dataclass
class _Applier:
    keys: tuple[str, ...]
    apply: Callable[..., None]
    last: tuple[Any, ...] | None = field(default=None)


_appliers: list[_Applier] = []


def on_change(keys: tuple[str, ...], apply: Callable[..., None]) -> None:
    """Register ``apply(*values)`` — called with the values of ``keys`` when this process first
    applies its settings and again whenever any of them changes."""
    _appliers.append(_Applier(tuple(keys), apply))


def apply_changes() -> None:
    """Apply every registered setting whose value differs from the one last applied here. A value
    that cannot be used is logged naming the setting, and the value in force stays."""
    for applier in _appliers:
        try:
            current = tuple(value(key) for key in applier.keys)
        except SettingInvalid as err:
            log.error("setting %s is not applied, the value in force stays: %s", err.key, err)
            continue
        if current != applier.last:
            applier.apply(*current)
            applier.last = current


def applied(key: str) -> Any | None:
    """The value of ``key`` this process last applied, or ``None`` when nothing applies it."""
    for applier in _appliers:
        if key in applier.keys and applier.last is not None:
            return applier.last[applier.keys.index(key)]
    return None


async def reload_and_apply() -> None:
    """Read the stored settings again and apply what changed: the config watcher's reload for the
    platform plane's ``settings`` stamp (REQ-1914)."""
    deployment_settings.reload()
    apply_changes()


def validate(values: dict[str, Any]) -> None:
    """Check every value as :func:`store` would, storing nothing."""
    for key, raw in values.items():
        _to_store(setting(key), raw)


def prospective(key: str, values: dict[str, Any]) -> Any:
    """What ``key`` resolves to once ``values`` are stored: the saved value when it is one of
    them (a saved ``None`` clears the stored value, leaving environment, config and default),
    else what it resolves to now."""
    s = setting(key)
    if key not in values:
        return resolve(key).value
    raw = values[key]
    if raw is not None:
        return _dereferenced(s, parse(s, raw, "stored"))
    below = [(name, raw) for name, raw in _sources(s) if name != "stored"]
    if s.env_adds_to_config and below:
        return _resolve_union(s, below).value
    for name, raw in below:
        return _dereferenced(s, parse(s, raw, name))
    return parse_default(s)


def store(db: "Database", values: dict[str, Any], *, updated_by: str) -> None:
    """Validate every value, then store them all. One bad value stores nothing.

    ``None`` clears a setting's stored value. A map setting is stored key by key: the keys given
    are set (or, given as None, cleared) and the others keep their stored values.
    """
    checked = {key: _to_store(setting(key), raw) for key, raw in values.items()}
    deployment_settings.write(db, checked, updated_by=updated_by)
    apply_changes()  # the worker that stores a change applies it at once


# --- describing a setting (the catalog the admin UI renders) -------------------------------------


def _described_state(s: Setting) -> dict[str, Any]:
    """Value, source and pending state — or, for a value that cannot be used, the error. The
    catalog still lists such a setting: it is the page the operator corrects it from."""
    try:
        resolved = resolve(s.key)
        return {
            "value": value(s.key),
            "source": resolved.source,
            "pending_restart": pending_restart(s.key),
            "resolved": resolved.value,
        }
    except SettingInvalid as err:
        return {
            "value": None,
            "source": err.source,
            "pending_restart": False,
            "resolved": None,
            "error": {"field": err.key, "source": err.source, "reason": err.reason},
        }


def _described_stored(s: Setting) -> Any:
    try:
        raw = _stored_raw(s)
        return None if raw is None else parse(s, raw, "stored")
    except SettingInvalid:
        return None  # reported by _described_state as the setting's error


def describe(key: str) -> dict[str, Any]:
    """One setting as the catalog shows it. A secret is described only as set or not set."""
    s = setting(key)
    state = _described_state(s)
    resolved = state.pop("resolved")
    out: dict[str, Any] = {
        "key": s.key,
        "type": "secret" if s.secret else s.type,
        "unit": s.unit,
        "source": state["source"],
        "env": s.env,
        "config_key": ".".join(s.config_path) if s.config_path else None,
        "restart_required": s.effect == "restart",
        "pending_restart": state["pending_restart"],
        "secret": s.secret,
        "editable": s.editable,
        "guard": s.guard,
    }
    if "error" in state:
        out["error"] = state["error"]
    if s.restart_scope is not None:
        out["restart_scope"] = s.restart_scope
    in_force = applied(s.key)
    if in_force is not None:
        out["applied"] = in_force  # what THIS worker's state was last built from
    if not s.editable:
        out["readonly_reason"] = s.readonly_reason
    if s.secret:
        out["set"] = resolved is not None
        return out
    out.update(
        min=s.min,
        max=s.max,
        value=state["value"],
        default=parse_default(s)
        if s.type != "map"
        else _mapping(s, dict(s.default or {}), "default"),
        stored=_described_stored(s),
    )
    if s.type == "enum":
        out["choices"] = list(s.choices or ())
    if s.type == "map":
        out["map_keys"] = list(s.map_keys or ())
        out["sources"] = map_sources(key) if "error" not in state else {}
    return out
