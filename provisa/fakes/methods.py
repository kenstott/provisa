# Copyright (c) 2026 Kenneth Stott
# Canary: 16929c73-2f47-4dc9-ab2f-b333c826ebbc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The fake methods a column may name besides Provisa's own kinds (REQ-1494): realistic values of a
kind -- a person's name, an email address, a phone number, a street address, a company, a
sentence. A method is checked when declared: it must exist, take the arguments given and make
values the column's type can hold.

A fake that is not stable is computed by the serving engine's own implementation of the method,
seeded by the keyed digest of the real value, and is consistent within that engine. A stable fake
is computed by Provisa's portable definition, the same on every engine, which covers the methods
in :data:`STABLE_METHODS` only; declaring any other method stable is refused by name.
"""

# Requirements: REQ-1494

from __future__ import annotations

import datetime as _dt
import decimal
import inspect
from functools import lru_cache
from typing import Any

from provisa.fakes.kinds import FakeRefused, Method

LOCALE = "en_US"

#: The methods the portable definition computes -- from the keyed digest, word lists and templates
#: shipped with Provisa and versioned with it -- so a stable fake of them is the same everywhere.
STABLE_METHODS = frozenset(
    {
        "first_name",
        "last_name",
        "name",
        "email",
        "user_name",
        "phone_number",
        "street_address",
        "city",
        "state",
        "postcode",
        "country",
        "company",
        "job",
        "word",
        "sentence",
        "uuid4",
    }
)

# Members every provider inherits that make no value.
_NOT_GENERATORS = frozenset(
    {
        "seed",
        "seed_instance",
        "seed_locale",
        "add_provider",
        "get_providers",
        "get_formatter",
        "set_formatter",
        "set_arguments",
        "get_arguments",
        "del_arguments",
        "parse",
        "format",
        "provider",
    }
)

_STRING = (str,)
_INTEGER = (int,)
_NUMERIC = (int, float, decimal.Decimal)
_HOLDS: dict[str, tuple[type, ...]] = {
    "text": _STRING,
    "integer": _INTEGER,
    "numeric": _NUMERIC,
    "boolean": (bool,),
    "date": (_dt.date,),
    "timestamp": (_dt.datetime,),
}


@lru_cache(maxsize=1)
def _generator() -> Any:
    from faker import Faker

    return Faker(LOCALE)


@lru_cache(maxsize=1)
def method_names() -> tuple[str, ...]:
    """Every fake method a column may name."""
    names: set[str] = set()
    for provider in _generator().providers:
        for name, _member in inspect.getmembers(provider, inspect.ismethod):
            if not name.startswith("_") and name not in _NOT_GENERATORS:
                names.add(name)
    return tuple(sorted(names))


def check_method(method: Method, holds: str | None) -> None:
    """Refuse a method that does not exist, cannot take the arguments given, or makes values a
    column holding ``holds`` (a family of :func:`provisa.fakes.checks.family`) cannot hold;
    ``holds`` None checks the call only."""
    if method.name not in method_names():
        raise FakeRefused(f"there is no fake kind or method {method.name!r}")
    signature = inspect.signature(getattr(_generator(), method.name))
    args = dict(method.args)
    accepted = {p.name for p in signature.parameters.values() if p.kind not in (p.VAR_POSITIONAL,)}
    takes_any = any(p.kind is p.VAR_KEYWORD for p in signature.parameters.values())
    unknown = sorted(set(args) - accepted) if not takes_any else []
    if unknown:
        raise FakeRefused(f"{method.name}() takes no argument {unknown[0]!r}")
    try:
        signature.bind(**args)
    except TypeError as exc:
        raise FakeRefused(f"{method.name}() cannot be called with {args!r}: {exc}") from exc
    sample = sample_of(method)
    if holds is None:
        return
    kinds = _HOLDS.get(holds)
    if (
        kinds is None
        or (isinstance(sample, bool) and bool not in kinds)
        or not isinstance(sample, kinds)
    ):
        raise FakeRefused(
            f"{method.name}() makes {type(sample).__name__} values, which a {holds} column "
            f"cannot hold"
        )


def sample_of(method: Method) -> Any:
    """One value the method makes, from a fixed seed; refused by name when the method refuses its
    arguments."""
    generator = _generator()
    generator.seed_instance(0)
    try:
        return getattr(generator, method.name)(**dict(method.args))
    except Exception as exc:  # noqa: BLE001 -- any refusal by the method is the declaration's fault
        raise FakeRefused(f"{method.name}() refuses {dict(method.args)!r}: {exc}") from exc
