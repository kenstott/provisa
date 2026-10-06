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

#: Methods whose value is a list of strings, shown as one text joined by its natural separator
#: (REQ-1494): paragraphs and texts by line, the others by space; letters run together.
JOINED = {
    "paragraphs": "\n",
    "texts": "\n",
    "sentences": " ",
    "words": " ",
    "get_words_list": " ",
    "random_choices": " ",
    "random_elements": " ",
    "random_sample": " ",
    "nic_handles": " ",
    "random_letters": "",
}

#: Methods whose value is a (code, name) pair, shown as the code.
CODED = frozenset({"currency", "cryptocurrency"})

#: Methods no column can declare (REQ-1494): each makes bytes, a tuple of several values (a
#: coordinate pair is two columns, and Provisa has no point type), a structure of mixed values, an
#: object, a generator, or needs a class argument (enum). Refused by name when declared, on every
#: engine; every other method is computed by every engine that computes fakes.
UNSUPPORTED = frozenset(
    {
        # bytes
        "binary", "image", "json_bytes", "tar", "zip",
        # tuples of several values
        "color_hsl", "color_hsv", "color_rgb", "color_rgb_float", "latlng", "local_latlng",
        "location_on_land", "passport_dates", "passport_owner", "pystruct", "pytuple",
        # structures of mixed values, generators
        "pyiterable", "pylist", "pyset", "profile", "pydict", "simple_profile", "time_series",
        # objects no column type holds
        "pyobject", "pytimezone", "time_delta", "time_object",
        # needs a class, or is not available in this locale
        "enum", "xml",
    }
)  # fmt: skip


def column_value(name: str, value: Any) -> Any:
    """A method's value as a column holds it: a list of strings joined, a (code, name) pair its
    code; anything else as made."""
    if name in JOINED:
        return JOINED[name].join(str(v) for v in value)
    if name in CODED:
        return value[0]
    return value


#: The arguments each method takes on every engine (REQ-1494): the range, format, length and
#: pattern arguments of the date, time, number and text methods. Another argument is refused by
#: name when declared -- an engine computing the method by its own implementation could not honour
#: it. Mirrors FakeMethods.ARGUMENTS of the Trino plugin (tests/unit/test_fake_method_arguments.py).
ARGUMENTS: dict[str, tuple[str, ...]] = {
    "bothify": ("text", "letters"),
    "numerify": ("text",),
    "lexify": ("text", "letters"),
    "hexify": ("text", "upper"),
    "pyint": ("min_value", "max_value", "step"),
    "random_int": ("min", "max", "step"),
    "random_number": ("digits", "fix_len"),
    "pyfloat": ("left_digits", "right_digits", "positive", "min_value", "max_value"),
    "pydecimal": ("left_digits", "right_digits", "positive", "min_value", "max_value"),
    "pystr": ("min_chars", "max_chars", "prefix", "suffix"),
    "password": ("length", "special_chars", "digits", "upper_case", "lower_case"),
    "nic_handle": ("suffix",),
    "nic_handles": ("count", "suffix"),
    "date": ("pattern", "end_datetime"),
    "time": ("pattern", "end_datetime"),
    "date_object": ("end_datetime",),
    "date_time": ("end_datetime",),
    "date_time_ad": ("start_datetime", "end_datetime"),
    "iso8601": ("end_datetime", "sep"),
    "boolean": ("chance_of_getting_true",),
    "pybool": ("truth_probability",),
    "random_element": ("elements",),
    "date_between": ("start_date", "end_date"),
    "date_time_between": ("start_date", "end_date"),
    "date_between_dates": ("date_start", "date_end"),
    "date_time_between_dates": ("datetime_start", "datetime_end"),
    "future_date": ("end_date",),
    "future_datetime": ("end_date",),
    "past_date": ("start_date",),
    "past_datetime": ("start_date",),
    "date_of_birth": ("minimum_age", "maximum_age"),
    "date_this_century": ("before_today", "after_today"),
    "date_this_decade": ("before_today", "after_today"),
    "date_this_year": ("before_today", "after_today"),
    "date_this_month": ("before_today", "after_today"),
    "date_time_this_century": ("before_now", "after_now"),
    "date_time_this_decade": ("before_now", "after_now"),
    "date_time_this_year": ("before_now", "after_now"),
    "date_time_this_month": ("before_now", "after_now"),
    "unix_time": ("start_datetime", "end_datetime"),
    "words": ("nb", "unique"),
    "sentences": ("nb",),
    "paragraphs": ("nb",),
    "texts": ("nb_texts", "max_nb_chars"),
    "sentence": ("nb_words",),
    "paragraph": ("nb_sentences",),
    "text": ("max_nb_chars",),
    "random_letters": ("length",),
    "random_choices": ("elements", "length"),
    "random_elements": ("elements", "length", "unique"),
    "random_sample": ("elements", "length"),
}

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


#: REQ-1494 (determinism): a fake is a keyed function of the value, so nothing it shows may depend
#: on the wall clock. Every method whose default range starts or ends at "now" is relative to this
#: instant instead, in UTC, on every engine: the Trino plugin's FakeMethods.REFERENCE_INSTANT and
#: PL/Python (which runs this module) read the same instant.
REFERENCE_INSTANT = _dt.datetime(2026, 7, 15, 12, 0, 0, tzinfo=_dt.timezone.utc)


class _AnyDateTime(type):
    # The providers test values with isinstance against these names: every datetime is one.
    def __instancecheck__(cls, obj: Any) -> bool:
        return isinstance(obj, _dt.datetime)


class _AnyDate(type):
    def __instancecheck__(cls, obj: Any) -> bool:
        return isinstance(obj, _dt.date)


class _PinnedDateTime(_dt.datetime, metaclass=_AnyDateTime):
    @classmethod
    def now(cls, tz: Any = None) -> Any:  # type: ignore[override]
        at = cls.fromtimestamp(REFERENCE_INSTANT.timestamp(), _dt.timezone.utc)
        return at.astimezone(tz) if tz is not None else at.replace(tzinfo=None)

    @classmethod
    def today(cls) -> Any:  # type: ignore[override]
        return cls.now()


class _PinnedDate(_dt.date, metaclass=_AnyDate):
    @classmethod
    def today(cls) -> Any:  # type: ignore[override]
        return cls(REFERENCE_INSTANT.year, REFERENCE_INSTANT.month, REFERENCE_INSTANT.day)


def _pin_clock() -> None:
    """REQ-1494 (determinism): the library's date and time providers read "now", "today" and the
    local time zone from their own module globals; they read the reference instant and UTC."""
    from faker.providers import date_time
    from faker.providers.passport import en_US as passport

    date_time.datetime = _PinnedDateTime  # type: ignore[misc]
    date_time.dtdate = _PinnedDate  # type: ignore[misc]
    date_time._get_local_timezone = lambda: _dt.timezone.utc
    passport.date = _PinnedDate  # type: ignore[misc]


def _seeded_passport() -> Any:
    """REQ-1494 (determinism): the library's passport_gender, and passport_full through it, draw
    from the process-wide random rather than the generator's; these draw from the generator, so
    the value's digest decides them."""
    from faker.providers import BaseProvider

    class SeededPassport(BaseProvider):
        def passport_gender(self) -> str:
            return self.generator.random.choices(["M", "F", "X"], weights=[0.493, 0.493, 0.014])[0]

        def passport_full(self) -> str:
            dob = self.generator.passport_dob()
            birth, issue, expiry = self.generator.passport_dates(dob)
            gender = self.passport_gender()
            given, surname = self.generator.passport_owner(gender=gender)
            number = self.generator.passport_number()
            return f"{given}\n{surname}\n{gender}\n{birth}\n{issue}\n{expiry}\n{number}\n"

    return SeededPassport


def new_generator() -> Any:
    """A generator of every fake method, deterministic in its seed (REQ-1494)."""
    from faker import Faker

    _pin_clock()
    gen = Faker(LOCALE)
    gen.add_provider(_seeded_passport())
    return gen


@lru_cache(maxsize=1)
def _generator() -> Any:
    return new_generator()


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
    if method.name in UNSUPPORTED:
        raise FakeRefused(f"{method.name}() makes values no column can hold")
    signature = inspect.signature(getattr(_generator(), method.name))
    args = dict(method.args)
    portable = ARGUMENTS.get(method.name, ())
    beyond = sorted(a for a in args if a not in portable)
    if beyond:
        raise FakeRefused(
            f"{method.name}() takes no argument {beyond[0]!r} on every engine"
            + (f"; it takes {', '.join(portable)}" if portable else "; it takes none")
        )
    accepted = {p.name for p in signature.parameters.values() if p.kind not in (p.VAR_POSITIONAL,)}
    takes_any = any(p.kind is p.VAR_KEYWORD for p in signature.parameters.values())
    unknown = sorted(set(args) - accepted) if not takes_any else []
    if unknown:
        raise FakeRefused(f"{method.name}() takes no argument {unknown[0]!r}")
    try:
        signature.bind(**args)
    except TypeError as exc:
        raise FakeRefused(f"{method.name}() cannot be called with {args!r}: {exc}") from exc
    sample = column_value(method.name, sample_of(method))
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
