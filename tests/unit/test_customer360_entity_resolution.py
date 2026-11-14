# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The customer360 demo's entity resolution (REQ-2034, amended 2026-11-14): the registry, the
matching job and the bridges it ships.

The registry (entity_resolution.ENTITY_SOURCES) is the one place a source is declared, its
templates applied as data over a row's own columns and refused when they name anything else. The
job matches each mention to its entity type's authority record, star-shaped, through govdata's
tiers: an organization by domain, normalised name, then Jaro-Winkler on the name past its first
word (high at 0.95, floor 0.85); a person by email, then within their resolved organization by last
name and first name. A mention matched to nothing keeps 'h:' || md5 of its source identity and says
why. The planted people and records behave as the demo tells them; a rerun gives the same ids; the
shipped bridges (db/er.sql) are the job's run over the shipped rows; and the hub's model scores sit
where the evidence says they should."""

# Requirements: REQ-2034
from __future__ import annotations

import hashlib
import importlib.util
import random
import sys
from pathlib import Path
from types import ModuleType

import pytest
import yaml

pytestmark = pytest.mark.unit

_DEMO = Path(__file__).resolve().parents[2] / "demo" / "named" / "customer360"
_CODE = _DEMO / "entity_resolution"


def _load(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # its dataclasses, and er_job's import, look it up by name
    spec.loader.exec_module(module)
    return module


ER = _load("entity_resolution", _CODE / "entity_resolution.py")
JOB = _load("er_job", _CODE / "er_job.py")
GEN = _load("customer360_generate", _DEMO / "generate.py")

ORG, PERSON = ER.ORGANIZATION, ER.PERSON
EntitySource, Key, OrgRef, Refused = ER.EntitySource, ER.Key, ER.OrgRef, ER.RegistryRefused


@pytest.fixture(scope="module")
def shipped():
    """The job's run over the rows the demo ships, and those rows."""
    rows, columns = JOB.read_replica_rows(_DEMO)
    return JOB.run(rows, columns, JOB.SEED_RUN_ID), rows, columns


def _by_label(rows: list[dict], source: str, label_start: str) -> dict:
    [row] = [r for r in rows if r["source_schema"] == source and r["label"].startswith(label_start)]
    return row


# --------------------------------------------------------------------------------------------- #
# Templates are data.                                                                           #
# --------------------------------------------------------------------------------------------- #


def test_a_template_formats_the_rows_own_columns_an_empty_one_as_nothing():
    row = {"Name": "Cobalt Freight", "Website": None}
    assert ER.render("{Name} ({Website})", row) == "Cobalt Freight ()"
    assert ER.render("{Name}", row) == "Cobalt Freight"


@pytest.mark.parametrize(
    "template",
    [
        "{Name.upper}",  # an attribute
        "{Name[0]}",  # an index
        "{Name!r}",  # a conversion
        "{Name:>20}",  # a format spec
        "{0}",  # a positional field
        "{}",  # an empty field
        "{__import__('os').system('true')}",  # code, which is never run
    ],
)
def test_a_template_field_that_is_not_a_bare_column_name_is_refused(template):
    with pytest.raises(Refused, match="template"):
        ER.template_fields(template)


def test_a_row_lacking_a_templates_column_is_refused_not_guessed():
    with pytest.raises(Refused, match="Website"):
        ER.render("{Name} ({Website})", {"Name": "Cobalt Freight"})


def _org(table: str, **kw):
    base = {"key": Key("id", "varchar"), "reference": {"name": "name"}}
    return EntitySource(entity_type=ORG, source="s", table=table, **{**base, **kw})


def _person(table: str, **kw):
    base = {
        "key": Key("id", "varchar"),
        "reference": {"first_name": "first", "last_name": "last", "email": "email"},
        "organization": OrgRef("s", "acct", "org"),
    }
    return EntitySource(entity_type=PERSON, source="s", table=table, **{**base, **kw})


def test_a_template_naming_a_column_the_table_lacks_is_refused_before_any_row_is_read():
    registry = [
        _org("acct", authority=True, label="{name} <{missing}>"),
        _person("contact", authority=True),
    ]
    rows = {("s", "acct"): [{"id": "1", "name": "A"}], ("s", "contact"): []}
    columns = {
        ("s", "acct"): ["id", "name"],
        ("s", "contact"): ["id", "first", "last", "email", "org"],
    }
    with pytest.raises(Refused, match="label 'missing'"):
        JOB.run(rows, columns, "r", registry)


def test_a_reference_or_key_column_the_table_lacks_is_refused():
    entry = _org("acct", reference={"name": "name", "domain": "site"})
    with pytest.raises(Refused, match="reference domain 'site'"):
        ER.check_columns(entry, ["id", "name"])
    with pytest.raises(Refused, match="key 'id'"):
        ER.check_columns(_org("acct"), ["name"])


def test_match_text_and_label_default_to_the_reference_names():
    assert _org("acct").match_template == "{name}"
    assert _org("acct").label_template == "{name}"
    assert _person("c").match_template == "{first} {last}"


# --------------------------------------------------------------------------------------------- #
# The registry.                                                                                 #
# --------------------------------------------------------------------------------------------- #


def test_the_shipped_registry_has_one_authority_per_entity_type_salesforce():
    ER.check_registry(ER.ENTITY_SOURCES)
    assert ER.authority(ORG).identity == "salesforce.Account"
    assert ER.authority(PERSON).identity == "salesforce.Contact"


def test_two_authorities_or_none_for_an_entity_type_are_refused():
    two = [_org("a", authority=True), _org("b", authority=True), _person("c", authority=True)]
    with pytest.raises(Refused, match="exactly one authority"):
        ER.check_registry(two)
    with pytest.raises(Refused, match="exactly one authority"):
        ER.check_registry([_org("a", authority=True), _person("c")])


def test_a_person_whose_organization_is_no_organization_entry_is_refused():
    registry = [
        _org("acct", authority=True),
        _person("c", authority=True, organization=OrgRef("s", "nope", "org")),
    ]
    with pytest.raises(Refused, match="no organization entry"):
        ER.check_registry(registry)


def test_an_entry_with_roles_its_algorithm_does_not_read_is_refused():
    with pytest.raises(Refused, match="not organization roles"):
        _org("a", reference={"name": "name", "first_name": "f"})
    with pytest.raises(Refused, match="first_name and last_name, or name"):
        _person("c", reference={"name": "n", "first_name": "f", "last_name": "l"})


def test_every_registry_entry_is_a_registered_table_and_its_key_a_column_of_its_type():
    config = yaml.safe_load((_DEMO / "config.yaml").read_text())
    tables = {
        (t["source_id"], t["table"]): {c["name"]: c for c in t["columns"]} for t in config["tables"]
    }
    for entry in ER.ENTITY_SOURCES:
        columns = tables[(entry.source, entry.table)]
        assert columns[entry.key.column]["data_type"] == entry.key.type, entry
        ER.check_columns(entry, columns)


# --------------------------------------------------------------------------------------------- #
# Similarity.                                                                                   #
# --------------------------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("a", "b", "expected"),
    [
        ("MARTHA", "MARHTA", 0.9611),
        ("DWAYNE", "DUANE", 0.84),
        ("DIXON", "DICKSONX", 0.8133),
        ("", "X", 0.0),
    ],
)
def test_jaro_winkler_gives_the_textbook_values(a, b, expected):
    assert JOB.jaro_winkler(a, b) == pytest.approx(expected, rel=1e-14, abs=5e-5)


@pytest.mark.parametrize(
    ("a", "b"),
    [
        ("JON", "JOHN"),
        ("KATHERINE", "CATHERINE"),
        ("JEFFERY", "JEFFREY"),
        ("ROBBERT", "ROBERT"),
        ("BOB", "ROBERT"),
        ("LIZ", "ELIZABETH"),
        ("mdia grp", "media"),
        ("logistcs grp", "logistics"),
    ],
)
def test_jaro_winkler_is_duckdbs_the_function_govdata_scores_with(a, b):
    import duckdb

    [(expected,)] = duckdb.sql(
        "SELECT jaro_winkler_similarity($a, $b)", params={"a": a, "b": b}
    ).fetchall()
    assert JOB.jaro_winkler(a, b) == pytest.approx(expected, rel=1e-14)


def test_an_organizations_name_is_normalised_as_govdata_normalises_it():
    assert JOB.norm_org_name("The Murfin Drilling Co., Inc.") == "murfin drilling"
    assert JOB.norm_org_name("Alder & Finch") == "alder and finch"
    assert JOB.norm_org_name("ferncliff-retail") == "ferncliff retail"


# --------------------------------------------------------------------------------------------- #
# The organization track, tier by tier.                                                         #
# --------------------------------------------------------------------------------------------- #

_ORG_REGISTRY = [
    _org("acct", authority=True, reference={"name": "name", "domain": "site"}, order_by="created"),
    _org("other", reference={"name": "name", "domain": "site", "email": "email"}),
    _person("contact", authority=True),
]


def _org_run(others: list[dict]) -> dict[tuple[str, str], dict]:
    accounts = [
        {
            "id": "A2",
            "name": "Cobalt Freight Inc.",
            "site": "https://www.cobaltfreight.com",
            "created": "2024-02-01",
        },
        {
            "id": "A1",
            "name": "Cobalt Freight US",
            "site": "http://cobaltfreight.com/us",
            "created": "2024-01-01",
        },
        {
            "id": "B1",
            "name": "Kestrel Aerospace",
            "site": "https://kestrelaero.com",
            "created": "2024-01-01",
        },
        {
            "id": "C1",
            "name": "Juniper Logistics",
            "site": "https://juniper.com",
            "created": "2024-01-01",
        },
        {
            "id": "D1",
            "name": "Northgate Foods",
            "site": "https://northgate.com",
            "created": "2024-01-01",
        },
        {
            "id": "D2",
            "name": "Northgate Foods Ltd",
            "site": "https://northgate.co.uk",
            "created": "2024-01-01",
        },
    ]
    other_cols = ["id", "name", "site", "email"]
    rows = {
        ("s", "acct"): accounts,
        ("s", "other"): [{c: o.get(c) for c in other_cols} for o in others],
        ("s", "contact"): [],
    }
    columns = {
        ("s", "acct"): ["id", "name", "site", "created"],
        ("s", "other"): other_cols,
        ("s", "contact"): ["id", "first", "last", "email", "org"],
    }
    bridges = JOB.run(rows, columns, "r1", _ORG_REGISTRY)
    return {(r["source_table"], r["source_key"]): r for r in bridges.org}


def _verdict(row: dict) -> tuple:
    return (row["canonical_id"], row["match_method"], row["match_confidence"], row["match_score"])


def test_an_authority_record_is_its_own_anchor_unless_an_earlier_one_shares_its_domain():
    got = _org_run([])
    assert _verdict(got[("acct", "A1")]) == ("A1", "authority", "high", 1.0)  # created first
    assert _verdict(got[("acct", "A2")]) == ("A1", "exact_domain", "high", 1.0)
    assert got[("acct", "A2")]["golden_source"] == "s.acct"


def test_a_web_domain_or_a_business_email_domain_is_an_exact_identifier():
    got = _org_run(
        [
            {"id": "o1", "name": "Anything", "site": "cobaltfreight.com"},
            {"id": "o2", "name": "Anything", "email": "ap@kestrelaero.com"},
        ]
    )
    assert _verdict(got[("other", "o1")]) == ("A1", "exact_domain", "high", 1.0)
    assert _verdict(got[("other", "o2")]) == ("B1", "exact_domain", "high", 1.0)


def test_a_personal_mailbox_is_no_identifier_but_the_name_still_matches():
    got = _org_run(
        [{"id": "o1", "name": "KESTREL AEROSPACE INC", "email": "kestrel.aero@gmail.com"}]
    )
    assert _verdict(got[("other", "o1")]) == ("B1", "exact_normalized", "high", 1.0)


def test_a_fuzzy_name_scores_on_what_follows_its_first_word_high_low_or_below_the_floor():
    got = _org_run(
        [
            {"id": "hi", "name": "Kestrel Aerospce"},  # aerospce/aerospace 0.9778
            {"id": "lo", "name": "Juniper Logistcs Grp"},  # logistcs grp/logistics
            {"id": "no", "name": "Kestrel Arspc Grp"},  # arspc grp/aerospace
        ]
    )
    hi, lo, no = got[("other", "hi")], got[("other", "lo")], got[("other", "no")]
    assert (hi["match_method"], hi["match_confidence"], hi["canonical_id"]) == (
        "fuzzy",
        "high",
        "B1",
    )
    assert hi["match_score"] >= JOB.HIGH
    assert (lo["match_method"], lo["match_confidence"], lo["canonical_id"]) == (
        "fuzzy",
        "low",
        "C1",
    )
    assert JOB.FLOOR <= lo["match_score"] < JOB.HIGH
    assert no["match_confidence"] == "unresolved" and no["unresolved_reason"] == "below_floor"
    assert no["candidate_id"] == "B1" and no["candidate_score"] < JOB.FLOOR
    assert no["canonical_id"].startswith("h:") and no["golden_source"] is None


def test_a_name_no_block_holds_has_no_candidate_and_two_equal_anchors_are_ambiguous():
    got = _org_run(
        [
            {"id": "x", "name": "Harbor Lane Bakery", "email": "ap@harborlanebakery.com"},
            {"id": "amb", "name": "Northgate Foods"},  # D1 and D2, two domains, one name
        ]
    )
    assert got[("other", "x")]["unresolved_reason"] == "no_candidate"
    assert got[("other", "x")]["candidate_id"] is None
    assert got[("other", "amb")]["unresolved_reason"] == "ambiguous"
    assert got[("other", "amb")]["candidate_id"] == "D1"


def test_a_placeholder_or_a_personal_mailbox_with_nothing_to_match_is_excluded():
    got = _org_run(
        [
            {"id": "tbd", "name": "TBD"},
            {"id": "gm", "name": "Tessa Rowe Design", "email": "tessa@gmail.com"},
        ]
    )
    assert got[("other", "tbd")]["unresolved_reason"] == "excluded"
    assert got[("other", "gm")]["unresolved_reason"] == "excluded"


def test_an_unmatched_mention_keeps_the_md5_of_its_source_identity():
    got = _org_run([{"id": "tbd", "name": "TBD"}])
    expected = "h:" + hashlib.md5(b"s.other.id.tbd").hexdigest()  # noqa: S324 -- the id's formula
    assert got[("other", "tbd")]["canonical_id"] == expected


# --------------------------------------------------------------------------------------------- #
# The person track, tier by tier.                                                               #
# --------------------------------------------------------------------------------------------- #

_PERSON_REGISTRY = [
    _org("acct", authority=True),
    _person("contact", authority=True),
    _person("user"),
]


def _person_run(users: list[dict]) -> dict[tuple[str, str], dict]:
    accounts = [{"id": "A", "name": "Cobalt Freight"}, {"id": "B", "name": "Juniper Logistics"}]
    contacts = [
        {
            "id": "c1",
            "first": "John",
            "last": "Smith",
            "email": "john.smith@cobalt.com",
            "org": "A",
        },
        {"id": "c2", "first": "Maria", "last": "Lopez", "email": "maria@cobalt.com", "org": "A"},
        {"id": "c3", "first": "Maria", "last": "Lopez", "email": "mlopez@juniper.com", "org": "B"},
        {"id": "c4", "first": "Robert", "last": "Harlan", "email": "rh@cobalt.com", "org": "A"},
        {"id": "c5", "first": "Ann", "last": "Lee", "email": "ann@cobalt.com", "org": "A"},
        {"id": "c6", "first": "Ann", "last": "Lee", "email": "ann.lee@cobalt.com", "org": "A"},
    ]
    person_cols = ["id", "first", "last", "email", "org"]
    rows = {("s", "acct"): accounts, ("s", "contact"): contacts, ("s", "user"): users}
    columns = {
        ("s", "acct"): ["id", "name"],
        ("s", "contact"): person_cols,
        ("s", "user"): person_cols,
    }
    bridges = JOB.run(rows, columns, "r1", _PERSON_REGISTRY)
    return {(r["source_table"], r["source_key"]): r for r in bridges.person}


def _user(key: str, first: str, last: str, email: str, org: str | None) -> dict:
    return {"id": key, "first": first, "last": last, "email": email, "org": org}


def test_a_contact_is_its_own_golden_record_in_its_accounts_context():
    got = _person_run([])
    assert _verdict(got[("contact", "c1")]) == ("c1", "authority", "high", 1.0)
    assert got[("contact", "c1")]["org_canonical_id"] == "A"


def test_a_persons_tiers_exact_email_then_names_within_their_organization():
    got = _person_run(
        [
            _user("e", "Johnny", "Smyth", "JOHN.SMITH@cobalt.com", None),  # email, any case
            _user("n", "John", "Smith", "jsmith@cobalt.com", "A"),  # name exact in org
            _user("h", "Robbert", "Harlan", "x@cobalt.com", "A"),  # ROBBERT/ROBERT 0.9667
            _user("l", "Jon", "Smith", "y@cobalt.com", "A"),  # JON/JOHN 0.9333
            _user("b", "Bob", "Harlan", "z@cobalt.com", "A"),  # BOB/ROBERT 0.5
        ]
    )
    assert _verdict(got[("user", "e")]) == ("c1", "exact_email", "high", 1.0)
    assert _verdict(got[("user", "n")]) == ("c1", "exact_normalized", "high", 1.0)
    assert _verdict(got[("user", "h")]) == ("c4", "fuzzy", "high", 0.9667)
    assert _verdict(got[("user", "l")]) == ("c1", "fuzzy", "low", 0.9333)
    bob = got[("user", "b")]
    assert (bob["unresolved_reason"], bob["candidate_id"], bob["candidate_score"]) == (
        "below_floor",
        "c4",
        0.5,
    )


def test_the_same_name_at_two_organizations_is_two_people():
    got = _person_run(
        [
            _user("u1", "Maria", "Lopez", "m1@x.com", "A"),
            _user("u2", "Maria", "Lopez", "m2@x.com", "B"),
        ]
    )
    assert got[("user", "u1")]["canonical_id"] == "c2"
    assert got[("user", "u2")]["canonical_id"] == "c3"


def test_a_person_with_no_resolved_organization_has_no_candidate():
    got = _person_run([_user("u", "Maria", "Lopez", "m@x.com", "nowhere")])
    assert got[("user", "u")]["unresolved_reason"] == "no_candidate"
    assert got[("user", "u")]["org_canonical_id"] is None


def test_two_contacts_of_one_name_in_one_organization_are_ambiguous():
    got = _person_run([_user("u", "Ann", "Lee", "al@x.com", "A")])
    assert got[("user", "u")]["unresolved_reason"] == "ambiguous"
    assert got[("user", "u")]["candidate_id"] == "c5"


# --------------------------------------------------------------------------------------------- #
# The planted cases, in the rows the demo ships.                                                #
# --------------------------------------------------------------------------------------------- #


def _contact(rows: dict, first: str, last: str, email: str) -> str:
    [found] = [
        r["Id"]
        for r in rows[("salesforce", "Contact")]
        if (r["FirstName"], r["LastName"], r["Email"]) == (first, last, email)
    ]
    return found


def _account(rows: dict, name: str) -> str:
    [found] = [r["Id"] for r in rows[("salesforce", "Account")] if r["Name"] == name]
    return found


def test_one_person_in_two_spellings_and_address_formats_matches(shipped):
    bridges, rows, _ = shipped
    john = _contact(rows, "John", "Smith", "john.smith@cobaltfreight.com")
    jon = _by_label(bridges.person, "servicenow", "Jon Smith <jsmith@")
    assert (jon["canonical_id"], jon["match_method"], jon["match_confidence"]) == (
        john,
        "fuzzy",
        "low",
    )
    reporter = _by_label(bridges.person, "jira", "John Smith <jsmith@")
    assert (reporter["canonical_id"], reporter["match_method"]) == (john, "exact_normalized")
    jeffery = _by_label(bridges.person, "servicenow", "Jeffery Marsh")
    assert jeffery["canonical_id"] == _contact(
        rows, "Jeffrey", "Marsh", "jeffrey.marsh@kestrelaero.com"
    )
    assert (jeffery["match_method"], jeffery["match_confidence"]) == ("fuzzy", "high")
    catherine = _by_label(bridges.person, "servicenow", "Catherine Doyle")
    assert catherine["canonical_id"] == _contact(
        rows, "Katherine", "Doyle", "katherine.doyle@ironbarkenergy.com"
    )
    assert (catherine["match_method"], catherine["match_confidence"]) == ("fuzzy", "low")


@pytest.mark.parametrize(
    ("nickname", "first", "last", "email"),
    [
        ("Bob Harlan", "Robert", "Harlan", "robert.harlan@marloweins.com"),
        ("Liz Moreno", "Elizabeth", "Moreno", "elizabeth.moreno@lumendental.com"),
    ],
)
def test_a_nickname_falls_below_the_floor_and_is_unresolved(shipped, nickname, first, last, email):
    bridges, rows, _ = shipped
    row = _by_label(bridges.person, "servicenow", nickname)
    assert row["match_confidence"] == "unresolved" and row["unresolved_reason"] == "below_floor"
    assert row["candidate_id"] == _contact(rows, first, last, email)
    assert row["candidate_score"] < JOB.FLOOR
    assert row["canonical_id"].startswith("h:")


def test_the_two_maria_lopezes_stay_two_people_each_in_her_own_organization(shipped):
    bridges, rows, _ = shipped
    cobalt = _contact(rows, "Maria", "Lopez", "maria.lopez@cobaltfreight.com")
    juniper = _contact(rows, "Maria", "Lopez", "mlopez@juniperlogistics.com")
    assert cobalt != juniper
    at_juniper = _by_label(
        bridges.person, "servicenow", "Maria Lopez <maria.lopez@juniperlogistics.com>"
    )
    assert (at_juniper["canonical_id"], at_juniper["match_method"]) == (juniper, "exact_normalized")
    assert at_juniper["org_canonical_id"] == _account(rows, "Juniper Logistics")
    at_cobalt = _by_label(
        bridges.person, "servicenow", "Maria Lopez <maria.lopez@cobaltfreight.com>"
    )
    assert at_cobalt["canonical_id"] == cobalt
    billing = _by_label(bridges.person, "stripe", "Maria Lopez")
    assert (billing["canonical_id"], billing["match_method"]) == (juniper, "exact_normalized")


def test_the_person_who_moved_resolves_in_two_organizations(shipped):
    bridges, rows, _ = shipped
    old = _contact(rows, "Daniel", "Okafor", "daniel.okafor@graniteridgebank.com")
    new = _contact(rows, "Daniel", "Okafor", "dokafor@vantagepayroll.com")
    at_granite = _by_label(bridges.person, "servicenow", "Daniel Okafor")
    at_vantage = _by_label(bridges.person, "jira", "Daniel Okafor")
    assert at_granite["canonical_id"] == old and at_vantage["canonical_id"] == new
    assert at_granite["org_canonical_id"] == _account(rows, "Granite Ridge Bank")
    assert at_vantage["org_canonical_id"] == _account(rows, "Vantage Payroll")


@pytest.mark.parametrize(
    ("source", "label", "reason", "candidate"),
    [
        ("stripe", "Harbor Lane Bakery", "no_candidate", None),
        ("servicenow", "Halcyon Mdia Grp", "below_floor", "Halcyon Media"),
        ("stripe", "Tessa Rowe Design", "excluded", None),
        ("jira", "TBD", "excluded", None),
    ],
)
def test_the_four_planted_records_are_unresolved_with_their_reason(
    shipped, source, label, reason, candidate
):
    bridges, rows, _ = shipped
    row = _by_label(bridges.org, source, label)
    assert (row["match_confidence"], row["unresolved_reason"]) == ("unresolved", reason)
    assert row["candidate_id"] == (candidate and _account(rows, candidate))
    assert row["canonical_id"].startswith("h:") and row["golden_source"] is None
    if reason == "below_floor":
        assert row["candidate_score"] == pytest.approx(0.79, rel=1e-14)


def test_a_personal_mailbox_customer_named_like_its_account_is_matched_by_name(shipped):
    bridges, rows, _ = shipped
    saltmarsh = _by_label(bridges.org, "stripe", "Saltmarsh Brewing Co")
    assert (saltmarsh["canonical_id"], saltmarsh["match_method"]) == (
        _account(rows, "Saltmarsh Brewing"),
        "exact_normalized",
    )


def test_the_shipped_people_number_thirty_to_sixty(shipped):
    bridges, rows, _ = shipped
    people = {r["canonical_id"] for r in bridges.person}
    assert 30 <= len(people) <= 60


# --------------------------------------------------------------------------------------------- #
# Reruns and the shipped bridges.                                                               #
# --------------------------------------------------------------------------------------------- #


def _ids(rows: list[dict]) -> list[tuple]:
    return [tuple(v for k, v in r.items() if k != "match_run_id") for r in rows]


def test_a_rerun_over_unchanged_rows_gives_the_same_canonical_ids_in_any_read_order(shipped):
    first, rows, columns = shipped
    shuffled = {k: random.Random(7).sample(list(v), len(v)) for k, v in rows.items()}
    second = JOB.run(shuffled, columns, "er-2026-11-14T00:00:00Z")
    assert _ids(second.org) == _ids(first.org)
    assert _ids(second.person) == _ids(first.person)
    assert {r["match_run_id"] for r in second.org + second.person} == {"er-2026-11-14T00:00:00Z"}


def test_the_shipped_bridges_are_the_jobs_run_over_the_shipped_rows(shipped):
    bridges, _, _ = shipped
    assert (_DEMO / "db" / "er.sql").read_text(encoding="utf-8") == JOB.seed_sql(bridges)


def test_every_mention_of_every_registry_entry_is_one_bridge_row(shipped):
    bridges, rows, _ = shipped
    for entry in ER.ENTITY_SOURCES:
        expected = {m.key for m in JOB.mentions(entry, rows[(entry.source, entry.table)])}
        bridge = bridges.org if entry.entity_type == ORG else bridges.person
        got = [
            r["source_key"]
            for r in bridge
            if (r["source_schema"], r["source_table"]) == (entry.source, entry.table)
        ]
        assert sorted(got) == sorted(expected) and len(got) == len(set(got)), entry.identity


# --------------------------------------------------------------------------------------------- #
# The hub's model scores, against the evidence.                                                 #
# --------------------------------------------------------------------------------------------- #

_TABLE = {
    "salesforce": "Account",
    "servicenow": "core_company",
    "jira": "bug",
    "stripe": "get_customers",
}
_PERSON_TABLE = {
    "salesforce": "Contact",
    "servicenow": "sys_user",
    "jira": "bug",
    "stripe": "get_customers",
}


def _model_links(shipped) -> list[tuple[str, float, bool]]:
    """Each model link the hub holds, (entity type, score, whether the evidence confirms it), for the
    links the evidence resolves: a link is confirmed when the evidence matches its record to the
    golden record the hub's master is anchored on."""
    bridges, _, _ = shipped
    hub = GEN.build(GEN.AS_OF)
    org = {(r["source_schema"], r["source_table"], r["source_key"]): r for r in bridges.org}
    person = {(r["source_schema"], r["source_table"], r["source_key"]): r for r in bridges.person}
    master_anchor = {
        x["master_id"]: org[("salesforce", "Account", x["system_key"])]["canonical_id"]
        for x in hub.xref
        if x["system"] == "salesforce"
    }
    person_anchor = {
        x["person_master_id"]: x["system_key"]
        for x in hub.xref_person
        if x["system"] == "salesforce"
    }
    out = []
    for x in hub.xref:
        evidence = org.get((x["system"], _TABLE[x["system"]], x["system_key"]))
        if (
            x["match_method"] == "model"
            and evidence
            and evidence["match_confidence"] != "unresolved"
        ):
            out.append(
                (
                    ORG,
                    x["match_score"],
                    evidence["canonical_id"] == master_anchor.get(x["master_id"]),
                )
            )
    for x in hub.xref_person:
        evidence = person.get((x["system"], _PERSON_TABLE[x["system"]], x["system_key"]))
        if (
            x["match_method"] == "model"
            and evidence
            and evidence["match_confidence"] != "unresolved"
        ):
            out.append(
                (
                    PERSON,
                    x["match_score"],
                    evidence["canonical_id"] == person_anchor[x["person_master_id"]],
                )
            )
    return out


def test_the_models_wrong_links_score_from_0_60_to_0_80_one_of_them_a_persons(shipped):
    links = _model_links(shipped)
    wrong = [(kind, score) for kind, score, confirmed in links if not confirmed]
    assert {kind for kind, _ in wrong} == {ORG, PERSON}
    assert len(wrong) >= 4  # the Larkspur swap's two and at least two people
    assert all(0.60 <= score <= 0.80 for _, score in wrong), wrong


def test_the_links_the_evidence_confirms_mostly_score_above_0_90_and_some_low(shipped):
    for kind in (ORG, PERSON):
        confirmed = [score for k, score, ok in _model_links(shipped) if k == kind and ok]
        assert confirmed, kind
        above = [s for s in confirmed if s > 0.90]
        assert len(above) / len(confirmed) >= 0.9, (kind, confirmed)
    low_but_right = [s for _, s, ok in _model_links(shipped) if ok and s < 0.80]
    assert 1 <= len(low_but_right) <= 2


def test_every_hub_link_carries_its_method_and_only_a_models_its_score(shipped):
    hub = GEN.build(GEN.AS_OF)
    for x in hub.xref + hub.xref_person:
        assert x["match_method"] in ("rule", "steward", "model")
        is_model = x["match_method"] == "model"
        assert (x["match_score"] is not None) == is_model
        assert x["model_version"] == ("match-v2" if is_model else None)
        assert is_model == (x["linked_by"] == "bulk import")
    assert {x["linked_at"] for x in hub.xref if x["match_method"] == "model"} == {
        "2026-08-14 02:30:00"
    }
