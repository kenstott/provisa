# Copyright (c) 2026 Kenneth Stott
# Canary: b021e0ed-83c6-4f6c-bd7b-14bf4f8ce3f7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Write the customer360 demo's data: what Halyard Software's four SaaS systems hold (the rows the
demo declares for them, replica_rows/) and its customer master hub (db/init.sql):

    .venv/bin/python3 demo/named/customer360/generate.py [--as-of YYYY-MM-DD]

Every date is relative to AS_OF, the day the hub last synced from the four systems (mdm.sync_run):
the views measure "this quarter" and "overdue" from that day, so the renewals this quarter and the
overdue invoices are there whenever the rows are read. Deterministic: the same rows come out on
every run for the same day.

Each system keeps its own id for a customer and shares none:

  salesforce  Account (001...) and its Opportunity rows: one renewal a year per account
  servicenow  core_company (sys_id) and incident rows that name it (company)
  jira        the ENG project's Bug form: its Customer field (customfield_10050) holds the
              customer's name as the support engineer typed it
  stripe      customers (cus_...) and their invoices (in_..., customer)

The hub (schema mdm) holds master customers and a crosswalk from each master to each system's id.
It is the system of record, and it is planted wrong in the ways real hubs are:

  * Larkspur Analytics and Larkspur Labs had their Stripe customers swapped by the August bulk
    import. Larkspur Analytics renews this quarter and its October invoice is overdue, but the hub
    links it to Larkspur Labs' Stripe customer, so the hub's answer misses it (the reveal).
  * Brightwater Health's ServiceNow company and Willow Creek Clinics' Jira customer sit under a
    second master each: duplicate customers.
  * Xylo Robotics' Jira customer, Alder & Finch's ServiceNow company, Bramble Software's Stripe
    customer, and the records that replaced Tamarack's and Yarrow's are in no master: coverage gaps.
  * The hub still points at Tamarack Mining's deleted ServiceNow company and Yarrow Insurance
    Brokers' deleted Stripe customer: stale links.

The four systems hold people too (REQ-2034, amended 2026-11-14): Salesforce contacts, ServiceNow
users (each incident's caller), the person who raised each Jira bug through the support portal,
and the billing contact on a Stripe customer. The hub keeps a master person for each contact and
links the other systems' people to it. The August bulk import was a matching model's run
(match-v2): it linked every Stripe customer and every person outside Salesforce, each link with
its score. The people are planted the way real ones go wrong:

  * Cobalt Freight's John Smith is Jon Smith at jsmith@ in ServiceNow, Kestrel's Jeffrey Marsh is
    Jeffery Marsh at jmarsh@ and Ironbark's Katherine Doyle is Catherine Doyle at cdoyle@: one
    person each, written differently.
  * Marlowe's Robert Harlan is Bob in ServiceNow and Lumen Dental's Elizabeth Moreno is Liz: a
    nickname no string similarity reaches.
  * Two Maria Lopezes, one at Cobalt Freight and one at Juniper Logistics, are two people. The
    model linked Juniper's to Cobalt's.
  * Daniel Okafor moved from Granite Ridge Bank to Vantage Payroll: a contact at each. The model
    linked his new Jira reporter address to his old master.

And four records no rule can give a customer: Harbor Lane Bakery, billed in Stripe with no
Salesforce account; "Halcyon Mdia Grp", a ServiceNow company typed by the desk; Tessa Rowe Design,
a Stripe customer on a personal mailbox; and a Jira bug whose Customer field says "TBD".

One organization is misspelled where the fuzzy tier still finds it: Redfern Hotels' Jira bugs say
"Redfern Hottles", a match below the high band that is flagged low.
"""

from __future__ import annotations

import argparse
import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path

HERE = Path(__file__).parent
ROWS = HERE / "replica_rows"
#: The day the hub last synced from the four systems; the data is written relative to it.
AS_OF = date(2026, 11, 7)

ACCOUNT_COLUMNS = ["Id", "Name", "Website", "Industry", "Type", "BillingCountry", "CreatedDate"]
OPPORTUNITY_COLUMNS = [
    "Id",
    "AccountId",
    "Name",
    "Type",
    "StageName",
    "Amount",
    "CloseDate",
    "IsClosed",
]
COMPANY_COLUMNS = ["sys_id", "name", "website", "city", "country", "customer", "sys_created_on"]
INCIDENT_COLUMNS = [
    "sys_id",
    "number",
    "short_description",
    "state",
    "priority",
    "opened_at",
    "resolved_at",
    "company",
    "company__display",
    "correlation_id",
    "caller_id",
]
BUG_COLUMNS = [
    "key",
    "id",
    "summary",
    "status",
    "created",
    "updated",
    "priority",
    "customer",
    "reporter_name",
    "reporter_email",
]
CUSTOMER_COLUMNS = [
    "id",
    "name",
    "email",
    "created",
    "currency",
    "delinquent",
    "description",
    "billing_contact_name",
]
CONTACT_COLUMNS = ["Id", "AccountId", "FirstName", "LastName", "Email", "Title", "CreatedDate"]
SYS_USER_COLUMNS = ["sys_id", "first_name", "last_name", "email", "company", "sys_created_on"]
INVOICE_COLUMNS = [
    "id",
    "customer",
    "number",
    "status",
    "amount_due",
    "amount_remaining",
    "currency",
    "created",
    "due_date",
    "collection_method",
]

# Renewal: 0 this quarter, 1 next quarter, 2 the one after.
THIS_Q, NEXT_Q, LATER_Q = 0, 1, 2


@dataclass
class Company:
    """One real customer of Halyard Software, and how each system spells it."""

    key: str
    name: str  # Salesforce's spelling: the account name
    website: str
    industry: str
    country: str
    renews: int  # the quarter its renewal closes in, from AS_OF's
    amount: int  # its annual contract value, USD
    snow_name: str | None = None  # ServiceNow's company name; None: no ServiceNow company
    snow_website: str | None = None  # empty: matched by name only
    jira_names: tuple[str, ...] = ()  # the Customer field's spellings on its bugs
    stripe_name: str | None = None  # None: no Stripe customer
    stripe_email: str | None = None
    open_p1: int = 0  # open P1 incidents
    blocked: int = 0  # blocked Jira fixes
    overdue: int = 0  # overdue Stripe invoices
    extra_accounts: tuple[
        tuple[str, str], ...
    ] = ()  # (name, website) duplicate Salesforce accounts
    tags: set[str] = field(default_factory=set)


COMPANIES = [
    Company(
        "larkspur-analytics",
        "Larkspur Analytics",
        "https://www.larkspuranalytics.com",
        "Technology",
        "US",
        THIS_Q,
        184000,
        "Larkspur Analytics Inc.",
        "larkspuranalytics.com",
        ("Larkspur Analytics",),
        "LARKSPUR ANALYTICS INC",
        "ap@larkspuranalytics.com",
        overdue=1,
    ),
    Company(
        "larkspur-labs",
        "Larkspur Labs",
        "https://larkspurlabs.io",
        "Biotechnology",
        "US",
        NEXT_Q,
        96000,
        "Larkspur Labs",
        "https://larkspurlabs.io",
        ("larkspur labs",),
        "Larkspur Labs",
        "billing@larkspurlabs.io",
    ),
    Company(
        "brightwater",
        "Brightwater Health",
        "https://www.brightwaterhealth.com",
        "Healthcare",
        "US",
        NEXT_Q,
        212000,
        "Brightwater Health, Inc.",
        "https://brightwaterhealth.com",
        ("Brightwater Health",),
        "Brightwater Health",
        "finance@brightwaterhealth.com",
    ),
    Company(
        "cobalt",
        "Cobalt Freight",
        "https://www.cobaltfreight.com",
        "Transportation",
        "US",
        THIS_Q,
        158000,
        "Cobalt Freight Inc.",
        "https://www.cobaltfreight.com",
        ("Cobalt Freight",),
        "COBALT FREIGHT LLC",
        "ap@cobaltfreight.com",
        open_p1=1,
    ),
    Company(
        "ferncliff",
        "Ferncliff Retail",
        "https://ferncliff.co",
        "Retail",
        "GB",
        THIS_Q,
        77000,
        "Ferncliff Retail Ltd",
        "https://ferncliff.co",
        ("ferncliff-retail", "Ferncliff Retail"),
        "Ferncliff Retail Ltd",
        "accounts@ferncliff.co",
        blocked=1,
    ),
    Company(
        "granite",
        "Granite Ridge Bank",
        "https://www.graniteridgebank.com",
        "Banking",
        "US",
        THIS_Q,
        265000,
        "Granite Ridge Bank",
        "https://graniteridgebank.com",
        ("Granite Ridge Bank",),
        "Granite Ridge Bank",
        "payables@graniteridgebank.com",
        overdue=1,
    ),
    Company(
        "halcyon",
        "Halcyon Media",
        "https://halcyonmedia.tv",
        "Media",
        "US",
        THIS_Q,
        64000,
        "Halcyon Media",
        "https://halcyonmedia.tv",
        ("Halcyon Media",),
        "Halcyon Media",
        "billing@halcyonmedia.tv",
    ),
    Company(
        "ironbark",
        "Ironbark Energy",
        "https://www.ironbarkenergy.com",
        "Energy",
        "AU",
        THIS_Q,
        231000,
        "Ironbark Energy Pty Ltd",
        "https://ironbarkenergy.com",
        ("Ironbark Energy",),
        "Ironbark Energy",
        "ap@ironbarkenergy.com",
        open_p1=1,
        blocked=1,
    ),
    Company(
        "juniper",
        "Juniper Logistics",
        "https://juniperlogistics.com",
        "Transportation",
        "US",
        THIS_Q,
        88000,
        "Juniper Logistics",
        "https://juniperlogistics.com",
        ("Juniper Logistics",),
        "Juniper Logistics",
        "ap@juniperlogistics.com",
    ),
    Company(
        "kestrel",
        "Kestrel Aerospace",
        "https://kestrelaero.com",
        "Aerospace",
        "US",
        THIS_Q,
        305000,
        "Kestrel Aerospace Corp.",
        "https://kestrelaero.com",
        ("Kestrel Aerospace",),
        "Kestrel Aerospace Corporation",
        "billing@kestrelaero.com",
    ),
    Company(
        "lumen",
        "Lumen Dental Group",
        "https://www.lumendental.com",
        "Healthcare",
        "CA",
        THIS_Q,
        59000,
        "Lumen Dental Group",
        "https://lumendental.com",
        ("Lumen Dental",),
        "Lumen Dental Group",
        "office@lumendental.com",
        open_p1=1,
        overdue=1,
    ),
    Company(
        "marlowe",
        "Marlowe Insurance",
        "https://marloweins.com",
        "Insurance",
        "US",
        NEXT_Q,
        143000,
        "Marlowe Insurance",
        "https://marloweins.com",
        ("Marlowe Insurance",),
        "Marlowe Insurance",
        "ap@marloweins.com",
        open_p1=1,
        blocked=1,
    ),
    Company(
        "northgate",
        "Northgate Foods",
        "https://northgatefoods.com",
        "Food & Beverage",
        "US",
        THIS_Q,
        71000,
        "Northgate Foods",
        "https://northgatefoods.com",
        ("Northgate Foods",),
        "Northgate Foods",
        "ap@northgatefoods.com",
    ),
    Company(
        "oakmont",
        "Oakmont Legal",
        "https://oakmontlegal.com",
        "Legal",
        "US",
        LATER_Q,
        52000,
        "Oakmont Legal LLP",
        "https://oakmontlegal.com",
        (),
        "Oakmont Legal LLP",
        "billing@oakmontlegal.com",
    ),
    Company(
        "pinecrest",
        "Pinecrest Schools",
        "https://pinecrestschools.org",
        "Education",
        "US",
        THIS_Q,
        46000,
        "Pinecrest Schools",
        "https://pinecrestschools.org",
        ("Pinecrest Schools",),
        "Pinecrest Schools",
        "finance@pinecrestschools.org",
        blocked=1,
    ),
    Company(
        "quillon",
        "Quillon Systems",
        "https://quillon.com",
        "Technology",
        "DE",
        LATER_Q,
        118000,
        "Quillon Systems GmbH",
        "https://quillon.com",
        ("Quillon Systems",),
        "Quillon Systems GmbH",
        "rechnung@quillon.com",
        extra_accounts=(("Quillon Systems Inc.", "http://www.quillon.com/us"),),
    ),
    Company(
        "redfern",
        "Redfern Hotels",
        "https://redfernhotels.com",
        "Hospitality",
        "GB",
        NEXT_Q,
        99000,
        "Redfern Hotels",
        "https://redfernhotels.com",
        ("Redfern Hottles",),  # a misspelling the fuzzy tier matches low
        "Redfern Hotels",
        "accounts@redfernhotels.com",
    ),
    # Its Stripe customer was opened with the owner's own address: matched by name.
    Company(
        "saltmarsh",
        "Saltmarsh Brewing",
        "https://saltmarshbrewing.com",
        "Food & Beverage",
        "US",
        THIS_Q,
        28000,
        "Saltmarsh Brewing Co.",
        "https://saltmarshbrewing.com",
        ("Saltmarsh Brewing",),
        "Saltmarsh Brewing Co",
        "saltmarsh.brewing@gmail.com",
    ),
    Company(
        "tamarack",
        "Tamarack Mining",
        "https://tamarackmining.com",
        "Mining",
        "CA",
        LATER_Q,
        176000,
        "Tamarack Mining Corp",
        "https://tamarackmining.com",
        ("Tamarack Mining",),
        "Tamarack Mining",
        "ap@tamarackmining.com",
        tags={"stale-servicenow"},
    ),
    Company(
        "umber",
        "Umber Architecture",
        "https://umberarch.com",
        "Architecture",
        "US",
        LATER_Q,
        34000,
        "Umber Architecture",
        "https://umberarch.com",
        (),
        "Umber Architecture",
        "studio@umberarch.com",
    ),
    Company(
        "vantage",
        "Vantage Payroll",
        "https://vantagepayroll.com",
        "Financial Services",
        "US",
        THIS_Q,
        137000,
        "Vantage Payroll",
        "https://vantagepayroll.com",
        ("Vantage Payroll",),
        "Vantage Payroll",
        "ap@vantagepayroll.com",
        overdue=1,
    ),
    Company(
        "willow",
        "Willow Creek Clinics",
        "https://willowcreekclinics.org",
        "Healthcare",
        "US",
        NEXT_Q,
        81000,
        "Willow Creek Clinics",
        "https://willowcreekclinics.org",
        ("Willow Creek Clinics",),
        "Willow Creek Clinics",
        "billing@willowcreekclinics.org",
        tags={"duplicate-jira"},
    ),
    Company(
        "xylo",
        "Xylo Robotics",
        "https://xylorobotics.ai",
        "Robotics",
        "US",
        NEXT_Q,
        112000,
        None,
        None,
        ("Xylo Robotics",),
        "Xylo Robotics",
        "ap@xylorobotics.ai",
        tags={"gap-jira"},
    ),
    Company(
        "yarrow",
        "Yarrow Insurance Brokers",
        "https://yarrowbrokers.com",
        "Insurance",
        "GB",
        LATER_Q,
        67000,
        "Yarrow Insurance Brokers",
        "https://yarrowbrokers.com",
        (),
        "Yarrow Insurance Brokers Ltd",
        "accounts@yarrowbrokers.com",
        tags={"stale-stripe"},
    ),
    # An open P1 this quarter, already resolved: no risk.
    Company(
        "zephyr",
        "Zephyr Telecom",
        "https://zephyrtel.com",
        "Telecommunications",
        "US",
        THIS_Q,
        189000,
        "Zephyr Telecom",
        "https://zephyrtel.com",
        ("Zephyr Telecom",),
        "Zephyr Telecom",
        "ap@zephyrtel.com",
    ),
    Company(
        "alder",
        "Alder & Finch",
        "https://alderfinch.com",
        "Professional Services",
        "US",
        NEXT_Q,
        41000,
        "Alder and Finch",
        "https://alderfinch.com",
        ("Alder & Finch",),
        "Alder & Finch",
        "ap@alderfinch.com",
        tags={"gap-servicenow"},
    ),
    Company(
        "bramble",
        "Bramble Software",
        "https://bramblesoftware.io",
        "Technology",
        "US",
        LATER_Q,
        73000,
        "Bramble Software",
        "https://bramblesoftware.io",
        (),
        "Bramble Software Inc",
        "ar@bramblesoftware.io",
        tags={"gap-stripe"},
    ),
    Company(
        "cinder",
        "Cinder Studios",
        "https://cinderstudios.com",
        "Media",
        "US",
        LATER_Q,
        38000,
        None,
        None,
        ("Cinder Studios",),
        "Cinder Studios",
        "money@cinderstudios.com",
    ),
    Company(
        "driftwood",
        "Driftwood Travel",
        "https://driftwoodtravel.com",
        "Travel",
        "NZ",
        NEXT_Q,
        55000,
        "Driftwood Travel",
        "https://driftwoodtravel.com",
        (),
        "Driftwood Travel",
        "accounts@driftwoodtravel.com",
    ),
    Company(
        "elmstead",
        "Elmstead Manufacturing",
        "https://www.elmstead.com",
        "Manufacturing",
        "US",
        THIS_Q,
        214000,
        "Elmstead Manufacturing Co.",
        "https://elmstead.com",
        ("Elmstead Manufacturing",),
        "Elmstead Manufacturing",
        "ap@elmstead.com",
    ),
]


@dataclass
class Person:
    """One person at a customer, and how each system holds them."""

    company: str  # the Company.key they work at
    first: str  # Salesforce's spelling: the contact's
    last: str
    email: str
    title: str
    snow: tuple[str, str, str] | None = None  # ServiceNow user (first, last, email); None: none
    jira: tuple[str, str] | None = None  # the portal's reporter (name, email) on the company's bugs
    stripe: bool = False  # the billing contact on the company's Stripe customer
    caller: bool = False  # raises the company's open P1 incidents
    sf: bool = True  # a Salesforce contact
    joined: int = 0  # days before AS_OF the contact was created; 0: with the account


PEOPLE = [
    Person(
        "cobalt",
        "Maria",
        "Lopez",
        "maria.lopez@cobaltfreight.com",
        "VP Operations",
        snow=("Maria", "Lopez", "maria.lopez@cobaltfreight.com"),
        caller=True,
    ),
    # One person, two spellings and two address formats.
    Person(
        "cobalt",
        "John",
        "Smith",
        "john.smith@cobaltfreight.com",
        "IT Director",
        snow=("Jon", "Smith", "jsmith@cobaltfreight.com"),
        jira=("John Smith", "jsmith@cobaltfreight.com"),
    ),
    # Another Maria Lopez, at another customer: another person.
    Person(
        "juniper",
        "Maria",
        "Lopez",
        "mlopez@juniperlogistics.com",
        "Controller",
        snow=("Maria", "Lopez", "maria.lopez@juniperlogistics.com"),
        stripe=True,
    ),
    Person("juniper", "Peter", "Nguyen", "peter.nguyen@juniperlogistics.com", "IT Manager"),
    Person(
        "ironbark",
        "Katherine",
        "Doyle",
        "katherine.doyle@ironbarkenergy.com",
        "Head of Digital",
        snow=("Catherine", "Doyle", "cdoyle@ironbarkenergy.com"),
        caller=True,
    ),
    Person(
        "ironbark",
        "Liam",
        "Walsh",
        "liam.walsh@ironbarkenergy.com",
        "Platform Engineer",
        jira=("Liam Walsh", "liam.walsh@ironbarkenergy.com"),
    ),
    # A nickname: Liz in ServiceNow.
    Person(
        "lumen",
        "Elizabeth",
        "Moreno",
        "elizabeth.moreno@lumendental.com",
        "Practice Manager",
        snow=("Liz", "Moreno", "liz@lumendental.com"),
        caller=True,
    ),
    Person("lumen", "Arjun", "Patel", "arjun.patel@lumendental.com", "Finance Lead", stripe=True),
    # A nickname: Bob in ServiceNow.
    Person(
        "marlowe",
        "Robert",
        "Harlan",
        "robert.harlan@marloweins.com",
        "CTO",
        snow=("Bob", "Harlan", "bob.harlan@marloweins.com"),
        caller=True,
    ),
    Person(
        "marlowe",
        "Grace",
        "Kim",
        "grace.kim@marloweins.com",
        "Claims Systems Lead",
        jira=("Grace Kim", "grace.kim@marloweins.com"),
    ),
    # Daniel Okafor moved from Granite Ridge Bank to Vantage Payroll: a contact at each.
    Person(
        "granite",
        "Daniel",
        "Okafor",
        "daniel.okafor@graniteridgebank.com",
        "Payments Architect",
        snow=("Daniel", "Okafor", "daniel.okafor@graniteridgebank.com"),
    ),
    Person(
        "granite",
        "Helen",
        "Brooks",
        "helen.brooks@graniteridgebank.com",
        "Treasury Manager",
        stripe=True,
    ),
    Person(
        "vantage",
        "Daniel",
        "Okafor",
        "dokafor@vantagepayroll.com",
        "Director of Engineering",
        jira=("Daniel Okafor", "dokafor@vantagepayroll.com"),
        joined=40,
    ),
    Person(
        "vantage",
        "Samira",
        "Haddad",
        "samira.haddad@vantagepayroll.com",
        "Operations Lead",
        snow=("Samira", "Haddad", "samira.haddad@vantagepayroll.com"),
    ),
    Person(
        "kestrel",
        "Jeffrey",
        "Marsh",
        "jeffrey.marsh@kestrelaero.com",
        "Avionics Program Manager",
        snow=("Jeffery", "Marsh", "jmarsh@kestrelaero.com"),
    ),
    Person("kestrel", "Olivia", "Grant", "olivia.grant@kestrelaero.com", "Procurement"),
    Person(
        "larkspur-analytics",
        "Nadia",
        "Petrov",
        "nadia.petrov@larkspuranalytics.com",
        "COO",
        snow=("Nadia", "Petrov", "nadia.petrov@larkspuranalytics.com"),
        stripe=True,
    ),
    Person(
        "larkspur-labs",
        "Tomas",
        "Lind",
        "tomas.lind@larkspurlabs.io",
        "Lab Systems Manager",
        stripe=True,
    ),
    Person(
        "ferncliff",
        "Oliver",
        "Hughes",
        "oliver.hughes@ferncliff.co",
        "eCommerce Director",
        jira=("Oliver Hughes", "oliver.hughes@ferncliff.co"),
    ),
    Person(
        "halcyon",
        "Priya",
        "Shah",
        "priya.shah@halcyonmedia.tv",
        "Head of Production",
        snow=("Priya", "Shah", "priya.shah@halcyonmedia.tv"),
        jira=("Priya Shah", "priya.shah@halcyonmedia.tv"),
    ),
    # A ServiceNow user with no contact in Salesforce.
    Person(
        "halcyon",
        "Tom",
        "Becker",
        "tom.becker@halcyonmedia.tv",
        "Editor",
        snow=("Tom", "Becker", "tom.becker@halcyonmedia.tv"),
        sf=False,
    ),
    Person(
        "pinecrest",
        "Rachel",
        "Adams",
        "rachel.adams@pinecrestschools.org",
        "IT Coordinator",
        jira=("Rachel Adams", "rachel.adams@pinecrestschools.org"),
    ),
    Person(
        "zephyr",
        "Marcus",
        "Bell",
        "marcus.bell@zephyrtel.com",
        "Network Operations",
        snow=("Marcus", "Bell", "marcus.bell@zephyrtel.com"),
        caller=True,
    ),
    Person(
        "brightwater",
        "Hannah",
        "Cole",
        "hannah.cole@brightwaterhealth.com",
        "Clinical Systems Lead",
        snow=("Hannah", "Cole", "hannah.cole@brightwaterhealth.com"),
    ),
    Person(
        "elmstead",
        "Victor",
        "Sousa",
        "victor.sousa@elmstead.com",
        "Plant IT Manager",
        snow=("Victor", "Sousa", "victor.sousa@elmstead.com"),
        jira=("Victor Sousa", "victor.sousa@elmstead.com"),
    ),
    Person("northgate", "Emma", "Clarke", "emma.clarke@northgatefoods.com", "Supply Chain Lead"),
    Person(
        "redfern",
        "James",
        "Whitaker",
        "james.whitaker@redfernhotels.com",
        "Finance Director",
        stripe=True,
    ),
    Person("quillon", "Lena", "Fischer", "lena.fischer@quillon.com", "Head of IT"),
    Person(
        "willow",
        "Anna",
        "Reid",
        "anna.reid@willowcreekclinics.org",
        "Operations Manager",
        snow=("Anna", "Reid", "anna.reid@willowcreekclinics.org"),
    ),
    Person(
        "saltmarsh",
        "Ben",
        "Carter",
        "ben@saltmarshbrewing.com",
        "Owner",
        stripe=True,
    ),
    Person("tamarack", "Ian", "Frost", "ian.frost@tamarackmining.com", "Mine Systems Engineer"),
    Person(
        "xylo",
        "Mei",
        "Chen",
        "mei.chen@xylorobotics.ai",
        "Robotics Software Lead",
        jira=("Mei Chen", "mei.chen@xylorobotics.ai"),
    ),
]

#: Billed in Stripe and in no other system: (key, name, email, billing contact). Harbor Lane
#: Bakery's billing runs outside the CRM; Tessa Rowe Design is billed on a personal mailbox.
STRIPE_ONLY = (
    ("harbor-lane", "Harbor Lane Bakery", "ap@harborlanebakery.com", None),
    ("tessa-rowe", "Tessa Rowe Design", "tessa.rowe.design@gmail.com", "Tessa Rowe"),
)
#: A company the service desk opened by hand, its name mistyped, with no website.
SNOW_TYPO = ("halcyon-typo", "Halcyon Mdia Grp")
#: The Customer field of a bug raised before anyone knew whose it was.
JIRA_PLACEHOLDER = "TBD"

#: The matching model whose run the August bulk import was.
MODEL_VERSION = "match-v2"
#: The model's score on the links it made wrong or found hard, by (system, the record's
#: system_key's owner): every other link it made scores 0.91 and above (see _model_score).
#: Wrong links score 0.60 to 0.80; Saltmarsh's (a personal mailbox, matched by name) and Jon
#: Smith's are right but low.
WRONG_ORG_SCORES = {"larkspur-analytics": 0.71, "larkspur-labs": 0.66}
WILLOW_DUPLICATE_SCORE = 0.64
LOW_RIGHT_ORG_SCORES = {"saltmarsh": 0.78}
#: The person links the model made wrong, (company, last name, system) -> (the company of the
#: master it linked to instead, its score), and the ones it scored low: right, a guess, or none.
WRONG_PERSON_LINKS = {
    ("juniper", "Lopez", "servicenow"): ("cobalt", 0.77),
    ("vantage", "Okafor", "jira"): ("granite", 0.69),
}
LOW_PERSON_SCORES = {
    ("cobalt", "Smith", "servicenow"): 0.71,
    ("marlowe", "Harlan", "servicenow"): 0.83,
}
#: The person mentions the model left unlinked.
UNLINKED_PEOPLE = {("lumen", "Moreno", "servicenow")}


def _model_score(*parts: str) -> float:
    """A confident model score, 0.91 to 0.99, the same on every run for the same link."""
    return round(0.91 + int(_hex("score", *parts, n=6), 16) % 81 / 1000, 2)


def _hex(*parts: str | int, n: int = 32) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()[:n]


_B62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def _b62(*parts: str | int, n: int) -> str:
    value = int(_hex(*parts, n=40), 16)
    out = []
    for _ in range(n):
        value, digit = divmod(value, 62)
        out.append(_B62[digit])
    return "".join(out)


def sf_id(prefix: str, n: int) -> str:
    """An 18-character Salesforce id of the object ``prefix`` (001 an account, 006 an opportunity)."""
    return f"{prefix}Hp00000{n:05d}{_b62(prefix, n, n=3)}"


def quarter_start(day: date) -> date:
    return date(day.year, 3 * ((day.month - 1) // 3) + 1, 1)


def add_months(day: date, months: int) -> date:
    index = day.year * 12 + day.month - 1 + months
    return date(index // 12, index % 12 + 1, 1)


def epoch(moment: datetime) -> int:
    return int((moment - datetime(1970, 1, 1)).total_seconds())


def ts(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class Data:
    accounts: list[dict] = field(default_factory=list)
    opportunities: list[dict] = field(default_factory=list)
    companies: list[dict] = field(default_factory=list)
    incidents: list[dict] = field(default_factory=list)
    bugs: list[dict] = field(default_factory=list)
    customers: list[dict] = field(default_factory=list)
    invoices: list[dict] = field(default_factory=list)
    contacts: list[dict] = field(default_factory=list)
    users: list[dict] = field(default_factory=list)
    masters: list[dict] = field(default_factory=list)
    xref: list[dict] = field(default_factory=list)
    person_masters: list[dict] = field(default_factory=list)
    xref_person: list[dict] = field(default_factory=list)
    syncs: list[dict] = field(default_factory=list)


def build(as_of: date) -> Data:
    """Every system's rows and the hub's, as of ``as_of``."""
    d = Data()
    q0 = quarter_start(as_of)
    noon = time(12, 0)
    first_load = datetime.combine(as_of - timedelta(days=270), time(9, 0))
    bulk_import = datetime.combine(add_months(as_of, -3) + timedelta(days=13), time(2, 30))
    account_n = opp_n = inc_n = bug_n = 0
    by_key: dict[str, dict] = {}  # each company's ids in each system
    billing_contact = {p.company: f"{p.first} {p.last}" for p in PEOPLE if p.stripe}

    for i, c in enumerate(COMPANIES):
        ids: dict = {"accounts": []}
        by_key[c.key] = ids
        created = datetime.combine(as_of - timedelta(days=900 - 17 * i), time(10, 5))
        for name, website in ((c.name, c.website), *c.extra_accounts):
            account_n += 1
            acct = sf_id("001", account_n)
            ids["accounts"].append(acct)
            d.accounts.append(
                {
                    "Id": acct,
                    "Name": name,
                    "Website": website,
                    "Industry": c.industry,
                    "Type": "Customer",
                    "BillingCountry": c.country,
                    "CreatedDate": ts(created),
                }
            )
            created += timedelta(days=200)
        anchor = ids["accounts"][0]
        # Last year's renewal, won; this year's, open, closing in its quarter.
        close = add_months(q0, 3 * c.renews) + timedelta(days=9 + (7 * i) % 70)
        for year_back, stage, closed in (
            (1, "Closed Won", True),
            (0, "Negotiation/Review" if i % 2 else "Proposal/Price Quote", False),
        ):
            opp_n += 1
            close_day = date(close.year - year_back, close.month, min(close.day, 28))
            d.opportunities.append(
                {
                    "Id": sf_id("006", opp_n),
                    "AccountId": anchor,
                    "Name": f"{c.name} renewal {close_day.year}",
                    "Type": "Renewal",
                    "StageName": stage,
                    "Amount": float(c.amount),
                    "CloseDate": close_day.isoformat(),
                    "IsClosed": closed,
                }
            )

        if c.snow_name is not None:
            sys_id = _hex("core_company", c.key)
            ids["company"] = sys_id
            d.companies.append(
                {
                    "sys_id": sys_id,
                    "name": c.snow_name,
                    "website": c.snow_website,
                    "city": None,
                    "country": c.country,
                    "customer": True,
                    "sys_created_on": ts(created - timedelta(days=150)),
                }
            )
            if "stale-servicenow" in c.tags:
                # The company record the hub still names was deleted when this one replaced it.
                ids["stale_company"] = _hex("core_company", c.key, "deleted")

        if c.jira_names:
            ids["jira"] = c.jira_names
        if c.stripe_name is not None:
            cus = "cus_" + _b62("customer", c.key, n=14)
            ids["stripe"] = cus
            d.customers.append(
                {
                    "id": cus,
                    "name": c.stripe_name,
                    "email": c.stripe_email,
                    "created": epoch(created - timedelta(days=120)),
                    "currency": "usd",
                    "delinquent": c.overdue > 0,
                    "description": None,
                    "billing_contact_name": billing_contact.get(c.key),
                }
            )
            if "stale-stripe" in c.tags:
                ids["stale_stripe"] = "cus_" + _b62("customer", c.key, "deleted", n=14)

    # The records no rule gives a customer: Stripe customers billed outside the CRM, and the
    # company the service desk typed by hand.
    for k, (key, name, email, contact) in enumerate(STRIPE_ONLY):
        cus = "cus_" + _b62("customer", key, n=14)
        by_key[key] = {"stripe": cus}
        d.customers.append(
            {
                "id": cus,
                "name": name,
                "email": email,
                "created": epoch(
                    datetime.combine(as_of - timedelta(days=210 - 30 * k), time(10, 5))
                ),
                "currency": "usd",
                "delinquent": False,
                "description": None,
                "billing_contact_name": contact,
            }
        )
    typo_key, typo_name = SNOW_TYPO
    d.companies.append(
        {
            "sys_id": _hex("core_company", typo_key),
            "name": typo_name,
            "website": None,
            "city": None,
            "country": "US",
            "customer": True,
            "sys_created_on": ts(datetime.combine(as_of - timedelta(days=75), time(16, 40))),
        }
    )

    # People: each contact under its company's first account, each ServiceNow user under its
    # company, and the portal reporter each company's bugs carry.
    contact_n = 0
    people: list[dict] = []  # each person's ids, in PEOPLE's order
    for k, p in enumerate(PEOPLE):
        ids = {
            "created": datetime.combine(
                as_of - timedelta(days=p.joined or 600 - 9 * k), time(11, 15)
            )
        }
        people.append(ids)
        company = by_key[p.company]
        if p.sf:
            contact_n += 1
            ids["contact"] = sf_id("003", contact_n)
            d.contacts.append(
                {
                    "Id": ids["contact"],
                    "AccountId": company["accounts"][0],
                    "FirstName": p.first,
                    "LastName": p.last,
                    "Email": p.email,
                    "Title": p.title,
                    "CreatedDate": ts(ids["created"]),
                }
            )
        if p.snow is not None:
            first, last, email = p.snow
            ids["user"] = _hex("sys_user", p.company, email)
            d.users.append(
                {
                    "sys_id": ids["user"],
                    "first_name": first,
                    "last_name": last,
                    "email": email,
                    "company": company["company"],
                    "sys_created_on": ts(ids["created"] + timedelta(days=20)),
                }
            )
        if p.jira is not None:
            ids["reporter"] = p.jira
        if p.stripe:
            ids["billing"] = company["stripe"]
    users = {
        c.key: [people[k]["user"] for k, p in enumerate(PEOPLE) if p.company == c.key and p.snow]
        for c in COMPANIES
    }
    callers = {
        c.key: [people[k]["user"] for k, p in enumerate(PEOPLE) if p.company == c.key and p.caller]
        for c in COMPANIES
    }
    reporters = {p.company: people[k]["reporter"] for k, p in enumerate(PEOPLE) if p.jira}

    def caller(c: Company, n: int, urgent: bool) -> str | None:
        """Who raised a company's incident: its named caller for an urgent one, else its users in
        turn; none when the company has no ServiceNow user."""
        if urgent and callers[c.key]:
            return callers[c.key][0]
        return users[c.key][n % len(users[c.key])] if users[c.key] else None

    # The Larkspur swap's evidence: the overdue invoice is on Larkspur Analytics' own customer.
    # Stripe invoices: one a month for six months, each due 30 days after it is raised. Every
    # invoice is paid but the current month's (open, not yet due) and a planted overdue one (last
    # month's, open and past due).
    month0 = date(as_of.year, as_of.month, 1)
    for c in COMPANIES:
        cus = by_key[c.key].get("stripe")
        if cus is None:
            continue
        monthly = round(c.amount * 100 / 12)
        for back in range(5, -1, -1):
            raised = datetime.combine(add_months(month0, -back), time(8, 0))
            due = raised + timedelta(days=30)
            if back == 0:
                status, remaining = "open", monthly
            elif back == 1 and c.overdue:
                status, remaining = "open", monthly
            else:
                status, remaining = "paid", 0
            d.invoices.append(
                {
                    "id": "in_" + _b62("invoice", c.key, back, n=24),
                    "customer": cus,
                    "number": f"{_b62('prefix', c.key, n=6).upper()}-{6 - back:04d}",
                    "status": status,
                    "amount_due": monthly,
                    "amount_remaining": remaining,
                    "currency": "usd",
                    "created": epoch(raised),
                    "due_date": epoch(due),
                    "collection_method": "send_invoice",
                }
            )
    # Billed outside the CRM: three months, each paid.
    for key, *_ in STRIPE_ONLY:
        for back in range(3, 0, -1):
            raised = datetime.combine(add_months(month0, -back), time(8, 0))
            d.invoices.append(
                {
                    "id": "in_" + _b62("invoice", key, back, n=24),
                    "customer": by_key[key]["stripe"],
                    "number": f"{_b62('prefix', key, n=6).upper()}-{4 - back:04d}",
                    "status": "paid",
                    "amount_due": 42000,
                    "amount_remaining": 0,
                    "currency": "usd",
                    "created": epoch(raised),
                    "due_date": epoch(raised + timedelta(days=30)),
                    "collection_method": "send_invoice",
                }
            )

    # ServiceNow incidents: a resolved history for every company, and the planted open P1s. An open
    # P1 is fixed in Jira: its correlation_id names the bug.
    def bug(
        summary: str,
        status: str,
        priority: str,
        opened: datetime,
        customer: str,
        reporter: tuple[str, str] | None,
    ) -> str:
        nonlocal bug_n
        bug_n += 1
        key = f"ENG-{400 + bug_n}"
        d.bugs.append(
            {
                "key": key,
                "id": str(10400 + bug_n),
                "summary": summary,
                "status": status,
                "created": ts(opened),
                "updated": ts(opened + timedelta(days=3)),
                "priority": priority,
                "customer": customer,
                "reporter_name": reporter[0] if reporter else None,
                "reporter_email": reporter[1] if reporter else None,
            }
        )
        return key

    def incident(
        c: Company,
        company: str,
        summary: str,
        state: int,
        priority: int,
        opened: datetime,
        resolved: datetime | None,
        fix: str | None,
        caller_id: str | None,
    ) -> None:
        nonlocal inc_n
        inc_n += 1
        d.incidents.append(
            {
                "sys_id": _hex("incident", inc_n),
                "number": f"INC{1000100 + inc_n:07d}",
                "short_description": summary,
                "state": state,
                "priority": priority,
                "opened_at": ts(opened),
                "resolved_at": ts(resolved) if resolved else None,
                "company": company,
                "company__display": c.snow_name,
                "correlation_id": fix,
                "caller_id": caller_id,
            }
        )

    topics = (
        "Login failures after SSO change",
        "Export job times out",
        "Dashboard slow to load",
        "Webhook deliveries delayed",
        "Report totals do not match",
        "API rate limit errors",
    )
    for i, c in enumerate(COMPANIES):
        ids = by_key[c.key]
        jira_names = ids.get("jira", ())
        reporter = reporters.get(c.key)
        for j, name in enumerate(jira_names):
            opened = datetime.combine(as_of - timedelta(days=60 + 11 * i + 5 * j), noon)
            bug(
                f"{topics[(i + j) % len(topics)]} ({c.name})",
                "Done",
                "Medium",
                opened,
                name,
                reporter,
            )
        company = ids.get("company")
        if company is None:
            continue
        opened = datetime.combine(as_of - timedelta(days=120 + 3 * i), time(9, 40))
        incident(
            c,
            company,
            topics[i % len(topics)],
            7,
            1 + i % 3,
            opened,
            opened + timedelta(days=2),
            None,
            caller(c, 0, False),
        )
        opened = datetime.combine(as_of - timedelta(days=20 + i % 9), time(14, 10))
        incident(
            c, company, topics[(i + 2) % len(topics)], 2, 3, opened, None, None, caller(c, 1, False)
        )
        if c.key == "zephyr":
            opened = datetime.combine(as_of - timedelta(days=12), time(7, 55))
            incident(
                c,
                company,
                "Voice API outage in us-east",
                6,
                1,
                opened,
                opened + timedelta(hours=9),
                None,
                caller(c, 0, True),
            )
        for p in range(c.open_p1):
            opened = datetime.combine(as_of - timedelta(days=4 + p + i % 5), time(3, 20))
            status = "Blocked" if c.blocked else "In Progress"
            fix = bug(
                f"P1: {topics[(i + 4) % len(topics)]} ({c.name})",
                status,
                "Highest",
                opened,
                jira_names[0],
                reporter,
            )
            incident(
                c,
                company,
                f"P1: {topics[(i + 4) % len(topics)]}",
                2 if p == 0 else 1,
                1,
                opened,
                None,
                fix,
                caller(c, p, True),
            )
        # A blocked fix with no P1 behind it.
        if c.blocked and not c.open_p1:
            opened = datetime.combine(as_of - timedelta(days=15 + i % 6), noon)
            bug(
                f"Blocked: {topics[(i + 3) % len(topics)]} ({c.name})",
                "Blocked",
                "High",
                opened,
                jira_names[-1],
                reporter,
            )
        # Open, not P1, not blocked: what does not count.
        if jira_names:
            opened = datetime.combine(as_of - timedelta(days=9 + i % 4), noon)
            bug(
                f"{topics[(i + 5) % len(topics)]} ({c.name})",
                "In Progress",
                "Low",
                opened,
                jira_names[0],
                reporter,
            )
    # A bug raised before anyone knew whose it was.
    bug(
        "Uploads stall at 99% on large media files",
        "To Do",
        "Medium",
        datetime.combine(as_of - timedelta(days=6), time(15, 30)),
        JIRA_PLACEHOLDER,
        reporters["halcyon"],
    )

    # The hub: a master each, linked to each system's ids by the first load, the steward's links
    # since, and the August bulk import: the matching model's run, each of its links scored.
    def master(n: int, name: str, created: datetime) -> str:
        master_id = f"MC-{10000 + n}"
        d.masters.append({"master_id": master_id, "name": name, "created_at": ts(created)})
        return master_id

    def scored(by: str, score: float | None) -> dict:
        """A link's match metadata: the first load's rules, a person's hand, or the model's run
        and its score."""
        if by == "bulk import":
            assert score is not None, "a model link carries its score"
            return {"match_method": "model", "match_score": score, "model_version": MODEL_VERSION}
        assert score is None, f"a {by} link has no score"
        method = "rule" if by == "initial load" else "steward"
        return {"match_method": method, "match_score": None, "model_version": None}

    def link(
        master_id: str, system: str, key: str, at: datetime, by: str, score: float | None = None
    ) -> None:
        d.xref.append(
            {
                "master_id": master_id,
                "system": system,
                "system_key": key,
                "linked_at": ts(at),
                "linked_by": by,
                **scored(by, score),
            }
        )

    masters: dict[str, str] = {}
    n = 0
    for i, c in enumerate(COMPANIES):
        n += 1
        at = first_load + timedelta(days=(i % 6) * 31)
        by = "initial load" if at == first_load else "steward"
        masters[c.key] = master(n, c.name, at)
    larkspur_swap = {"larkspur-analytics": "larkspur-labs", "larkspur-labs": "larkspur-analytics"}
    for i, c in enumerate(COMPANIES):
        ids = by_key[c.key]
        m = masters[c.key]
        at = first_load + timedelta(days=(i % 6) * 31)
        by = "initial load" if at == first_load else "steward"
        for acct in ids["accounts"]:
            link(m, "salesforce", acct, at, by)
        if "company" in ids and "gap-servicenow" not in c.tags and c.key != "brightwater":
            if "stale-servicenow" in c.tags:
                link(m, "servicenow", ids["stale_company"], at, by)
            else:
                link(m, "servicenow", ids["company"], at, by)
        if c.key not in ("xylo", "willow"):
            for name in ids.get("jira", ()):
                link(m, "jira", name, at + timedelta(days=2), by)
        if "stripe" in ids and "gap-stripe" not in c.tags:
            if c.key in larkspur_swap:
                other = by_key[larkspur_swap[c.key]]["stripe"]
                link(m, "stripe", other, bulk_import, "bulk import", WRONG_ORG_SCORES[c.key])
            elif "stale-stripe" in c.tags:
                link(m, "stripe", ids["stale_stripe"], at, by)
            else:
                # The model linked every billed customer the hub still held.
                score = LOW_RIGHT_ORG_SCORES.get(c.key) or _model_score("stripe", c.key)
                link(m, "stripe", ids["stripe"], bulk_import, "bulk import", score)
    # Duplicate customers: a second master for Brightwater's ServiceNow company (opened by the
    # support desk) and for Willow Creek's Jira customer (the bulk import).
    n += 1
    dup = master(n, "Brightwater Health, Inc.", first_load + timedelta(days=95))
    link(
        dup,
        "servicenow",
        by_key["brightwater"]["company"],
        first_load + timedelta(days=95),
        "support desk",
    )
    n += 1
    dup = master(n, "Willow Creek Clinics (support)", bulk_import)
    for name in by_key["willow"]["jira"]:
        link(dup, "jira", name, bulk_import, "bulk import", WILLOW_DUPLICATE_SCORE)

    # People: a master person for each contact, under its company's master, linked to the contact
    # by the first load (or a steward, for a contact made since). The model's run linked each
    # other system's person to a master person, wrongly in places (WRONG_PERSON_LINKS).
    def person_link(
        pm: str, system: str, key: str, at: datetime, by: str, score: float | None
    ) -> None:
        d.xref_person.append(
            {
                "person_master_id": pm,
                "system": system,
                "system_key": key,
                "linked_at": ts(at),
                "linked_by": by,
                **scored(by, score),
            }
        )

    person_master: dict[tuple[str, str], str] = {}  # (company, last name) -> master person
    for k, p in enumerate(PEOPLE):
        if not p.sf:
            continue
        ids = people[k]
        pm = f"MP-{20001 + len(d.person_masters)}"
        person_master[(p.company, p.last)] = pm
        at, by = (
            (first_load, "initial load")
            if ids["created"] <= first_load
            else (
                ids["created"] + timedelta(days=1),
                "steward",
            )
        )
        d.person_masters.append(
            {
                "person_master_id": pm,
                "name": f"{p.first} {p.last}",
                "email": p.email,
                "master_id": masters[p.company],
                "created_at": ts(at),
            }
        )
        person_link(pm, "salesforce", ids["contact"], at, by, None)
    for k, p in enumerate(PEOPLE):
        ids = people[k]
        mentions = (
            ("servicenow", ids.get("user")),
            ("jira", ids["reporter"][1] if "reporter" in ids else None),
            ("stripe", ids.get("billing")),
        )
        for system, key in mentions:
            planted = (p.company, p.last, system)
            if key is None or planted in UNLINKED_PEOPLE:
                continue
            if planted in WRONG_PERSON_LINKS:
                other, score = WRONG_PERSON_LINKS[planted]
                pm = person_master[(other, p.last)]
            elif p.sf:
                pm = person_master[(p.company, p.last)]
                score = LOW_PERSON_SCORES.get(planted) or _model_score("person", system, key)
            else:
                continue  # no contact: the model had no master to link it to
            person_link(pm, system, key, bulk_import, "bulk import", score)

    synced = datetime.combine(as_of, time(6, 0))
    for k, system in enumerate(("salesforce", "servicenow", "jira", "stripe")):
        d.syncs.append({"system": system, "synced_at": ts(synced + timedelta(minutes=4 * k))})
    return d


Cell = str | int | float | bool | None


def _cell(value: Cell) -> str:
    if value is None:
        return "\\N"
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value)
    return text.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def write_tsv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["\t".join(columns)] + ["\t".join(_cell(r[c]) for c in columns) for r in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _sql(value: Cell) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


HUB_SCHEMA = """\
-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- Halyard Software's customer master hub (schema mdm), written by generate.py: master customers and
-- master people, the crosswalks from each master to each system's own id with how each link was
-- made (a rule of the first load, a steward, or a matching model's run and its score), and when the
-- hub last synced from each system. Read live by the customer360 demo (REQ-2034).

CREATE SCHEMA mdm;

CREATE TABLE mdm.master_customer (
  master_id varchar PRIMARY KEY,
  name varchar NOT NULL,
  created_at timestamp NOT NULL
);

CREATE TABLE mdm.xref (
  master_id varchar NOT NULL REFERENCES mdm.master_customer (master_id),
  system varchar NOT NULL CHECK (system IN ('salesforce', 'servicenow', 'jira', 'stripe')),
  system_key varchar NOT NULL,
  linked_at timestamp NOT NULL,
  linked_by varchar NOT NULL,
  match_method varchar NOT NULL CHECK (match_method IN ('rule', 'steward', 'model')),
  match_score numeric(3, 2) CHECK (match_score BETWEEN 0 AND 1),
  model_version varchar,
  PRIMARY KEY (system, system_key),
  CHECK ((match_method = 'model') = (match_score IS NOT NULL AND model_version IS NOT NULL))
);

CREATE TABLE mdm.master_person (
  person_master_id varchar PRIMARY KEY,
  name varchar NOT NULL,
  email varchar NOT NULL,
  master_id varchar NOT NULL REFERENCES mdm.master_customer (master_id),
  created_at timestamp NOT NULL
);

CREATE TABLE mdm.xref_person (
  person_master_id varchar NOT NULL REFERENCES mdm.master_person (person_master_id),
  system varchar NOT NULL CHECK (system IN ('salesforce', 'servicenow', 'jira', 'stripe')),
  system_key varchar NOT NULL,
  linked_at timestamp NOT NULL,
  linked_by varchar NOT NULL,
  match_method varchar NOT NULL CHECK (match_method IN ('rule', 'steward', 'model')),
  match_score numeric(3, 2) CHECK (match_score BETWEEN 0 AND 1),
  model_version varchar,
  PRIMARY KEY (system, system_key),
  CHECK ((match_method = 'model') = (match_score IS NOT NULL AND model_version IS NOT NULL))
);

CREATE TABLE mdm.sync_run (
  system varchar PRIMARY KEY,
  synced_at timestamp NOT NULL
);
"""


LINK_COLUMNS = [
    "system",
    "system_key",
    "linked_at",
    "linked_by",
    "match_method",
    "match_score",
    "model_version",
]


def write_hub(path: Path, d: Data) -> None:
    out = [HUB_SCHEMA]
    for table, columns, rows in (
        ("mdm.master_customer", ["master_id", "name", "created_at"], d.masters),
        ("mdm.xref", ["master_id", *LINK_COLUMNS], d.xref),
        (
            "mdm.master_person",
            ["person_master_id", "name", "email", "master_id", "created_at"],
            d.person_masters,
        ),
        ("mdm.xref_person", ["person_master_id", *LINK_COLUMNS], d.xref_person),
        ("mdm.sync_run", ["system", "synced_at"], d.syncs),
    ):
        values = ",\n".join("  (" + ", ".join(_sql(r[c]) for c in columns) + ")" for r in rows)
        out.append(f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n{values};\n")
    path.write_text("\n".join(out), encoding="utf-8")


def write(root: Path, as_of: date) -> Data:
    """Write every file under ``root`` (the demo's directory) as of ``as_of``."""
    d = build(as_of)
    rows = root / "replica_rows"
    write_tsv(rows / "salesforce" / "salesforce.Account.tsv", ACCOUNT_COLUMNS, d.accounts)
    write_tsv(
        rows / "salesforce" / "salesforce.Opportunity.tsv", OPPORTUNITY_COLUMNS, d.opportunities
    )
    write_tsv(rows / "salesforce" / "salesforce.Contact.tsv", CONTACT_COLUMNS, d.contacts)
    write_tsv(rows / "servicenow" / "servicenow.core_company.tsv", COMPANY_COLUMNS, d.companies)
    write_tsv(rows / "servicenow" / "servicenow.incident.tsv", INCIDENT_COLUMNS, d.incidents)
    write_tsv(rows / "servicenow" / "servicenow.sys_user.tsv", SYS_USER_COLUMNS, d.users)
    write_tsv(rows / "jira" / "jira.bug.tsv", BUG_COLUMNS, d.bugs)
    write_tsv(rows / "stripe" / "default.get_customers.tsv", CUSTOMER_COLUMNS, d.customers)
    write_tsv(rows / "stripe" / "default.get_invoices.tsv", INVOICE_COLUMNS, d.invoices)
    (root / "db").mkdir(exist_ok=True)
    write_hub(root / "db" / "init.sql", d)
    return d


def main() -> None:
    parser = argparse.ArgumentParser(description="Write the customer360 demo's data.")
    parser.add_argument("--as-of", type=date.fromisoformat, default=AS_OF)
    args = parser.parse_args()
    d = write(HERE, args.as_of)
    print(
        f"customer360 as of {args.as_of}: {len(d.accounts)} accounts, {len(d.opportunities)} opportunities,"
        f" {len(d.contacts)} contacts, {len(d.users)} ServiceNow users,"
        f" {len(d.companies)} companies, {len(d.incidents)} incidents, {len(d.bugs)} bugs,"
        f" {len(d.customers)} Stripe customers, {len(d.invoices)} invoices; hub {len(d.masters)} masters,"
        f" {len(d.xref)} links; {len(d.person_masters)} master people, {len(d.xref_person)} person links"
    )


if __name__ == "__main__":
    main()
