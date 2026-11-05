# Copyright (c) 2026 Kenneth Stott
# Canary: 0d7a3c58-91e4-4b26-8f1d-5e2b7a96c403
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The QuickBooks Online Accounting API entities a source offers as tables (REQ-2197), with their
fields as the API's entity reference describes them.

A field becomes columns by one rule (:func:`columns_of`), the rule a record is read by
(``quickbooks.rows``):

- a scalar field is one column, named in snake case (``TotalAmt`` -> ``total_amt``);
- a reference (``CustomerRef``, ``{"value": "58", "name": "Amy's Bird Sanctuary"}``) is two:
  ``customer_ref_value`` and ``customer_ref_name``;
- ``MetaData`` is ``meta_create_time`` and ``meta_last_updated_time``;
- any other object or list (an address, the ``Line`` items) is one JSON column, as the generic
  API source keeps a nested value.

A field is *filterable* where QuickBooks' query language takes a condition on it: only those are
ever pushed into a query, so a read never asks QuickBooks for a filter it refuses. ``like`` marks
the text fields it matches with ``LIKE``.
"""

# Requirements: REQ-2197
from __future__ import annotations

import re
from dataclasses import dataclass

from provisa.api_source.models import ApiColumnType

# Kinds of field, as they are written below.
TEXT = "text"
NUMBER = "number"
INTEGER = "integer"
BOOLEAN = "boolean"
DATE = "date"
TIMESTAMP = "timestamp"
REF = "ref"  # {"value", "name"}
JSON = "json"  # any other object or list

_COLUMN_TYPES = {
    TEXT: ApiColumnType.string,
    NUMBER: ApiColumnType.number,
    INTEGER: ApiColumnType.integer,
    BOOLEAN: ApiColumnType.boolean,
    DATE: ApiColumnType.date,
    TIMESTAMP: ApiColumnType.timestamp,
    JSON: ApiColumnType.jsonb,
}

#: The field every entity carries its creation and last change under.
METADATA = "MetaData"
_METADATA_FIELDS = (
    ("CreateTime", "meta_create_time"),
    ("LastUpdatedTime", "meta_last_updated_time"),
)


@dataclass(frozen=True)
class Field:
    name: str  # as QuickBooks names it
    kind: str
    filterable: bool = False
    like: bool = False  # filterable with LIKE (text only)


@dataclass(frozen=True)
class Column:
    """One column of an entity's table, and the field its conditions are written on."""

    name: str
    type: ApiColumnType
    path: str  # the field in QuickBooks' query language: "TotalAmt", "CustomerRef"
    filterable: bool
    like: bool
    kind: str


@dataclass(frozen=True)
class Entity:
    name: str
    description: str
    fields: tuple[Field, ...]
    #: Whether the entity has an ``Active`` flag. QuickBooks reads only active records of such an
    #: entity unless a query names ``Active``, so a read that does not asks for both.
    has_active: bool = False


def snake(name: str) -> str:
    """A QuickBooks field name in snake case: ``TotalAmt`` -> ``total_amt``,
    ``PrimaryEmailAddr`` -> ``primary_email_addr``, ``domain`` -> ``domain``."""
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name)
    return spaced.lower()


def columns_of(entity: Entity) -> list[Column]:
    """The columns of ``entity``'s table, in the order its fields are listed."""
    columns: list[Column] = []
    for f in entity.fields:
        if f.kind == REF:
            base = snake(f.name)
            columns.append(
                Column(f"{base}_value", ApiColumnType.string, f.name, f.filterable, False, REF)
            )
            columns.append(Column(f"{base}_name", ApiColumnType.string, "", False, False, TEXT))
        else:
            columns.append(
                Column(snake(f.name), _COLUMN_TYPES[f.kind], f.name, f.filterable, f.like, f.kind)
            )
    for qb_name, column in _METADATA_FIELDS:
        columns.append(
            Column(column, ApiColumnType.timestamp, f"{METADATA}.{qb_name}", True, False, TIMESTAMP)
        )
    return columns


def _f(name: str, kind: str = TEXT, *, filterable: bool = False, like: bool = False) -> Field:
    return Field(name, kind, filterable or like, like)


# What every entity starts with: its id and the version a change is sent against.
_ID = (_f("Id", filterable=True), _f("SyncToken"), _f("domain"), _f("sparse", BOOLEAN))
_CURRENCY = (_f("CurrencyRef", REF), _f("ExchangeRate", NUMBER))
_TXN = (
    _f("DocNumber", filterable=True),
    _f("TxnDate", DATE, filterable=True),
    _f("PrivateNote"),
    _f("TotalAmt", NUMBER, filterable=True),
    *_CURRENCY,
    _f("Line", JSON),
    _f("LinkedTxn", JSON),
    _f("DepartmentRef", REF),
)
_SALES = (
    *_TXN,
    _f("CustomerRef", REF, filterable=True),
    _f("CustomerMemo", JSON),
    _f("BillAddr", JSON),
    _f("ShipAddr", JSON),
    _f("BillEmail", JSON),
    _f("TxnTaxDetail", JSON),
    _f("ApplyTaxAfterDiscount", BOOLEAN),
    _f("PrintStatus"),
    _f("EmailStatus"),
    _f("ClassRef", REF),
    _f("SalesTermRef", REF),
    _f("ShipDate", DATE),
    _f("TrackingNum"),
    _f("HomeTotalAmt", NUMBER),
)
_PARTY = (
    _f("DisplayName", like=True),
    _f("Title"),
    _f("GivenName", like=True),
    _f("MiddleName"),
    _f("FamilyName", like=True),
    _f("Suffix"),
    _f("CompanyName", like=True),
    _f("PrintOnCheckName"),
    _f("Active", BOOLEAN, filterable=True),
    _f("PrimaryPhone", JSON),
    _f("Mobile", JSON),
    _f("Fax", JSON),
    _f("PrimaryEmailAddr", JSON),
    _f("WebAddr", JSON),
    _f("BillAddr", JSON),
    _f("Balance", NUMBER, filterable=True),
    *_CURRENCY,
)

_ENTITIES: tuple[Entity, ...] = (
    Entity(
        "Account",
        "The chart of accounts: each account money is recorded against.",
        (
            *_ID,
            _f("Name", like=True),
            _f("FullyQualifiedName", like=True),
            _f("SubAccount", BOOLEAN, filterable=True),
            _f("ParentRef", REF, filterable=True),
            _f("Description", filterable=True),
            _f("Active", BOOLEAN, filterable=True),
            _f("Classification", filterable=True),
            _f("AccountType", filterable=True),
            _f("AccountSubType", filterable=True),
            _f("AcctNum"),
            _f("CurrentBalance", NUMBER, filterable=True),
            _f("CurrentBalanceWithSubAccounts", NUMBER, filterable=True),
            _f("CurrencyRef", REF),
            _f("TaxCodeRef", REF),
        ),
        has_active=True,
    ),
    Entity(
        "Attachable",
        "A file or note attached to a transaction or a list record.",
        (
            *_ID,
            _f("FileName", like=True),
            _f("Note"),
            _f("Category"),
            _f("ContentType"),
            _f("Size", NUMBER),
            _f("TempDownloadUri"),
            _f("AttachableRef", JSON),
        ),
    ),
    Entity(
        "Bill",
        "A bill from a vendor, to be paid.",
        (
            *_TXN,
            *_ID,
            _f("VendorRef", REF, filterable=True),
            _f("APAccountRef", REF),
            _f("SalesTermRef", REF),
            _f("DueDate", DATE, filterable=True),
            _f("Balance", NUMBER, filterable=True),
            _f("VendorAddr", JSON),
            _f("HomeBalance", NUMBER),
        ),
    ),
    Entity(
        "BillPayment",
        "A payment made against one or more bills.",
        (
            *_TXN,
            *_ID,
            _f("VendorRef", REF, filterable=True),
            _f("PayType"),
            _f("CheckPayment", JSON),
            _f("CreditCardPayment", JSON),
            _f("APAccountRef", REF),
        ),
    ),
    Entity(
        "Budget",
        "A budget, by account and period.",
        (
            *_ID,
            _f("Name"),
            _f("StartDate", DATE),
            _f("EndDate", DATE),
            _f("BudgetType"),
            _f("BudgetEntryType"),
            _f("Active", BOOLEAN),
            _f("BudgetDetail", JSON),
        ),
    ),
    Entity(
        "Class",
        "A class transactions are tracked by.",
        (
            *_ID,
            _f("Name", like=True),
            _f("FullyQualifiedName", like=True),
            _f("SubClass", BOOLEAN),
            _f("ParentRef", REF),
            _f("Active", BOOLEAN, filterable=True),
        ),
        has_active=True,
    ),
    Entity(
        "CompanyInfo",
        "The company the source is authorized for.",
        (
            *_ID,
            _f("CompanyName"),
            _f("LegalName"),
            _f("Country"),
            _f("FiscalYearStartMonth"),
            _f("CompanyStartDate", DATE),
            _f("SupportedLanguages"),
            _f("CompanyAddr", JSON),
            _f("LegalAddr", JSON),
            _f("CustomerCommunicationAddr", JSON),
            _f("PrimaryPhone", JSON),
            _f("Email", JSON),
            _f("WebAddr", JSON),
            _f("NameValue", JSON),
        ),
    ),
    Entity(
        "CreditMemo",
        "A credit given to a customer.",
        (*_SALES, *_ID, _f("Balance", NUMBER), _f("RemainingCredit", NUMBER)),
    ),
    Entity(
        "Customer",
        "A customer, or a customer's job.",
        (
            *_ID,
            *_PARTY,
            _f("FullyQualifiedName", like=True),
            _f("Job", BOOLEAN, filterable=True),
            _f("ParentRef", REF),
            _f("Level", INTEGER),
            _f("BillWithParent", BOOLEAN),
            _f("ShipAddr", JSON),
            _f("Notes"),
            _f("Taxable", BOOLEAN),
            _f("BalanceWithJobs", NUMBER),
            _f("PreferredDeliveryMethod"),
            _f("SalesTermRef", REF),
            _f("PaymentMethodRef", REF),
        ),
        has_active=True,
    ),
    Entity(
        "Department",
        "A location or department transactions are tracked by.",
        (
            *_ID,
            _f("Name", like=True),
            _f("FullyQualifiedName", like=True),
            _f("SubDepartment", BOOLEAN),
            _f("ParentRef", REF),
            _f("Active", BOOLEAN, filterable=True),
        ),
        has_active=True,
    ),
    Entity(
        "Deposit",
        "A deposit into a bank account.",
        (*_TXN, *_ID, _f("DepositToAccountRef", REF), _f("CashBack", JSON)),
    ),
    Entity(
        "Employee",
        "An employee.",
        (
            *_ID,
            _f("DisplayName", like=True),
            _f("Title"),
            _f("GivenName", like=True),
            _f("MiddleName"),
            _f("FamilyName", like=True),
            _f("Suffix"),
            _f("PrintOnCheckName"),
            _f("Active", BOOLEAN, filterable=True),
            _f("EmployeeNumber"),
            _f("PrimaryPhone", JSON),
            _f("Mobile", JSON),
            _f("PrimaryEmailAddr", JSON),
            _f("PrimaryAddr", JSON),
            _f("BillableTime", BOOLEAN),
            _f("BillRate", NUMBER),
            _f("HiredDate", DATE),
            _f("ReleasedDate", DATE),
            _f("BirthDate", DATE),
            _f("Gender"),
        ),
        has_active=True,
    ),
    Entity(
        "Estimate",
        "An estimate, or quote, given to a customer.",
        (
            *_SALES,
            *_ID,
            _f("TxnStatus", filterable=True),
            _f("ExpirationDate", DATE),
            _f("AcceptedBy"),
            _f("AcceptedDate", DATE),
        ),
    ),
    Entity(
        "Invoice",
        "An invoice sent to a customer.",
        (
            *_SALES,
            *_ID,
            _f("DueDate", DATE, filterable=True),
            _f("Balance", NUMBER, filterable=True),
            _f("Deposit", NUMBER),
            _f("AllowOnlineCreditCardPayment", BOOLEAN),
            _f("AllowOnlineACHPayment", BOOLEAN),
            _f("HomeBalance", NUMBER),
        ),
    ),
    Entity(
        "Item",
        "A product or service sold or bought.",
        (
            *_ID,
            _f("Name", like=True),
            _f("FullyQualifiedName", like=True),
            _f("Sku", like=True),
            _f("Description"),
            _f("Active", BOOLEAN, filterable=True),
            _f("Type", filterable=True),
            _f("Taxable", BOOLEAN),
            _f("UnitPrice", NUMBER),
            _f("PurchaseCost", NUMBER),
            _f("PurchaseDesc"),
            _f("TrackQtyOnHand", BOOLEAN),
            _f("QtyOnHand", NUMBER),
            _f("InvStartDate", DATE),
            _f("IncomeAccountRef", REF),
            _f("ExpenseAccountRef", REF),
            _f("AssetAccountRef", REF),
            _f("ParentRef", REF),
            _f("SubItem", BOOLEAN),
            _f("Level", INTEGER),
        ),
        has_active=True,
    ),
    Entity(
        "JournalEntry",
        "A journal entry: debits and credits posted to accounts.",
        (*_TXN, *_ID, _f("Adjustment", BOOLEAN), _f("TxnTaxDetail", JSON)),
    ),
    Entity(
        "Payment",
        "A payment received from a customer.",
        (
            *_TXN,
            *_ID,
            _f("CustomerRef", REF, filterable=True),
            _f("DepositToAccountRef", REF),
            _f("PaymentMethodRef", REF),
            _f("PaymentRefNum"),
            _f("UnappliedAmt", NUMBER),
            _f("ProcessPayment", BOOLEAN),
        ),
    ),
    Entity(
        "PaymentMethod",
        "A way a customer pays: cash, check, a card.",
        (
            *_ID,
            _f("Name", like=True),
            _f("Type"),
            _f("Active", BOOLEAN, filterable=True),
        ),
        has_active=True,
    ),
    Entity(
        "Purchase",
        "An expense paid by cash, check or card.",
        (
            *_TXN,
            *_ID,
            _f("AccountRef", REF),
            _f("PaymentType", filterable=True),
            _f("EntityRef", REF),
            _f("Credit", BOOLEAN),
            _f("PaymentMethodRef", REF),
            _f("RemitToAddr", JSON),
        ),
    ),
    Entity(
        "PurchaseOrder",
        "A purchase order sent to a vendor.",
        (
            *_TXN,
            *_ID,
            _f("VendorRef", REF, filterable=True),
            _f("APAccountRef", REF),
            _f("POStatus"),
            _f("DueDate", DATE),
            _f("ShipAddr", JSON),
            _f("VendorAddr", JSON),
            _f("POEmail", JSON),
        ),
    ),
    Entity(
        "RefundReceipt",
        "A refund paid to a customer.",
        (*_SALES, *_ID, _f("DepositToAccountRef", REF), _f("PaymentMethodRef", REF)),
    ),
    Entity(
        "SalesReceipt",
        "A sale paid in full when it was made.",
        (
            *_SALES,
            *_ID,
            _f("DepositToAccountRef", REF),
            _f("PaymentMethodRef", REF),
            _f("PaymentRefNum"),
            _f("Balance", NUMBER),
        ),
    ),
    Entity(
        "TaxAgency",
        "An agency sales tax is paid to.",
        (
            *_ID,
            _f("DisplayName", like=True),
            _f("TaxRegistrationNumber"),
            _f("TaxTrackedOnSales", BOOLEAN),
            _f("TaxTrackedOnPurchases", BOOLEAN),
        ),
    ),
    Entity(
        "TaxCode",
        "A sales tax code.",
        (
            *_ID,
            _f("Name", like=True),
            _f("Description"),
            _f("Active", BOOLEAN, filterable=True),
            _f("Taxable", BOOLEAN),
            _f("TaxGroup", BOOLEAN),
            _f("SalesTaxRateList", JSON),
            _f("PurchaseTaxRateList", JSON),
        ),
        has_active=True,
    ),
    Entity(
        "TaxRate",
        "A sales tax rate.",
        (
            *_ID,
            _f("Name", like=True),
            _f("Description"),
            _f("Active", BOOLEAN, filterable=True),
            _f("RateValue", NUMBER),
            _f("AgencyRef", REF),
            _f("SpecialTaxType"),
            _f("DisplayType"),
        ),
        has_active=True,
    ),
    Entity(
        "Term",
        "Payment terms: when a bill or invoice is due.",
        (
            *_ID,
            _f("Name", like=True),
            _f("Active", BOOLEAN, filterable=True),
            _f("Type"),
            _f("DueDays", INTEGER),
            _f("DiscountDays", INTEGER),
            _f("DiscountPercent", NUMBER),
            _f("DayOfMonthDue", INTEGER),
        ),
        has_active=True,
    ),
    Entity(
        "TimeActivity",
        "Time worked by an employee or vendor.",
        (
            *_ID,
            _f("TxnDate", DATE, filterable=True),
            _f("NameOf"),
            _f("EmployeeRef", REF),
            _f("VendorRef", REF),
            _f("CustomerRef", REF),
            _f("ItemRef", REF),
            _f("ClassRef", REF),
            _f("BillableStatus"),
            _f("Taxable", BOOLEAN),
            _f("HourlyRate", NUMBER),
            _f("Hours", INTEGER),
            _f("Minutes", INTEGER),
            _f("StartTime", TIMESTAMP),
            _f("EndTime", TIMESTAMP),
            _f("Description"),
        ),
    ),
    Entity(
        "Transfer",
        "Money moved between two accounts.",
        (
            *_ID,
            _f("TxnDate", DATE, filterable=True),
            _f("PrivateNote"),
            _f("Amount", NUMBER),
            _f("FromAccountRef", REF),
            _f("ToAccountRef", REF),
            *_CURRENCY,
        ),
    ),
    Entity(
        "Vendor",
        "A vendor: someone the company buys from.",
        (
            *_ID,
            *_PARTY,
            _f("AcctNum"),
            _f("Vendor1099", BOOLEAN),
            _f("TaxIdentifier"),
            _f("TermRef", REF),
        ),
        has_active=True,
    ),
    Entity(
        "VendorCredit",
        "A credit received from a vendor.",
        (
            *_TXN,
            *_ID,
            _f("VendorRef", REF, filterable=True),
            _f("APAccountRef", REF),
            _f("Balance", NUMBER),
        ),
    ),
)

ENTITIES: dict[str, Entity] = {e.name: e for e in _ENTITIES}


def entity(name: str) -> Entity:
    """The entity named ``name`` (as QuickBooks spells it, or as its table is named: ``Invoice``,
    ``invoice``, ``journal_entry``). Refused by name when QuickBooks has no such entity."""
    found = ENTITIES.get(name) or _BY_TABLE.get(name.lower())
    if found is None:
        raise KeyError(f"QuickBooks has no entity {name!r}; it offers {', '.join(ENTITIES)}")
    return found


def table_name(entity_name: str) -> str:
    """The table an entity is offered as: ``JournalEntry`` -> ``journal_entry``."""
    return snake(entity_name)


_BY_TABLE = {table_name(e.name): e for e in _ENTITIES}
