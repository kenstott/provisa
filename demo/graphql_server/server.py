# Copyright (c) 2026 Kenneth Stott
# Canary: 9cf11bbb-2341-4dce-a079-8309b0fda6da
# Canary: PLACEHOLDER
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Demo GraphQL server — animal shelter staff & breed management.

Exposes the staffing and breed-custody data that drives care protocols,
adoption placement decisions, and shift-coverage planning at the shelter.
"""

from __future__ import annotations

import strawberry
from strawberry.asgi import GraphQL
from typing import Optional


# ---------------------------------------------------------------------------
# Seed data
# ---------------------------------------------------------------------------

_BREEDS: list[dict] = [
    {
        "name": "Siamese",
        "species": "cat",
        "care_level": "moderate",
        "avg_lifespan_years": 15,
        "typical_habitat": "indoor",
        "description": "High-demand adoption candidate; vocal temperament requires experienced-adopter screening before placement",
    },
    {
        "name": "Maine Coon",
        "species": "cat",
        "care_level": "moderate",
        "avg_lifespan_years": 13,
        "typical_habitat": "indoor",
        "description": "Popular family placement match; longer intake cycle due to coat-maintenance training required for adopters",
    },
    {
        "name": "Golden Retriever",
        "species": "dog",
        "care_level": "high",
        "avg_lifespan_years": 11,
        "typical_habitat": "domestic",
        "description": "Top family placement breed; highest volunteer demand for daily socialization and exercise — drives kennel staffing peaks",
    },
    {
        "name": "African Lion",
        "species": "lion",
        "care_level": "expert",
        "avg_lifespan_years": 16,
        "typical_habitat": "savanna",
        "description": "Non-adoption large carnivore; accounts for the majority of specialist staffing costs and safety protocol overhead",
    },
    {
        "name": "Barbary Lion",
        "species": "lion",
        "care_level": "expert",
        "avg_lifespan_years": 18,
        "typical_habitat": "savanna",
        "description": "Conservation-custody animal; restricted handling rules and mandatory compliance reporting for every interaction",
    },
    {
        "name": "Holland Lop",
        "species": "rabbit",
        "care_level": "low",
        "avg_lifespan_years": 8,
        "typical_habitat": "indoor",
        "description": "Highest adoption turnover in the shelter; suitable for first-time owners, enabling short placement cycles and rapid intake throughput",
    },
]

_EMPLOYEES: list[dict] = [
    {
        "id": 1,
        "first_name": "Jordan",
        "last_name": "Alvarez",
        "hire_date": "2019-03-15",
        "department": "Large Carnivores",
        "contact": {
            "phone": "555-1001",
            "email": "j.alvarez@shelter.example",
            "ssn": "472-83-1901",
        },
    },
    {
        "id": 2,
        "first_name": "Morgan",
        "last_name": "Patel",
        "hire_date": "2020-07-01",
        "department": "Large Carnivores",
        "contact": {"phone": "555-1002", "email": "m.patel@shelter.example", "ssn": "319-54-7723"},
    },
    {
        "id": 3,
        "first_name": "Casey",
        "last_name": "Okonkwo",
        "hire_date": "2021-01-20",
        "department": "Domestic Animals",
        "contact": {
            "phone": "555-1003",
            "email": "c.okonkwo@shelter.example",
            "ssn": "584-22-6601",
        },
    },
    {
        "id": 4,
        "first_name": "Riley",
        "last_name": "Chen",
        "hire_date": "2021-06-10",
        "department": "Domestic Animals",
        "contact": {"phone": "555-1004", "email": "r.chen@shelter.example", "ssn": "261-79-3345"},
    },
    {
        "id": 5,
        "first_name": "Taylor",
        "last_name": "Nguyen",
        "hire_date": "2022-02-28",
        "department": "Domestic Animals",
        "contact": {"phone": "555-1005", "email": "t.nguyen@shelter.example", "ssn": "703-41-8812"},
    },
    {
        "id": 6,
        "first_name": "Avery",
        "last_name": "Sousa",
        "hire_date": "2022-09-05",
        "department": "Large Carnivores",
        "contact": {"phone": "555-1006", "email": "a.sousa@shelter.example", "ssn": "438-67-2294"},
    },
    {
        "id": 7,
        "first_name": "Quinn",
        "last_name": "Yamamoto",
        "hire_date": "2023-04-17",
        "department": "Domestic Animals",
        "contact": {
            "phone": "555-1007",
            "email": "q.yamamoto@shelter.example",
            "ssn": "156-93-5570",
        },
    },
    {
        "id": 8,
        "first_name": "Sage",
        "last_name": "Williams",
        "hire_date": "2023-08-01",
        "department": "Small Animals",
        "contact": {
            "phone": "555-1008",
            "email": "s.williams@shelter.example",
            "ssn": "829-35-1147",
        },
    },
    {
        "id": 9,
        "first_name": "Drew",
        "last_name": "Ferreira",
        "hire_date": "2024-01-15",
        "department": "Large Carnivores",
        "contact": {
            "phone": "555-1009",
            "email": "d.ferreira@shelter.example",
            "ssn": "617-28-9983",
        },
    },
    {
        "id": 10,
        "first_name": "Parker",
        "last_name": "Hassan",
        "hire_date": "2024-06-01",
        "department": "Small Animals",
        "contact": {"phone": "555-1010", "email": "p.hassan@shelter.example", "ssn": "394-51-6628"},
    },
]

_ASSIGNMENTS: list[dict] = [
    {
        "id": 1,
        "employee_id": 1,
        "breed_name": "African Lion",
        "role": "primary_keeper",
        "start_date": "2019-04-01",
        "end_date": None,
    },
    {
        "id": 2,
        "employee_id": 2,
        "breed_name": "African Lion",
        "role": "backup_keeper",
        "start_date": "2020-08-01",
        "end_date": None,
    },
    {
        "id": 3,
        "employee_id": 6,
        "breed_name": "African Lion",
        "role": "backup_keeper",
        "start_date": "2022-10-01",
        "end_date": None,
    },
    {
        "id": 4,
        "employee_id": 9,
        "breed_name": "African Lion",
        "role": "trainee",
        "start_date": "2024-02-01",
        "end_date": None,
    },
    {
        "id": 5,
        "employee_id": 1,
        "breed_name": "Barbary Lion",
        "role": "primary_keeper",
        "start_date": "2021-05-15",
        "end_date": None,
    },
    {
        "id": 6,
        "employee_id": 2,
        "breed_name": "Barbary Lion",
        "role": "backup_keeper",
        "start_date": "2021-05-15",
        "end_date": None,
    },
    {
        "id": 7,
        "employee_id": 3,
        "breed_name": "Siamese",
        "role": "primary_keeper",
        "start_date": "2021-02-01",
        "end_date": None,
    },
    {
        "id": 8,
        "employee_id": 4,
        "breed_name": "Siamese",
        "role": "backup_keeper",
        "start_date": "2021-07-01",
        "end_date": None,
    },
    {
        "id": 9,
        "employee_id": 3,
        "breed_name": "Maine Coon",
        "role": "primary_keeper",
        "start_date": "2021-02-01",
        "end_date": None,
    },
    {
        "id": 10,
        "employee_id": 5,
        "breed_name": "Maine Coon",
        "role": "backup_keeper",
        "start_date": "2022-03-15",
        "end_date": None,
    },
    {
        "id": 11,
        "employee_id": 4,
        "breed_name": "Golden Retriever",
        "role": "primary_keeper",
        "start_date": "2021-06-10",
        "end_date": None,
    },
    {
        "id": 12,
        "employee_id": 7,
        "breed_name": "Golden Retriever",
        "role": "backup_keeper",
        "start_date": "2023-05-01",
        "end_date": None,
    },
    {
        "id": 13,
        "employee_id": 8,
        "breed_name": "Holland Lop",
        "role": "primary_keeper",
        "start_date": "2023-08-01",
        "end_date": None,
    },
    {
        "id": 14,
        "employee_id": 10,
        "breed_name": "Holland Lop",
        "role": "backup_keeper",
        "start_date": "2024-07-01",
        "end_date": None,
    },
]

_SCHEDULES: list[dict] = [
    {
        "id": 1,
        "employee_id": 1,
        "day_of_week": "Monday",
        "shift_start": "07:00",
        "shift_end": "15:00",
        "location": "Large Carnivore Block A",
    },
    {
        "id": 2,
        "employee_id": 1,
        "day_of_week": "Tuesday",
        "shift_start": "07:00",
        "shift_end": "15:00",
        "location": "Large Carnivore Block A",
    },
    {
        "id": 3,
        "employee_id": 1,
        "day_of_week": "Wednesday",
        "shift_start": "07:00",
        "shift_end": "15:00",
        "location": "Large Carnivore Block A",
    },
    {
        "id": 4,
        "employee_id": 1,
        "day_of_week": "Thursday",
        "shift_start": "07:00",
        "shift_end": "15:00",
        "location": "Large Carnivore Block A",
    },
    {
        "id": 5,
        "employee_id": 1,
        "day_of_week": "Friday",
        "shift_start": "07:00",
        "shift_end": "15:00",
        "location": "Large Carnivore Block A",
    },
    {
        "id": 6,
        "employee_id": 2,
        "day_of_week": "Monday",
        "shift_start": "15:00",
        "shift_end": "23:00",
        "location": "Large Carnivore Block A",
    },
    {
        "id": 7,
        "employee_id": 2,
        "day_of_week": "Wednesday",
        "shift_start": "15:00",
        "shift_end": "23:00",
        "location": "Large Carnivore Block A",
    },
    {
        "id": 8,
        "employee_id": 2,
        "day_of_week": "Friday",
        "shift_start": "15:00",
        "shift_end": "23:00",
        "location": "Large Carnivore Block A",
    },
    {
        "id": 9,
        "employee_id": 2,
        "day_of_week": "Saturday",
        "shift_start": "08:00",
        "shift_end": "16:00",
        "location": "Large Carnivore Block B",
    },
    {
        "id": 10,
        "employee_id": 3,
        "day_of_week": "Monday",
        "shift_start": "08:00",
        "shift_end": "16:00",
        "location": "Domestic Cat Wing",
    },
    {
        "id": 11,
        "employee_id": 3,
        "day_of_week": "Tuesday",
        "shift_start": "08:00",
        "shift_end": "16:00",
        "location": "Domestic Cat Wing",
    },
    {
        "id": 12,
        "employee_id": 3,
        "day_of_week": "Wednesday",
        "shift_start": "08:00",
        "shift_end": "16:00",
        "location": "Domestic Cat Wing",
    },
    {
        "id": 13,
        "employee_id": 3,
        "day_of_week": "Thursday",
        "shift_start": "08:00",
        "shift_end": "16:00",
        "location": "Domestic Cat Wing",
    },
    {
        "id": 14,
        "employee_id": 4,
        "day_of_week": "Tuesday",
        "shift_start": "09:00",
        "shift_end": "17:00",
        "location": "Domestic Cat Wing",
    },
    {
        "id": 15,
        "employee_id": 4,
        "day_of_week": "Thursday",
        "shift_start": "09:00",
        "shift_end": "17:00",
        "location": "Dog Kennels",
    },
    {
        "id": 16,
        "employee_id": 4,
        "day_of_week": "Friday",
        "shift_start": "09:00",
        "shift_end": "17:00",
        "location": "Dog Kennels",
    },
    {
        "id": 17,
        "employee_id": 4,
        "day_of_week": "Saturday",
        "shift_start": "09:00",
        "shift_end": "17:00",
        "location": "Dog Kennels",
    },
    {
        "id": 18,
        "employee_id": 5,
        "day_of_week": "Monday",
        "shift_start": "10:00",
        "shift_end": "18:00",
        "location": "Domestic Cat Wing",
    },
    {
        "id": 19,
        "employee_id": 5,
        "day_of_week": "Wednesday",
        "shift_start": "10:00",
        "shift_end": "18:00",
        "location": "Domestic Cat Wing",
    },
    {
        "id": 20,
        "employee_id": 5,
        "day_of_week": "Friday",
        "shift_start": "10:00",
        "shift_end": "18:00",
        "location": "Domestic Cat Wing",
    },
    {
        "id": 21,
        "employee_id": 6,
        "day_of_week": "Tuesday",
        "shift_start": "07:00",
        "shift_end": "15:00",
        "location": "Large Carnivore Block A",
    },
    {
        "id": 22,
        "employee_id": 6,
        "day_of_week": "Thursday",
        "shift_start": "07:00",
        "shift_end": "15:00",
        "location": "Large Carnivore Block A",
    },
    {
        "id": 23,
        "employee_id": 6,
        "day_of_week": "Saturday",
        "shift_start": "07:00",
        "shift_end": "15:00",
        "location": "Large Carnivore Block B",
    },
    {
        "id": 24,
        "employee_id": 7,
        "day_of_week": "Monday",
        "shift_start": "09:00",
        "shift_end": "17:00",
        "location": "Dog Kennels",
    },
    {
        "id": 25,
        "employee_id": 7,
        "day_of_week": "Wednesday",
        "shift_start": "09:00",
        "shift_end": "17:00",
        "location": "Dog Kennels",
    },
    {
        "id": 26,
        "employee_id": 7,
        "day_of_week": "Friday",
        "shift_start": "09:00",
        "shift_end": "17:00",
        "location": "Dog Kennels",
    },
    {
        "id": 27,
        "employee_id": 8,
        "day_of_week": "Tuesday",
        "shift_start": "08:00",
        "shift_end": "16:00",
        "location": "Small Animal Room",
    },
    {
        "id": 28,
        "employee_id": 8,
        "day_of_week": "Thursday",
        "shift_start": "08:00",
        "shift_end": "16:00",
        "location": "Small Animal Room",
    },
    {
        "id": 29,
        "employee_id": 8,
        "day_of_week": "Saturday",
        "shift_start": "08:00",
        "shift_end": "16:00",
        "location": "Small Animal Room",
    },
    {
        "id": 30,
        "employee_id": 9,
        "day_of_week": "Wednesday",
        "shift_start": "15:00",
        "shift_end": "23:00",
        "location": "Large Carnivore Block B",
    },
    {
        "id": 31,
        "employee_id": 9,
        "day_of_week": "Friday",
        "shift_start": "15:00",
        "shift_end": "23:00",
        "location": "Large Carnivore Block B",
    },
    {
        "id": 32,
        "employee_id": 9,
        "day_of_week": "Sunday",
        "shift_start": "08:00",
        "shift_end": "16:00",
        "location": "Large Carnivore Block A",
    },
    {
        "id": 33,
        "employee_id": 10,
        "day_of_week": "Monday",
        "shift_start": "10:00",
        "shift_end": "18:00",
        "location": "Small Animal Room",
    },
    {
        "id": 34,
        "employee_id": 10,
        "day_of_week": "Wednesday",
        "shift_start": "10:00",
        "shift_end": "18:00",
        "location": "Small Animal Room",
    },
    {
        "id": 35,
        "employee_id": 10,
        "day_of_week": "Friday",
        "shift_start": "10:00",
        "shift_end": "18:00",
        "location": "Small Animal Room",
    },
]


# ---------------------------------------------------------------------------
# GraphQL types
# ---------------------------------------------------------------------------


@strawberry.type
class AnimalBreed:
    """Catalog of species and breeds in shelter custody — used to standardize care protocols, adoption eligibility rules, and staffing assignments."""

    name: str
    species: str
    care_level: str
    avg_lifespan_years: int
    typical_habitat: str
    description: str


@strawberry.type
class ContactInfo:
    """Staff contact and identity details — restricted to authorized roles."""

    phone: str
    email: str
    ssn: str


@strawberry.type
class Employee:
    """Shelter staff roster — the source of truth for who is authorized to receive breed assignments and appear on shift schedules."""

    id: int
    first_name: str
    last_name: str
    hire_date: str
    department: str
    contact: ContactInfo


@strawberry.type
class Assignment:
    """Accountability record linking a staff member to a breed — drives care coverage decisions and identifies single-point-of-failure risks when a primary keeper is unavailable."""

    id: int
    employee_id: int
    breed_name: str
    employee: Employee
    breed: AnimalBreed
    role: str
    start_date: str
    end_date: Optional[str]


@strawberry.type
class Schedule:
    """Weekly shift roster for all staff — used to verify that every breed has licensed keeper coverage on every day the shelter operates."""

    id: int
    employee: Employee
    day_of_week: str
    shift_start: str
    shift_end: str
    location: str


def _make_employee(e: dict) -> Employee:
    return Employee(
        id=e["id"],
        first_name=e["first_name"],
        last_name=e["last_name"],
        hire_date=e["hire_date"],
        department=e["department"],
        contact=ContactInfo(**e["contact"]),
    )


def _make_assignment(a: dict) -> Assignment:
    emp = next((_make_employee(e) for e in _EMPLOYEES if e["id"] == a["employee_id"]))
    breed = next((AnimalBreed(**b) for b in _BREEDS if b["name"] == a["breed_name"]))
    return Assignment(
        id=a["id"],
        employee_id=a["employee_id"],
        breed_name=a["breed_name"],
        employee=emp,
        breed=breed,
        role=a["role"],
        start_date=a["start_date"],
        end_date=a["end_date"],
    )


def _make_schedule(s: dict) -> Schedule:
    emp = next((_make_employee(e) for e in _EMPLOYEES if e["id"] == s["employee_id"]))
    return Schedule(
        id=s["id"],
        employee=emp,
        day_of_week=s["day_of_week"],
        shift_start=s["shift_start"],
        shift_end=s["shift_end"],
        location=s["location"],
    )


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------


@strawberry.type
class Query:
    @strawberry.field(
        description="Full breed catalog — use to answer what care protocols apply and which adopter profiles match each species in custody"
    )
    def animal_breeds(self) -> list[AnimalBreed]:
        return [AnimalBreed(**b) for b in _BREEDS]

    @strawberry.field(
        description="Care requirements and placement context for a single breed — use before making a staffing assignment or adoption decision"
    )
    def animal_breed(self, name: str) -> Optional[AnimalBreed]:
        row = next((b for b in _BREEDS if b["name"] == name), None)
        return AnimalBreed(**row) if row else None

    @strawberry.field(
        description="All staff eligible for breed assignments — use to identify who can be assigned or scheduled"
    )
    def employees(self) -> list[Employee]:
        return [_make_employee(e) for e in _EMPLOYEES]

    @strawberry.field(
        description="Contact details and department for a single staff member — use to route assignment requests or resolve scheduling conflicts"
    )
    def employee(self, id: int) -> Optional[Employee]:
        row = next((e for e in _EMPLOYEES if e["id"] == id), None)
        return _make_employee(row) if row else None

    @strawberry.field(
        description="Complete accountability map of staff to breeds — use to spot coverage gaps or single-keeper risk across all species"
    )
    def assignments(self) -> list[Assignment]:
        return [_make_assignment(a) for a in _ASSIGNMENTS]

    @strawberry.field(
        description="All breeds a given staff member is accountable for — use to assess workload before adding a new assignment"
    )
    def assignments_by_employee(self, employee_id: int) -> list[Assignment]:
        return [_make_assignment(a) for a in _ASSIGNMENTS if a["employee_id"] == employee_id]

    @strawberry.field(
        description="All staff responsible for a breed — use to verify minimum keeper coverage and identify backup gaps"
    )
    def assignments_by_breed(self, breed_name: str) -> list[Assignment]:
        return [_make_assignment(a) for a in _ASSIGNMENTS if a["breed_name"] == breed_name]

    @strawberry.field(
        description="Full weekly shift roster — use for shelter-wide coverage analysis and to surface days where any location is understaffed"
    )
    def schedules(self) -> list[Schedule]:
        return [_make_schedule(s) for s in _SCHEDULES]

    @strawberry.field(
        description="A staff member's shift coverage for the week — use for availability checks before scheduling a new assignment or event"
    )
    def schedule_by_employee(self, employee_id: int) -> list[Schedule]:
        return [_make_schedule(s) for s in _SCHEDULES if s["employee_id"] == employee_id]


schema = strawberry.Schema(query=Query)
app = GraphQL(schema)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=4000)
