#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 3a7f9d12-e841-4c5b-b203-8f1d6c2e7a94
#
# This source code is licensed under the Business Source License 1.1

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, field_validator, model_validator


class Status(str, Enum):
    proposed = "proposed"
    accepted = "accepted"
    in_progress = "in-progress"
    complete = "complete"
    rejected = "rejected"


class Priority(str, Enum):
    MUST = "MUST"
    SHOULD = "SHOULD"
    MAY = "MAY"


class ReqType(str, Enum):
    behavioral = "behavioral"
    structural = "structural"
    constraint = "constraint"
    ui = "ui"
    infrastructure = "infrastructure"


class CompetitiveStatus(str, Enum):
    ahead = "ahead"
    parity = "parity"
    gap = "gap"
    neutral = "neutral"


class CompetitivePosition(BaseModel):
    status: CompetitiveStatus
    rationale: str


class IntegrationTestJudgement(str, Enum):
    required = "required"
    not_required = "not_required"
    deferred = "deferred"


class Stakeholder(str, Enum):
    data_engineer = "data-engineer"
    compliance = "compliance"
    app_developer = "app-developer"
    ops = "ops"
    executive = "executive"


_COVERAGE_STATUSES = {Status.accepted, Status.in_progress, Status.complete}
_NEEDS_SCENARIO = {Status.accepted, Status.in_progress, Status.complete}


class Requirement(BaseModel):
    id: str
    status: Status
    group: str
    category: str
    priority: Priority
    type: ReqType
    description: str
    use_case: Optional[str] = None
    code: Optional[list[str]] = None
    tests: Optional[list[str]] = None
    integration_test: Optional[IntegrationTestJudgement] = None
    integration_test_reason: Optional[str] = None
    e2e: Optional[bool] = None
    e2e_reason: Optional[str] = None
    stakeholders: Optional[list[Stakeholder]] = None
    scenario: Optional[str] = None
    since: Optional[str] = None
    target: Optional[str] = None
    rejection_reason: Optional[str] = None
    competitive_position: Optional[CompetitivePosition] = None

    @field_validator("id")
    @classmethod
    def id_format(cls, v: str) -> str:
        if not v.startswith("REQ-") or not v[4:].isdigit():
            raise ValueError(f"id must be REQ-NNN, got {v!r}")
        return v

    @field_validator("since")
    @classmethod
    def since_format(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        parts = v.split("-")
        if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
            raise ValueError(f"since must be YYYY-MM, got {v!r}")
        return v

    @field_validator("target")
    @classmethod
    def target_format(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        import datetime as _dt

        parts = v.split("-")
        if len(parts) == 2 and parts[0].isdigit() and parts[1] in {"Q1", "Q2", "Q3", "Q4"}:
            return v
        try:
            _dt.date.fromisoformat(v)
            return v
        except ValueError:
            pass
        raise ValueError(f"target must be YYYY-QN or YYYY-MM-DD, got {v!r}")

    @model_validator(mode="after")
    def cross_field_rules(self) -> Requirement:
        errors: list[str] = []

        if self.status == Status.rejected and not self.rejection_reason:
            errors.append("rejection_reason required when status=rejected")

        if self.type == ReqType.behavioral and self.status in _NEEDS_SCENARIO and not self.scenario:
            errors.append(
                f"scenario required for behavioral requirements with status={self.status.value}"
            )

        if (
            self.priority == Priority.MUST
            and self.status == Status.complete
            and self.type in {ReqType.behavioral, ReqType.constraint}
            and not self.tests
        ):
            errors.append("tests required for MUST complete behavioral/constraint requirements")

        if self.status == Status.complete and not self.since:
            errors.append("since required when status=complete")

        if self.status in {Status.proposed, Status.accepted} and not self.target:
            # advisory only — warn but don't error
            pass

        if errors:
            raise ValueError(f"{self.id}: " + "; ".join(errors))

        return self


class RequirementsFile(BaseModel):
    requirements: list[Requirement]

    @model_validator(mode="after")
    def unique_ids(self) -> RequirementsFile:
        seen: set[str] = set()
        dupes: list[str] = []
        for req in self.requirements:
            if req.id in seen:
                dupes.append(req.id)
            seen.add(req.id)
        if dupes:
            raise ValueError(f"Duplicate REQ IDs: {dupes}")
        return self

    @classmethod
    def load(cls, path: Path) -> RequirementsFile:
        raw = yaml.safe_load(path.read_text())
        return cls(requirements=raw)

    def by_id(self, req_id: str) -> Optional[Requirement]:
        for r in self.requirements:
            if r.id == req_id:
                return r
        return None

    def next_id(self) -> str:
        nums = [int(r.id[4:]) for r in self.requirements]
        return f"REQ-{max(nums) + 1:03d}" if nums else "REQ-001"
