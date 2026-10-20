"""Write the cloudpolicy demo's data: the cloud inventory and the ServiceNow incidents as the rows
the demo declares for its two faked sources (replica_rows/), and fourteen days of the daily review
(db/history.sql), each day classified by the same rules as the daily_review view:

    .venv/bin/python3 demo/named/cloudpolicy/generate.py

Seeded, so the same rows come out on every run. The inventory is as it stood on REVIEW_DAY."""

from __future__ import annotations

import hashlib
import random
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path

HERE = Path(__file__).parent
REVIEW_DAY = date(2026, 10, 20)
HISTORY_DAYS = 14
EXCEPTION_DAYS = 90
DEADLINE_DAYS = {1: 7, 2: 14, 3: 30, 4: 90}  # §6.1, by incident priority

TEAMS = ("payments", "checkout", "analytics", "identity")
ACCOUNTS = {
    "Azure": [
        ("3f2a9c1e-7b4d-4e8a-9c21-5d6e7f8a9b0c", "eastus"),
        ("8b0e4d27-1c6a-4f39-a5e2-90d1c3b7e468", "westeurope"),
    ],
    "AWS": [("412907335521", "us-east-1"), ("803164229870", "eu-west-1")],
    "GCP": [("acme-payments-prod", "us-central1"), ("acme-analytics-prod", "europe-west1")],
}
DISK_TYPE = {"Azure": "Managed Disk", "AWS": "EBS Volume", "GCP": "Persistent Disk"}
INSTANCE_TYPE = {"Azure": "Standard_D4s_v5", "AWS": "m6i.xlarge", "GCP": "n2-standard-4"}

STORAGE_COLUMNS = (
    "cloud_provider account_id resource_name storage_type application region resource_group "
    "resource_id size_bytes storage_class replication_type encryption_enabled encryption_type "
    "encryption_key_type public_access_enabled public_access_level network_restrictions https_only "
    "versioning_enabled soft_delete_enabled soft_delete_retention_days backup_enabled "
    "lifecycle_rules_count access_tier created_date modified_date tags key_id attached_instance_id"
).split()
COMPUTE_COLUMNS = (
    "cloud_provider account_id instance_id instance_name application region availability_zone "
    "resource_group resource_id instance_type state platform architecture virtualization_type "
    "public_ip private_ip vpc_id subnet_id iam_role security_groups disk_encryption_enabled "
    "monitoring_enabled launch_time"
).split()
INCIDENT_COLUMNS = (
    "sys_id number short_description state priority category opened_at resolved_at "
    "u_policy_control assignment_group assignment_group__display cmdb_ci cmdb_ci__display"
).split()
OPEN, IN_PROGRESS, ON_HOLD, RESOLVED, CLOSED = 1, 2, 3, 6, 7
REMEDIATION, EXCEPTION = "Security", "Policy exception"


def sys_id(*parts: object) -> str:
    return hashlib.md5("/".join(map(str, parts)).encode()).hexdigest()  # noqa: S324 -- an id, not a secret


@dataclass
class Vm:
    cloud: str
    account: str
    region: str
    name: str
    team: str
    database: bool
    public_ip: str | None = None

    @property
    def resource_id(self) -> str:
        if self.cloud == "AWS":
            return f"arn:aws:ec2:{self.region}:{self.account}:instance/{self.instance_id}"
        if self.cloud == "GCP":
            return f"https://www.googleapis.com/compute/v1/projects/{self.account}/zones/{self.region}-a/instances/{self.name}"
        return f"/subscriptions/{self.account}/resourceGroups/rg-{self.team}/providers/Microsoft.Compute/virtualMachines/{self.name}"

    @property
    def instance_id(self) -> str:
        return {
            "AWS": "i-" + sys_id(self.name)[:17],
            "GCP": str(int(sys_id(self.name)[:15], 16)),
            "Azure": sys_id(self.name),
        }[self.cloud]


@dataclass
class Disk:
    vm: Vm | None
    cloud: str
    account: str
    region: str
    name: str
    team: str
    cmek: bool
    created: date
    fixed_on: date | None = None  # a violation fixed during the history: CMEK from this day

    @property
    def resource_id(self) -> str:
        if self.cloud == "AWS":
            return f"arn:aws:ec2:{self.region}:{self.account}:volume/vol-{sys_id(self.name)[:17]}"
        if self.cloud == "GCP":
            return f"projects/{self.account}/zones/{self.region}-a/disks/{self.name}"
        return f"/subscriptions/{self.account}/resourceGroups/rg-{self.team}/providers/Microsoft.Compute/disks/{self.name}"

    def key_id(self) -> str:
        if self.cloud == "AWS":
            return f"arn:aws:kms:{self.region}:{self.account}:key/{sys_id('key', self.team, self.account)[:8]}-{sys_id('k', self.team)[:4]}-4{sys_id('k2', self.team)[:3]}-8{sys_id('k3', self.team)[:3]}-{sys_id('k4', self.team)[:12]}"
        if self.cloud == "GCP":
            return f"projects/{self.account}/locations/{self.region}/keyRings/{self.team}-ring/cryptoKeys/{self.team}-disks"
        return f"https://kv-{self.team}-prod.vault.azure.net/keys/{self.team}-disks/{sys_id('ver', self.team)}"

    def cmek_on(self, day: date) -> bool:
        return self.cmek and (self.fixed_on is None or day >= self.fixed_on)


@dataclass
class Incident:
    number: str
    disk_name: str
    team: str
    category: str
    control: str
    priority: int
    opened: datetime
    resolved: datetime | None = None
    state: int = IN_PROGRESS
    description: str = ""
    ci_name: str | None = None  # the CI the incident names; the disk's own name unless it is stale
    extra: dict = field(default_factory=dict)


def build(rng: random.Random):
    vms: list[Vm] = []
    for cloud, accounts in ACCOUNTS.items():
        for n in range(37):
            account, region = accounts[n % 2]
            team = TEAMS[(n + len(cloud)) % 4]
            database = n % 6 == 0
            role = "db" if database else rng.choice(("app", "api", "worker", "batch"))
            vms.append(
                Vm(
                    cloud,
                    account,
                    region,
                    f"{team[:3]}-{role}-{cloud.lower()}-{n:02d}",
                    team,
                    database,
                )
            )
    # §5.1: three database servers with a public address.
    for vm, ip in zip(
        [v for v in vms if v.database][1::5][:3],
        ("20.81.112.40", "54.210.33.17", "34.122.9.201"),
        strict=True,
    ):
        vm.public_ip = ip

    disks: list[Disk] = []
    for vm in vms:
        for k in range(1 if vm.name.endswith(("3", "7")) else 2):
            created = REVIEW_DAY - timedelta(days=rng.randint(60, 700))
            disks.append(
                Disk(
                    vm,
                    vm.cloud,
                    vm.account,
                    vm.region,
                    f"{vm.name}-{'os' if k == 0 else 'data'}",
                    vm.team,
                    True,
                    created,
                )
            )
    for n in range(14):  # unattached: snapshots' restores, spares; §4.2 does not apply
        cloud = ("Azure", "AWS", "GCP")[n % 3]
        account, region = ACCOUNTS[cloud][n % 2]
        disks.append(
            Disk(
                None,
                cloud,
                account,
                region,
                f"spare-{cloud.lower()}-{n:02d}",
                TEAMS[n % 4],
                n % 3 != 0,
                REVIEW_DAY - timedelta(days=rng.randint(30, 400)),
            )
        )

    attached = [d for d in disks if d.vm is not None]
    rng.shuffle(attached)
    violating = attached[:30]
    fixed = attached[30:36]  # violations fixed during the history
    for d in violating:
        d.cmek = False
    incidents: list[Incident] = []
    seq = iter(range(10200, 10999))

    def incident(d: Disk, **kw) -> Incident:
        made = Incident(number=f"INC00{next(seq)}", disk_name=d.name, team=d.team, **kw)
        incidents.append(made)
        return made

    def at(days_ago: int, hour: int = 9) -> datetime:
        return datetime.combine(REVIEW_DAY - timedelta(days=days_ago), time(hour, 15))

    groups = {
        "ticketed": violating[0:12],
        "unticketed": violating[12:16],
        "overdue": violating[16:20],
        "closed": violating[20:23],
        "exception": violating[23:26],
        "expired": violating[26:28],
        "stale_ci": violating[28:29],
        "late_ticket": violating[29:30],
    }
    for i, d in enumerate(groups["ticketed"]):
        priority = 2 if i % 3 else 3
        opened = at(rng.randint(1, DEADLINE_DAYS[priority] - 1))
        control = "4.2"
        if i == 4:
            control = "5.1"  # cites the wrong control
        elif i == 8:
            control = "6.1"  # cites the deadline, not the control broken
        elif i == 10:
            control = "4.1"  # cites the withdrawn clause
        incident(
            d,
            category=REMEDIATION,
            control=control,
            priority=priority,
            opened=opened,
            state=rng.choice((OPEN, IN_PROGRESS)),
        )
    for d in groups["overdue"]:
        priority = rng.choice((1, 2))
        incident(
            d,
            category=REMEDIATION,
            control="4.2",
            priority=priority,
            opened=at(DEADLINE_DAYS[priority] + rng.randint(3, 20)),
            state=rng.choice((IN_PROGRESS, ON_HOLD)),
        )
    for d in groups["closed"]:
        opened = at(rng.randint(20, 40))
        incident(
            d,
            category=REMEDIATION,
            control="4.2",
            priority=3,
            opened=opened,
            resolved=opened + timedelta(days=rng.randint(2, 6)),
            state=CLOSED,
        )
    for d in groups["exception"]:
        incident(
            d,
            category=EXCEPTION,
            control="4.3",
            priority=4,
            opened=at(rng.randint(10, 80)),
            state=ON_HOLD,
        )
    for d in groups["expired"]:
        incident(
            d,
            category=EXCEPTION,
            control="4.3",
            priority=4,
            opened=at(rng.randint(95, 140)),
            state=ON_HOLD,
        )
    for d in groups["stale_ci"]:
        # The CMDB still names the disk this one replaced: the incident matches nothing in the inventory.
        incident(
            d,
            category=REMEDIATION,
            control="4.2",
            priority=2,
            opened=at(5),
            state=IN_PROGRESS,
            ci_name=d.name.replace("-data", "-data-old").replace("-os", "-os-old"),
        )
    for d in groups["late_ticket"]:
        incident(d, category=REMEDIATION, control="4.2", priority=3, opened=at(3), state=OPEN)
    for i, d in enumerate(fixed):
        opened = at(rng.randint(14, 25))
        resolved = datetime.combine(REVIEW_DAY - timedelta(days=rng.randint(1, 11)), time(16, 40))
        d.fixed_on = resolved.date()
        incident(
            d,
            category=REMEDIATION,
            control="4.2",
            priority=2 if i % 2 else 3,
            opened=opened,
            resolved=resolved,
            state=RESOLVED if i % 2 else CLOSED,
        )
    for inc in incidents:
        what = (
            "exception to disk encryption"
            if inc.category == EXCEPTION
            else "disk not encrypted with a customer-managed key"
        )
        inc.description = f"{inc.ci_name or inc.disk_name}: {what}"
    return vms, disks, incidents


def status_on(day: date, d: Disk, incidents: list[Incident]) -> tuple[str, str | None]:
    """The daily review's rule, as the daily_review view states it, for the disk ``d`` on ``day``."""
    if d.cmek_on(day):
        return "compliant", None
    known = [
        i for i in incidents if (i.ci_name or i.disk_name) == d.name and i.opened.date() <= day
    ]
    exceptions = sorted(
        (i for i in known if i.category == EXCEPTION), key=lambda i: i.opened, reverse=True
    )
    if exceptions and (day - exceptions[0].opened.date()).days < EXCEPTION_DAYS:
        return "covered by an exception", exceptions[0].number
    remediation = sorted(
        (i for i in known if i.category == REMEDIATION), key=lambda i: i.opened, reverse=True
    )
    open_now = [i for i in remediation if i.resolved is None or i.resolved.date() > day]
    if open_now:
        i = open_now[0]
        late = (day - i.opened.date()).days > DEADLINE_DAYS[i.priority]
        return ("overdue", i.number) if late else ("violation, ticketed", i.number)
    if remediation:
        return "closed, still not compliant", remediation[0].number
    if exceptions:
        return "exception expired", exceptions[0].number
    return "violation, no ticket", None


def tsv(value: object) -> str:
    if value is None:
        return r"\N"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return str(value).replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")


def write_tsv(path: Path, columns: Sequence[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["\t".join(columns)] + ["\t".join(tsv(r[c]) for c in columns) for r in rows]
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    rng = random.Random(2102)
    vms, disks, incidents = build(rng)
    created = datetime(2024, 3, 4, 10, 0)
    compute = [
        {
            "cloud_provider": v.cloud,
            "account_id": v.account,
            "instance_id": v.instance_id,
            "instance_name": v.name,
            "application": v.team,
            "region": v.region,
            "availability_zone": f"{v.region}-a" if v.cloud != "Azure" else "1",
            "resource_group": f"rg-{v.team}" if v.cloud == "Azure" else None,
            "resource_id": v.resource_id,
            "instance_type": INSTANCE_TYPE[v.cloud],
            "state": "running",
            "platform": "Linux",
            "architecture": "x86_64",
            "virtualization_type": "hvm",
            "public_ip": v.public_ip,
            "private_ip": f"10.{len(v.team)}.{sum(map(ord, v.name)) % 250}.{len(v.name) * 3}",
            "vpc_id": f"vnet-{v.team}" if v.cloud == "Azure" else f"vpc-{v.team}",
            "subnet_id": f"snet-{'data' if v.database else 'app'}",
            "iam_role": f"{v.team}-runtime",
            "security_groups": f'["sg-{v.team}-{"db" if v.database else "app"}"]',
            "disk_encryption_enabled": all(d.cmek for d in disks if d.vm is v),
            "monitoring_enabled": True,
            "launch_time": created + timedelta(days=sum(map(ord, v.name)) % 400),
        }
        for v in vms
    ]
    storage = [
        {
            "cloud_provider": d.cloud,
            "account_id": d.account,
            "resource_name": d.name,
            "storage_type": DISK_TYPE[d.cloud],
            "application": d.team,
            "region": d.region,
            "resource_group": f"rg-{d.team}" if d.cloud == "Azure" else None,
            "resource_id": d.resource_id,
            "size_bytes": (128 if d.name.endswith("-os") else 512) * 1024**3,
            "storage_class": {"Azure": "Premium_LRS", "AWS": "gp3", "GCP": "pd-balanced"}[d.cloud],
            "replication_type": "LRS" if d.cloud == "Azure" else None,
            "encryption_enabled": True,
            "encryption_type": ("Customer Managed Key" if d.cmek else "Service Managed Key")
            if d.cloud == "Azure"
            else ("aws:kms" if d.cloud == "AWS" else "CMEK" if d.cmek else "Google-managed"),
            "encryption_key_type": "customer-managed" if d.cmek else "service-managed",
            "public_access_enabled": False,
            "public_access_level": None,
            "network_restrictions": None,
            "https_only": None,
            "versioning_enabled": None,
            "soft_delete_enabled": None,
            "soft_delete_retention_days": None,
            "backup_enabled": True,
            "lifecycle_rules_count": None,
            "access_tier": None,
            "created_date": datetime.combine(d.created, time(11, 5)),
            "modified_date": datetime.combine(d.fixed_on or d.created, time(16, 40)),
            "tags": f'{{"team": "{d.team}", "env": "prod"}}',
            "key_id": d.key_id() if d.cmek else None,
            "attached_instance_id": d.vm.resource_id if d.vm else None,
        }
        for d in disks
    ]
    groups = {t: (sys_id("group", t), t.capitalize()) for t in TEAMS}
    incident_rows = [
        {
            "sys_id": sys_id("incident", i.number),
            "number": i.number,
            "short_description": i.description,
            "state": i.state,
            "priority": i.priority,
            "category": i.category,
            "opened_at": i.opened,
            "resolved_at": i.resolved,
            "u_policy_control": i.control,
            "assignment_group": groups[i.team][0],
            "assignment_group__display": groups[i.team][1],
            "cmdb_ci": sys_id("ci", i.ci_name or i.disk_name),
            "cmdb_ci__display": i.ci_name or i.disk_name,
        }
        for i in sorted(incidents, key=lambda i: i.number)
    ]
    write_tsv(
        HERE / "replica_rows" / "inventory" / "inventory.compute_resources.tsv",
        COMPUTE_COLUMNS,
        compute,
    )
    write_tsv(
        HERE / "replica_rows" / "inventory" / "inventory.storage_resources.tsv",
        STORAGE_COLUMNS,
        storage,
    )
    write_tsv(HERE / "replica_rows" / "itsm" / "itsm.incident.tsv", INCIDENT_COLUMNS, incident_rows)

    attached = [d for d in disks if d.vm is not None]
    lines = []
    for back in range(HISTORY_DAYS, 0, -1):
        day = REVIEW_DAY - timedelta(days=back)
        for d in sorted(attached, key=lambda d: d.name):
            status, number = status_on(day, d, incidents)
            values = (
                day.isoformat(),
                d.resource_id,
                d.name,
                d.cloud,
                d.account,
                d.team,
                status,
                number,
            )
            lines.append(
                "("
                + ", ".join(
                    "NULL" if v is None else "'" + v.replace("'", "''") + "'" for v in values
                )
                + ")"
            )
    header = (HERE / "db" / "init.sql").read_text().split("\n")[:8]
    sql = (
        "\n".join(header) + "\n--\n"
        "-- The fourteen days of the daily review before the review day, written by generate.py: each\n"
        "-- day as the daily trigger keeps it, classified by the daily_review view's rules on that day.\n"
        "INSERT INTO review_history (review_date, resource_id, resource_name, cloud_provider, account_id, application, status, incident_number) VALUES\n"
        + ",\n".join(lines)
        + ";\n"
    )
    (HERE / "db" / "history.sql").write_text(sql)
    today = [status_on(REVIEW_DAY, d, incidents)[0] for d in attached]
    print(
        f"{len(vms)} VMs, {len(disks)} disks ({len(attached)} attached), {len(incidents)} incidents"
    )
    print({s: today.count(s) for s in sorted(set(today))})


if __name__ == "__main__":
    main()
