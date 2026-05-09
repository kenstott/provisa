---
name: invariant-extraction
description: Patterns for reverse-engineering implicit requirements from provisa source code — assertions, raises, schema constraints, config defaults. Auto-triggers when requirements-tracker is reverse-engineering from code.
---

# Invariant Extraction from Code

## What to Scan For

When reverse-engineering requirements from a module, search for these code patterns. Each is an implicit requirement.

### 1. Assertions → Preconditions
```bash
grep -n "assert " provisa/**/*.py
```
Each `assert` is a requirement: "this condition must hold at this point."

```python
assert len(tables) > 0   # REQ: at least one table must be registered
assert join_key is not None  # REQ: join key must be resolved before query emission
```

### 2. ValueError / TypeError raises → Validation rules
```bash
grep -n "raise ValueError\|raise TypeError\|raise RuntimeError" provisa/**/*.py
```

```python
raise ValueError(f"Unknown dialect: {dialect!r}")
# REQ: dialect must be one of the known values
```

### 3. Pydantic models → Data contracts
Fields in Pydantic models are data model requirements:
- `field: Type` (no default) → field is mandatory
- `field: Type = default` → what the system assumes when not provided
- `field: Optional[Type]` → field may be absent; callers must handle None

### 4. GraphQL schema definitions → Interface contracts
```bash
grep -n "def resolve_\|strawberry.field\|strawberry.type" provisa/**/*.py
```
Each resolver signature is a contract: its parameters, return type, and description define a requirement.

### 5. Default parameter values → Configuration assumptions
```python
def compile(self, dialect: str = "trino"):
# REQ: default compilation target is Trino
```

### 6. Regex patterns → Format constraints
```bash
grep -n "re.compile\|re.match\|re.search" provisa/**/*.py
```
Each compiled regex is a format requirement on its input.

### 7. Environment variable reads → Deployment requirements
```bash
grep -rn "os.environ\|os.getenv" provisa/ tests/
```

### 8. SQLGlot dialect guards → Transpilation requirements
```python
if dialect == "trino":
    ...
# REQ: SQL output must be dialect-specific; generic ANSI SQL is not acceptable
```

## How to Extract Requirements

For each pattern found:

1. **State the requirement** — what must be true, not what the code does
2. **Classify** — which category from `requirements.md`
3. **Check for duplicates** — does an equivalent requirement already exist?
4. **One bullet per constraint** — don't bundle multiple requirements

**Good (precise):**
> `table_name` is never `None` after `CompilationContext` is fully constructed

**Bad (describes code):**
> `CompilationContext.__init__` sets `table_name` on `TableMeta`

## Module Priority Order

Scan in this order for highest invariant density:
1. `provisa/compiler/sql_gen.py` — core compilation invariants
2. `provisa/api/data/endpoint.py` — API contract invariants
3. `provisa/compiler/stage2.py` — semantic SQL pipeline contracts
4. `provisa/otel_compat.py` — OTel span and tracing invariants
5. `tests/unit/` — tests assert invariants; read them as specs
