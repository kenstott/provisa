---
name: pytest-patterns
description: Test conventions and pytest patterns for this project. Auto-triggers when writing or reviewing tests.
---

# Pytest Conventions

## Structure
- Tests in `tests/`, files: `test_*.py`, functions: `test_*`
- Fixtures in `conftest.py`
- Run: `python -m pytest tests/ -x -q`

## Fixtures
- Session-scoped for expensive setup (DB connections, Trino client)
- Function-scoped for state that needs cleanup
- Generator fixtures with `yield` for teardown:
```python
@pytest.fixture
def db_conn():
    conn = create_connection()
    yield conn
    conn.close()
```

## Markers
- `@pytest.mark.slow` — long-running tests
- `@pytest.mark.skipif` — conditional skip
- `@pytest.mark.integration` — requires external services

## Patterns
- One assertion per test where practical
- Test behavior, not implementation
- Mock external dependencies (Trino, RDBMS connections)
- Use `tmp_path` fixture for temp files
- Parametrize for boundary/equivalence testing:
```python
@pytest.mark.parametrize("input,expected", [(0, True), (-1, False)])
def test_validate(input, expected):
    assert validate(input) == expected
```

## Rules
- Never remove tests to make the suite pass
- No fallback values or silent error handling in test helpers
