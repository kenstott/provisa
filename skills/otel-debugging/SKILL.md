---
name: otel-debugging
description: How to assert on OpenTelemetry spans in Provisa tests — fixture setup, helper usage, and trace-missing diagnosis.
---

# OTel Debugging

## `otel_spans` Fixture (`tests/conftest.py:252`)

```python
@pytest.fixture
def otel_spans():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    old = trace.get_tracer_provider()
    trace.set_tracer_provider(provider)
    yield exporter
    exporter.shutdown()
    trace.set_tracer_provider(old)
```

- Sets up `InMemorySpanExporter` with a `SimpleSpanProcessor` (synchronous — no flush needed).
- Swaps the global tracer provider for the test's duration; restores it in teardown.
- Function-scoped. Does not inherit from or interact with `graphql_client` (session-scoped).
- The yielded value is the `InMemorySpanExporter` instance directly.

## Helper: `assert_span_emitted` (`tests/helpers.py:40`)

```python
def assert_span_emitted(exporter, name_fragment: str) -> None:
    spans = exporter.get_finished_spans()
    names = [s.name for s in spans]
    assert any(name_fragment in n for n in names), (
        f"No span matching {name_fragment!r} found. Emitted: {names}"
    )
```

- Substring match against `span.name`. Not a prefix or regex match.
- Failure message prints all emitted span names — read it before digging further.

## Usage Pattern

```python
def test_something(otel_spans):
    # trigger instrumented code path
    result = call_some_function()

    # assert a span was emitted
    assert_span_emitted(otel_spans, "compiler.compile_query")

    # inspect attributes
    spans = otel_spans.get_finished_spans()
    span = next(s for s in spans if "compiler.compile_query" in s.name)
    assert "SELECT" in span.attributes["db.statement"]
```

## Known Instrumented Code Paths

| Library / layer | Span name pattern | Key attributes |
|---|---|---|
| `sql_gen.compile_query` | `compiler.compile_query` | `graphql.field`, `db.statement` (first 1000 chars) |
| asyncpg (via OTel contrib) | `postgresql.query` or `db.query` | `db.statement` |
| FastAPI (via OTel contrib) | route path, e.g. `/graphql` | HTTP status, method |
| httpx (outbound) | `HTTP GET`, `HTTP POST` | `http.url` |

Span name patterns for asyncpg and httpx depend on the OTel contrib version in use. When unsure, print `[s.name for s in otel_spans.get_finished_spans()]` first.

## Diagnosing a Missing Span

Check in order:

1. **Is the code path calling the instrumented library?** A span is only emitted when the actual library call executes (e.g., asyncpg `execute`, not a mock).
2. **Is `otel_spans` in the test signature?** Without it the global provider is whatever was set at import time — likely a no-op provider.
3. **Is the span created in a different async task?** OTel context does not automatically propagate across `asyncio.create_task()` boundaries. The child task must explicitly receive and activate the parent context.
4. **Is `SimpleSpanProcessor` flushing synchronously?** It is — no explicit flush needed. If spans are still missing, the span was never started.

## Anti-Patterns

- **Never assert on span count.** Count varies with code path; only assert on name fragments or attributes.
- **Never use `otel_spans` with `graphql_client` in the same test.** `graphql_client` is session-scoped; `otel_spans` is function-scoped. The instrumented app was already created before the fixture swaps the provider.
- **Never patch `_tracer` directly.** The `otel_spans` fixture patches the global provider correctly; tracer-level patching is fragile.
