---
name: debug-python
description: Python debugging conventions and diagnostic tools for this project. Auto-triggers when investigating errors or failures.
---

# Debugging Conventions

## Logging
- Module-scoped: `logger = logging.getLogger(__name__)`
- No `print()` — always `logger.debug()` / `logger.info()`

## Standard Tools
- `pdb` / `breakpoint()` for interactive debugging
- IPython `embed()` for exploration
- `git bisect` for regression hunting

## Trino Diagnostics
- Query plans: `EXPLAIN ANALYZE`
- Connector issues: check catalog registration, INFORMATION_SCHEMA
- Cross-source joins: verify predicate pushdown via plan

## SQLGlot Diagnostics
- Compare input PG-style SQL with transpiled output
- Check dialect-specific function mappings
- Verify type casting across dialects

## FastAPI Diagnostics
- Request validation: check Pydantic model errors
- Middleware chain: verify auth/security middleware order
- Async issues: check for blocking calls in async handlers

## Common Issues
- SQLGlot transpilation edge cases between PG and Trino SQL
- Trino connector configuration for new source types
- Connection pool exhaustion under load
- RLS injection producing invalid SQL for specific dialects

## Investigation Process
1. Reproduce reliably
2. Isolate minimal failing case
3. Observe (stack traces, logs, state)
4. Hypothesize (rank: recent changes > config > data > env > library bug)
5. Verify hypothesis before fixing
