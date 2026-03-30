---
name: async-patterns
description: Asyncio conventions and patterns used in this project. Auto-triggers when working with async code.
---

# Async Patterns

## FastAPI Routes
- Async handlers in `provisa/api/`
- Use `async def` for route handlers that do I/O

## Sync/Async Boundary
- `asyncio.to_thread()` for blocking operations from async context
- Never block the event loop with sync I/O
- Use executor for CPU-bound work or sync DB calls

## Patterns
```python
# Parallel async execution
async def execute_queries(queries: list[Query]) -> list[Result]:
    tasks = [execute_one(q) for q in queries]
    return await asyncio.gather(*tasks)

# Sync → async bridge
result = await asyncio.to_thread(sync_function, arg)
```

## Anti-Patterns
- `time.sleep()` in async code (use `asyncio.sleep()`)
- Blocking DB calls without executor
- Creating new event loops inside async functions
- Fire-and-forget tasks without error handling
