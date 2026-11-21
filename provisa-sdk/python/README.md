# provisa-sdk

Optional decorators and type aliases for Provisa Python code sources. Code that imports nothing
of Provisa's keeps working: the docstring tags stay the primary form.

Each decorator writes its docstring tag line into the function's docstring and returns the same
function. Provisa reads the decorated function exactly as it reads the docstring form, and the
SDK does nothing when the function runs.

```python
from provisa_sdk import Rows, table
from provisa_services import ReadContext


@table(loader=100)
def orders(inputs: OrderInputs, context: ReadContext) -> Rows[Order]:
    """Open orders."""
    ...
```

reads as

```python
def orders(inputs: OrderInputs, context: ReadContext) -> Iterable[Order]:
    """Open orders.

    @table
    @loader 100
    """
    ...
```

| Decorator | Docstring tag |
|---|---|
| `@table`, `@table(loader=True)`, `@table(loader=N)` | `@table`, plus `@loader` or `@loader N` |
| `@stream` | `@stream` |
| `@command`, `@command(insert="t")` (`update=`, `delete=`) | `@command`, plus `@insert t` |
| `@query` | `@query` |
| `@columns` | `@columns` |
| `@change(table=, key=[...], removed=, cursor=)` | one `@change part=value` line each |
| `@on_start`, `@on_stop`, `@health` | `@on-start`, `@on-stop`, `@health` |
| `@requires("NAME")`, `@requires("NAME", secret=True)` | `@requires NAME`, `@requires NAME:secret` |

A type checker holds each decorated function to its tag's signature: a `@table` returns a
collection and takes `(inputs, context)` with either left out, a loader takes a `ReadContext`,
a `@stream` is a generator, and a `@health` returns text.

Result aliases: `Rows`, `AsyncRows`, `Stream`, `AsyncStream`, `Batch`, `Results`. Column aliases:
`Text`, `BigInt`, `Double`, `Numeric`, `Timestamp`, `Date`, `Boolean`, `Json`, each the Python type
Provisa already maps.

The wheel also carries type stubs for `provisa_services` (`ReadContext`, `Filter`, `Order`,
`TableRef`, `TableConnection` and the services). At run time the host's own module is imported.
`gen_services_stubs.py` writes the stubs from that module.

Adding `provisa-sdk` as a dependency means the code source needs a lockfile.
