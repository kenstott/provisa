# Business Logic in Provisa

## The problem {: #the-problem }

Every organization has standards for business logic, and most teams follow them. Yet over years, as systems are built and replaced and teams change, the logic spreads across ETL jobs, dashboards, APIs and stored procedures. Each piece makes sense on its own. Often nobody planned the whole, and more often nobody planned the whole across time. The result is familiar: "net revenue" is computed three ways in three places, and no one can say which is right.

## What business logic is {: #definition }

Business logic is the rules that create, update, delete or transform data.

Business terminology is separate. The names and descriptions of domains, tables and columns, and the terms in the [glossary](glossary.md), say what data means. Business logic says what is done to it.

## Three roles {: #roles }

Every rule plays one of three roles, and each role can be written in three ways:

| Role | SQL | External call | Code |
|---|---|---|---|
| **Dataset** | view, statement command | API or procedure that returns rows | code resolver `@table`, `@stream`; `python` or `script` command |
| **Value** | view expression, metric | — | `python` or `script` command |
| **Change** | `INSERT`, `UPDATE`, `DELETE`, statement command | API write, mutation, procedure | `@command`; `python` or `script` command |

- **External call.** Another system's GraphQL, REST or gRPC API, or a database's stored procedure, registered as a command.
- **Tables as arguments.** A command can take tables as arguments, so a dataset can be built from other datasets.
- **Metric.** A named aggregate, defined once, such as `net_revenue` as `SUM(orders.amount) - SUM(orders.refunds)`. Each query breaks it out by the dimensions it asks for, so the warehouse, the dashboard and the API all read the same number.

## Hidden side effects {: #side-effects }

A hidden side effect is one you can't see from the rule's definition.

Every rule is named, typed, registered, audited and secured. A command can also require approval before it runs.

| | Possible side effects | Limited by |
|---|---|---|
| **SQL** | None | — |
| **External call** | The other system's own workflows, notifications and cascades | Egress allow-list (HTTP, gRPC); credentials held as secrets (procedures, source operations) |
| **Code resolver** | Calls to outside services and networks; results that vary; cost | Pinned code, pinned host fingerprint, sandbox, metering |
| **Host code** | Reads anything the server can (memory, credentials, files, network); can starve the server or take it down | `commands.allow_host_code`, off by default |

An external call is typed by the system's own schema, or by the arguments and return columns you declare when you register the command.

A code resolver's code is pinned by package version, Git commit or archive hash, so it is promoted with the model.

Host code is a `python` or `script` command. It runs inside the server with the server's full access, so code that's malicious, buggy or greedy can read anything the server can, starve it or take it down. It is convenient, and safe when you trust the code. The model holds only a reference (`module:attr` or a command line), so the code doesn't travel with promotion. It runs only where `commands.allow_host_code` is on.

These limits contain side effects but can't remove them. What an external system or your own code does is ultimately up to it.

## What the model gives you {: #what-you-get }

- Every rule is registered in a domain, with grants.
- Every rule is governed the same way, whichever way it is written.
- Every rule is visible in [lineage](lineage.md).

## Read next {: #read-next }

- [Views](configuration.md#views-governed-computed-datasets)
- [Metrics](modeling.md#metrics)
- [Commands](commands.md)
- [Code Resolvers](code-sources.md)
- [Data-quality checks](sources.md#data-quality-checkers)
