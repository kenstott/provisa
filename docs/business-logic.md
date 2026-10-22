# Business Logic in Provisa

## The problem {: #the-problem }

Business logic ends up everywhere: in ETL jobs, dashboards and APIs, owned by different teams and catalogued by no one. "Net revenue" is computed one way in the warehouse job, another in the finance dashboard and a third in the billing API, and nobody can say which is right.

That's nobody's fault. There has been no shared, simple model to put the rules in.

## What business logic is {: #definition }

Business logic is the rules that create, update, delete or transform data.

Business terminology is separate. The names and descriptions of domains, tables and columns, and the terms in the [glossary](glossary.md), say what data means. Business logic says what is done to it.

## Three roles {: #roles }

Every rule plays one of three roles, and each role can be written in three ways:

| Role | SQL | External call | Code resolver |
|---|---|---|---|
| **Dataset** | view, statement command | API or procedure that returns rows | `@table`, `@stream` |
| **Value** | view expression, metric | — | — |
| **Change** | `INSERT`, `UPDATE`, `DELETE`, statement command | API write, mutation, procedure | `@command` |

- **External call.** Another system's GraphQL, REST or gRPC API, or a database's stored procedure, registered as a command.
- **Metric.** A named aggregate, defined once, such as `net_revenue` as `SUM(orders.amount) - SUM(orders.refunds)`. Each query breaks it out by the dimensions it asks for, so the warehouse, the dashboard and the API all read the same number.

## Hidden side effects {: #side-effects }

A hidden side effect is one you can't see from the rule's definition.

Every rule is named, typed, registered, audited and secured.

| | Possible side effects | Limited by |
|---|---|---|
| **SQL** | None | — |
| **External call** | The other system's own workflows, notifications and cascades | Approval |
| **Code resolver** | Calls to outside services and networks; results that vary; cost | Sandbox, metering |

An external call is typed by the system's own schema, or by the arguments and return columns you declare when you register the command.

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
