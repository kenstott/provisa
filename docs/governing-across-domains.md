# Governing Across Domains

## The problem {: #problem }

Ask the same question through two parts of your business and you often get two answers. Net revenue in the board pack doesn't match net revenue in the finance dashboard, and neither matches the billing API.

## Why it happens {: #why }

The logic behind an answer lives in one team's jobs and another team's dashboards. What its terms mean is written down somewhere else, if at all, and each delivery channel adds rules of its own. Each piece was built for a reason. Nobody planned the whole, and nobody planned it across years of systems built and replaced and teams that changed. In the end people accept the mess as the cost of specialization and of serving many different stakeholders.

## The boundary {: #boundary }

Fixing it starts with scope. You have to decide what is governed, and for whom.

The boundary is the domain. Inside a domain are its systems of origin and record. The business unit runs them for its own stakeholders, and they are often vendor or internal applications. At its edge are its [data products](data-products.md) and data contracts, described by the [Open Data Product Standard](https://github.com/bitol-io/open-data-product-standard) (ODPS) and the [Open Data Contract Standard](https://github.com/bitol-io/open-data-contract-standard) (ODCS). They define how the domain shares data outside itself.

First-class governance is everything that crosses those edges. It is for the people who need one enterprise answer, from executives up to the board and out to regulators. It is also for the owners of reference data, and for the systems of record that read that reference data back.

## What domains use {: #model }

A domain creates governed data outputs from four things in the Provisa model. Business logic and business meaning describe what the outputs are. Security decides who may see them, and drivers bring the data in and process it.

Business logic is the typed generators and transformers of tables and values. Views, metrics, commands and code resolvers are all business logic. See [Business Logic in Provisa](business-logic.md).

Business meaning is the names and descriptions of domains, tables and columns, with the [glossary](glossary.md) and tags bound to them. The glossary is an ontology of concepts, their definitions and the relationships between them. See also [Data Modeling](modeling.md).

Security carries meaning of its own, usually derived from an organization's roles, processes and policies. In Provisa it is [roles](security.md#roles-granted-derived-and-meta), including roles derived from a user's attributes, with row rules, column masks and approvals. See [Security Model](security.md).

Drivers come in three kinds. [Sources](sources.md) are how data enters the circle. The [engine](engines.md) is the data processing service, such as Trino, DuckDB or Postgres, and each [region](regions.md) has its own. Processing patterns are what the service runs. Provisa supports [star schemas](modeling.md#kimball-star-schema-and-data-vault) built from entities and facts, data vault, and type 2 history kept as bitemporal views.

## One description of the whole {: #whole }

What's missing elsewhere is one description of the whole circle. In the model, every derivation between domain edges is named, so provenance is plain. You can trace a figure in a board report step by step in [lineage](lineage.md) to the system of record that captured it.

## One runtime {: #runtime }

Provisa runs the model the way an operating system runs a computer.

| Operating system | Provisa |
|---|---|
| Kernel | The model and the governed query pipeline |
| Drivers | Sources for ingress, engines for processing |
| System calls | Request languages and transports: SQL, GraphQL, Cypher, REST, JSON:API, gRPC, Arrow Flight, MCP |
| Applications | Data-quality checks, reports, notebooks and agents |

Serving is part of the runtime itself. You don't add a driver to serve data; every request language and transport reads the same model through the same governed pipeline.

Provisa doesn't prescribe how a domain works inside. A domain can change its own systems, and the answers that cross its edge are still defined in one place.
