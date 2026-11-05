# Access Model

Your rights in Provisa are a composition. **Scope** says where a column reaches. **Capabilities** say which features a role may use, and **grants** say which tables, columns and rows those features touch. Nothing else enters into it. No role bypasses the rules, and no domain is an exception.

Anyone can work out a role's rights from what is written down, whether they hold the role or audit it. Defaults and composition keep this from turning into a stream of manual grants.

## Scope

Every column has a scope. A scope answers two questions about the column: which roles read its data, and which roles see that it exists. There are four scopes.

| Scope | Data: who reads it | Metadata: who sees the column exists |
| --- | --- | --- |
| `own-domain` | Roles granted the table's own domain directly. A role granted `"*"` counts. | The same roles. Everyone else sees no column. |
| `domain` (the default) | Roles that reach the table's domain, through a parent domain if need be. | The same roles. Everyone else sees no column. |
| `listed` | As for `domain`. | Every role in the organisation sees the table's name and the column's name. Nothing else is shown: no description, type, sample, value or count. |
| `public` | Any role the column's grant list names, in any domain. Masks, row rules and limits apply as they do inside the domain. | Every role in the organisation sees the names and descriptions. |

A column whose grant list (`visible_to`) is empty is read by no role, in any scope.

Read `listed` as `domain` plus public metadata. Read `public` as `listed` with the data opened to every domain.

Scope is decided first. The column's grant list (`visible_to`) then picks readers among the roles the scope allows. A role must pass both. A role reads a column only if it passes both the scope and the grant list; each is checked on its own.

The difference between `own-domain` and `domain` shows up only in a domain hierarchy. A role granted `legal` reaches every domain under `legal`, so a `domain` column in a child domain is open to it. An `own-domain` column is not. Reach inherited from a parent domain does not count, even when the grant list names the role. Only a role granted the child domain itself, or `"*"`, is served the column. The check runs when roles load, at each schema rebuild, so a change to a role's domain access takes effect at the next rebuild.

Viewer holds no meta grant: `catalog_read` is held by a role that names it. On SQL, a role with no meta grant still sees core metadata for the tables of non-system domains it can read, write or list, on the pgwire catalog and in meta. List means metadata only, and querying a table a role only lists is refused by name. The gRPC proto, JSON:API, GraphQL, Cypher, SPARQL and OpenAPI show what a role can read or write. Every catalog is scoped to a role's data domains, and shows a system domain only to a role holding it.

Use `listed` to make a table discoverable. Someone in another domain can find its keys and ask its owners for a grant, and still reads nothing until a grant is given. A table appears outside its domain only through its listed or public columns.

### Scope is a property of the column

Scope belongs to the resource. It sits on the column and says how far the data reaches, whoever asks. It is separate from roles, which carry capabilities and grants.

That separation keeps security rules short. One scope on a column covers every role, those that exist now and those created later. One rule on a domain does the same. You do not revisit the rule when a role is added.

### Not an OAuth scope

The word "scope" here does not mean an OAuth scope. An OAuth scope is attached to a token. It limits what a client may do on a user's behalf, so it sits on the grant side. A Provisa column scope is attached to the column and limits how far the data reaches, so it sits on the resource side.

The two do not interact. Provisa does not read OAuth scopes to decide what a role may read. A personal access token is narrowed by the role you issue it for. When you sign in to a source such as Microsoft 365, Provisa asks that provider for OAuth scopes. Those belong to the provider and are not touched by a column's scope.

## Domain hierarchy

A domain may sit under one parent domain, such as a business unit with the organizations inside it. Reach flows down and never up.

- **Roles.** A role granted a parent domain reaches every domain under it, at any depth. That includes domains added later, and the role is not edited. Granting a child domain gives nothing upward.
- **Sources.** A source allowed to the parent is allowed to its children.
- **Row filters.** A row filter on a parent domain applies to the tables of every domain under it. It is ANDed with the child's own filter and with every other ancestor's. A child only narrows.
- **Stewards.** A domain with no steward uses its nearest ancestor's.
- **Tables.** A table sits in exactly one domain, whether or not that domain has children, and you address it as `domain.table`. Nothing is namespaced per domain.

### Example: legal and executive_litigation

`executive_litigation` sits under `legal`. Two roles matter here. `legal_viewer` is granted `legal`. `exec_litigation_counsel` is granted `executive_litigation`.

You declare a new table, `executive_settlements`, in `executive_litigation`. Its sensitive columns, such as the settlement amount and the counterparty, get the `own-domain` scope.

- `exec_litigation_counsel` reads them, as its grants allow.
- `legal_viewer` is never served them. It reaches `executive_litigation` only through `legal`, and that does not count.
- A legal role created next year is never served them either. Nobody edits anything for that to hold.

Without `own-domain` you would build the same wall by hand. Every column is granted to Viewer at registration, and `legal_viewer` is built on Viewer, so it is named by default. You would clone the roles that need the data and rewrite each column's grant list to name only the clones. Then you would add a row filter on `executive_litigation` for every legal role, so that those roles read no rows of the tables it holds. The filter fails open. A legal role created later has no filter and reads every row. Each new role is a chance to forget one.

With `own-domain`, one setting on the column replaces all of that.

The domain row filter still has its job. Put a filter on `legal` and it narrows the rows of `executive_litigation` tables that legal roles already read. A column's scope decides whether a role is served the column at all. A row filter decides which rows of the columns it is served.

#### When a sibling fits better

A child domain suits a team whose data legal should mostly reach, with a few exceptions. Reach flows down and row filters carry down, and `own-domain` marks the exceptions.

If the exceptions would be most of the child's columns, make `executive_litigation` a sibling of `legal` instead. Legal roles then reach nothing in it unless you grant them the domain. You grant `public` or `listed` on the few columns you want to share. A sibling costs you the inherited reach and filters. If you find yourself marking nearly every column `own-domain`, that is the sign.

## Capabilities

A capability opens a feature, such as querying, registering sources or managing data quality checks. It never widens what you can read or write. Whatever a feature shows or changes stays inside your grants, and that includes names, counts, error text, lineage, profiling results and exports.

Roles carry capabilities. Provisa ships a set of standard roles, and you build your own roles on top of them.

| Standard role | Built on | Adds |
| --- | --- | --- |
| Viewer | | Reading the catalog, the glossary and data products |
| Analyst | Viewer | Querying and query development |
| Developer | Analyst | Views, environments and write |
| Modeler | Developer | Bypassing the relationship guard |
| Data product owner | Viewer | Editing the data products it owns |
| Data quality reader | Viewer | Reading check definitions and results |
| Data quality steward | Viewer | Managing checks, expectations and promises |
| Source admin | Viewer | Registering sources and tables |
| Security admin | Viewer | Access configuration, column grants and masking |
| Data steward | Security admin, Analyst | Glossary editing, relationships, data product editing and full results |
| Org operator | | Org settings, observability, and approving views and relationships |
| Org admin | All of the above | User and environment management |

Provisa maintains the standard roles, and you cannot edit them. You extend them with child roles of your own. Standard roles are the palette: when Provisa adds a capability to one, a role built on it holds that capability only once it is given it.

### Inheritance

A role may have several parents. It stores what it is given, and it holds that intersected with the union of what its parents hold, for capabilities, domains and data_residency's regions alike. A role with no parents holds what it is given. A parent's narrowing reaches every role below it. So a role is always a subset of the union of its parents. Saving a right no parent holds is refused by name, and a hand-written configuration is refused the same way when it loads. The role form lists what the parents hold, all selected, for you to unselect, and shows the rest disabled. Column grants are not narrowed: a child reads what its ancestors are granted.

A domain follows the same rule, and `"*"` stands for every data domain. The standard roles reach ALL. A Finance steward, built on Data steward, lists finance and reaches finance. To give a role reach to a domain you must reach that domain yourself.

A role reads no row its parents could not. Its rows are its own rule AND the rules of its parents combined with OR. A parent with no rule stands for every row, and a role with no rule of its own reads under its parents' rules. A property such as a rate limit comes from the role itself, or else from the first ancestor that sets it. Ancestors are searched depth-first, in the order the parents are declared.

A role with no capabilities can do nothing, reads included. Such a role has three uses: it carries a name or session values for row rules, it runs a command whose `visible_to` names it and does nothing else, or it is a building block holding only grants, which a role that includes it reads while taking its capabilities from its other parents.

## Grants

A grant gives a role read or write on a column. Read and write are separate grants.

When you register a table, every column is granted for read to the role you registered as and to the standard Viewer role. Both appear in each column's grant list, where you can see and change them. Registration never grants write. Write is always a grant you make explicitly.

An empty grant list means no role can read the column. This holds in every domain.

To make a column sensitive, remove Viewer from its grant list and keep the roles that should read it.

You can grant, mask or configure access only on columns you can read yourself. Rights flow by delegation. The registrant sets the first grants, and the holders of a column then pass on what they hold. This applies everywhere grants and masks are written, including the admin API and MCP tools.

Every column keeps a reader. You cannot remove your own read on a column; to keep its values from yourself, add a mask rule for your own role. The column's name stays visible, so you can still administer it. The last role holding read on a column cannot lose it either.

If no role can read a column, register the table again from its source. Registration shows the source's raw schema and sets new grants.

Seeds and demo configurations state their grants explicitly, as every column does.

## No deny

Capabilities and grants only add. Nothing subtracts from what another role or an ancestor grants, so there is no precedence rule to learn. Revoking takes back a grant and nothing more.

Each use of a deny has an additive form.

| You want | Do this |
| --- | --- |
| A role without one capability its parent holds | Leave that capability unselected on the role. |
| A column for everyone except some people | Grant it to the roles that should read it instead of to Viewer. |
| A column whose name is visible but whose values are hidden | Add a mask. Unmasking is a grant. |
| Only some of a table's rows | Add a row rule. A role reads no row its parents could not. |

## What sits outside the rules

Two things stand outside the rules, and neither is a person. Registration is where data enters governance. While you register a source you see its raw schema, and once registration ends your grants apply like anyone else's. Provisa's service identity runs background work such as replica builds. It is not capped, and it never shows anything to a person.

## Seeing a role's effective rights

On the Security page, every role's detail panel shows the composition in two tabs. **Capabilities** draws the role's lineage from the standard roles down to it, with each node listing what it adds. **Data access** lists every table the role reaches once scope is applied. Each table expands to its columns, showing scope, read or write, mask, row rule and the role whose grant gives the access. A derived role carries no capabilities, so its panel has the Data access tab only.

A table's panel answers the same question from the other side. Choose a column and it lists every role that reads it, writes it, sees it masked or has a row rule on it. Each role comes with the chain behind its access: the granting role, any ancestor it inherits through, the column's scope and the role's reach. Only columns you can read can be chosen.

Both views are capped by your own read rights. They list only tables and columns you can read.

## Limits

A role can also carry limits. A rate limit caps how many requests per second the role may send. A row limit caps the rows any result returns, applied as a LIMIT on the statement. Session variables are named values that a statement reads with `current_setting('provisa.<name>')` when the request binds none of that name. Each is the role's own value, or else the first ancestor's that sets it, and each applies on every transport.


## Three layers of administration

The boundary between the platform and an organization is not a recommendation: the platform admin holds no data capability. It decides which organizations exist and who their org admins are, and otherwise never acts inside an organization's data, members, or model. Below that boundary, the layers are carried by the standard roles' default capabilities — what is enforced is capabilities themselves, not role names. An organization may group them differently. A small org's admin may hold all of them. A larger org may define a dedicated role for source and table registration, or separate out load management. The layers below describe what the standard roles do.

**Platform admin** sets up the physical infrastructure. The work covers the regions the deployment runs in, the nodes in those regions, each organization's stores and engines within its granted regions, and mail providers.

**Org admin** defines how the organization's data is processed on that infrastructure.

*Sources and placement.* The org admin connects sources and sets each source's or table's home region.

*Load management.* Every source is read either live, replicated, or load-protected, and each is a trade-off between freshness and the load placed on the source.

- **Live read** — every query hits the source directly. Data is as fresh as the source; every request adds load.
- **Replicate** — the engine keeps a copy. Queries read the copy; only refreshes hit the source. Staleness equals the refresh interval, set by `cache_ttl` or a change signal. `replicate: 0` forces a copy always; `replicate: N` above 0 promotes a table once traffic passes N governed statements per interval (Hot-N). See [The `replicate` setting](configuration.md#the-replicate-setting).
- **Load-protected** — the source is never read live. Queries serve the last snapshot; the scheduler refreshes it only on its gates — `off_peak_window`, a `cache_ttl` cadence, or a change signal. At least one gate is required. Per-table `load_protected` overrides the source default.
- **Response cache** — `cache_ttl` on a source or table governs the response cache: repeat queries within the TTL are answered without touching any source.
- **Concurrency cap** — `max_live_concurrency` on a source caps live reads across all instances at once. Replica reads take no permit.
- **Per-role caps** — `rate_limit` limits requests per second from a role. `max_rows` injects a LIMIT on every result, applied even when the query names none.

See [Freshness and load: who decides](configuration.md#freshness-and-load-who-decides) for the full picture.

*Roles.* The org admin defines each role — its capabilities and parents — and assigns members to roles.

*Outbound syncs.* External catalog connections and metadata exports.

*Registries and AI models.* Package and container registries for the org's code sources; the org's natural-language query vendor and model.

**Data steward** decides how the org admin's data is used, what it means, and who may see it.

- **Use.** Registering tables, naming them, defining relationships, views, and data products.
- **Meaning.** Terms, ontology, descriptions, tags, and classification.
- **Access.** Column grants, row rules, masks, fakes for sensitive columns, sensitivity tags, and residency grants on roles.

The steward decides what a role may see. The org admin decides what roles exist and who holds them.

Every admin page belongs to one layer. Its settings are writable only by that layer's capabilities.

## Comparison with other products

Each entry cites that product's own documentation. An empty cell means the cited documentation does not address that question.

| Product | A newly created object | Seeing metadata without data access | Role composition | Who grants access | Deny |
| --- | --- | --- | --- | --- | --- |
| Provisa | The registering role and Viewer read every column. An empty grant list means no role. | `listed` scope shows table and column names to every role in the organisation. | Several parents; a role holds what it is given of what they hold. Standard roles are locked and extended by child roles. | Any holder, for columns it can read itself. | None. Grants and capabilities only add. |
| Snowflake | "Each securable object is owned by a single role, which by default is the role used to create the object." "Unless allowed by a grant, access is denied." ([access control overview](https://docs.snowflake.com/en/user-guide/security-access-control-overview)) | | Roles are granted to roles, and a role inherits the privileges of roles granted to it. One functional role can receive several access roles ([access control considerations](https://docs.snowflake.com/en/user-guide/security-access-control-considerations)). System-defined roles cannot be dropped ([overview](https://docs.snowflake.com/en/user-guide/security-access-control-overview)). | The object's owner. In a managed access schema, only the schema owner or a role with MANAGE GRANTS ([overview](https://docs.snowflake.com/en/user-guide/security-access-control-overview)). | REVOKE "removes one or more privileges on a securable object from a role or database role" ([REVOKE](https://docs.snowflake.com/en/sql-reference/sql/revoke-privilege)). |
| Databricks Unity Catalog | "The principal that creates an object becomes its initial owner" ([permissions concepts](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/permissions-concepts)). | BROWSE lets users "discover and view metadata about objects without granting access to the underlying data". For tables, views, volumes and functions it is granted at the catalog level only ([privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges)). | Privileges inherit down the object hierarchy, from catalog to schema to table ([permissions concepts](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/permissions-concepts)). | Owners, and holders of MANAGE ([privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges)). | GRANT and REVOKE add and remove privileges. ABAC policies can deny a privilege, and "Denials inherit downward the same way" ([permissions concepts](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/permissions-concepts)). |
| Microsoft Purview | Users see data assets where they hold at least Read permission in Data Map or on the Azure or Fabric resource ([roles and permissions](https://learn.microsoft.com/en-us/purview/data-governance-roles-permissions)). | Global Catalog Reader "can read published artifacts across governance domains that don't have a Local Catalog Reader specified" ([roles and permissions](https://learn.microsoft.com/en-us/purview/data-governance-roles-permissions)). | Built-in roles at the tenant, catalog and governance domain levels. Data Quality Steward and Data Quality Reader are sub-roles that require other roles to be held as well ([roles and permissions](https://learn.microsoft.com/en-us/purview/data-governance-roles-permissions)). | "Only a user with the Governance domain owner role can grant governance domain permissions" ([roles and permissions](https://learn.microsoft.com/en-us/purview/data-governance-roles-permissions)). | |
| Alation | New users get the Viewer role by default. Private data sources need explicit access ([roles overview](https://docs.alation.com/en/latest/welcome/CatalogBasics/RolesOverview.html)). | Viewers are read-only users who search and browse the catalog content they are permitted to see ([roles overview](https://docs.alation.com/en/latest/welcome/CatalogBasics/RolesOverview.html)). | Seven built-in roles, from Server Admin to Viewer ([roles overview](https://docs.alation.com/en/latest/welcome/CatalogBasics/RolesOverview.html)). | Data Source Admins manage a source's settings. Only Server Admin, Catalog Admin and Source Admin users can be assigned as Data Source Admins ([roles overview](https://docs.alation.com/en/latest/welcome/CatalogBasics/RolesOverview.html)). | |
| Collibra | | | Global roles "apply across all Collibra applications and modules". Resource roles "apply to specific data assets or resources". A user can hold roles directly or through group membership ([users, roles and permissions](https://productresources.collibra.com/docs/collibra/latest/Content/Settings/UsersAndGroups/co_user-roles-permissions.htm)). | Resource role membership is managed on each resource ([users, roles and permissions](https://productresources.collibra.com/docs/collibra/latest/Content/Settings/UsersAndGroups/co_user-roles-permissions.htm)). | |
| DataHub | | The Reader role "can read all metadata" ([roles](https://docs.datahub.com/docs/authorization/roles)). | Three built-in roles: Admin, Editor and Reader. Custom roles are not supported. Policies "will only add to what a user can do" ([roles](https://docs.datahub.com/docs/authorization/roles)). | Policies grant further privileges on top of a role ([roles](https://docs.datahub.com/docs/authorization/roles)). | |
| PostgreSQL | "For most kinds of objects, the initial state is that only the owner (or a superuser) can do anything with the object." No privileges go to PUBLIC by default on tables or table columns ([privileges](https://www.postgresql.org/docs/current/ddl-priv.html)). | | A role can be a member of several group roles. Members granted with INHERIT use the privileges of the roles they belong to ([role membership](https://www.postgresql.org/docs/current/role-membership.html)). | The owner. A privilege granted "with grant option" lets the recipient grant it to others ([privileges](https://www.postgresql.org/docs/current/ddl-priv.html)). | Privileges are added with GRANT and removed with REVOKE ([privileges](https://www.postgresql.org/docs/current/ddl-priv.html)). |
| AWS IAM | | | Identity-based and resource-based policies combine as a union ([policy evaluation logic](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic.html)). | | "An explicit deny in either of these policies overrides the allow" ([policy evaluation logic](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic.html)). |
| Windows access control lists | | | | | A DACL holds access-allowed and access-denied entries. In the preferred order, explicit deny entries come before explicit allow entries, and explicit entries come before inherited ones ([order of ACEs in a DACL](https://learn.microsoft.com/en-us/windows/win32/secauthz/order-of-aces-in-a-dacl)). |

### Per-role limits

| Product | What a limit attaches to | Request rate | Result size or cost | Session settings |
| --- | --- | --- | --- | --- |
| Provisa | A role, inherited from the first ancestor that sets it | A rate limit per role | A row limit per role, applied as a LIMIT | Session variables per role |
| PostgreSQL | A role | | | `ALTER ROLE ... SET` changes "a role's session default for a configuration variable", and the value takes effect "only at login" ([ALTER ROLE](https://www.postgresql.org/docs/current/sql-alterrole.html)). `CONNECTION LIMIT` "specifies how many concurrent connections the role can make" ([CREATE ROLE](https://www.postgresql.org/docs/current/sql-createrole.html)). |
| Snowflake | A warehouse or the account for resource monitors. The account, a user or a session for statement timeouts | | A resource monitor tracks warehouse credit usage, and "Resource monitors work for warehouses only" ([resource monitors](https://docs.snowflake.com/en/user-guide/resource-monitors)). STATEMENT_TIMEOUT_IN_SECONDS cancels a running statement after a set time, and "can be set for Account » User » Session" as well as on a warehouse ([parameters](https://docs.snowflake.com/en/sql-reference/parameters)). | |
| Hasura | A role, or globally: "API limits can have a global or per role configuration" ([API limits](https://hasura.io/docs/2.0/security/api-limits/)) | Rate limits per minute | Depth, node, time and batch limits | |
| AWS API Gateway | An API key, through a usage plan ([usage plans](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-api-usage-plans.html)) | A throttling limit and a quota limit per API key. AWS states these "are not hard limits, and are applied on a best-effort basis" ([usage plans](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-api-usage-plans.html)). | | |

Hasura's API limits are available on Cloud Professional, Cloud Enterprise and Self-Hosted Enterprise ([API limits](https://hasura.io/docs/2.0/security/api-limits/)).
