# Access Model

Your rights in Provisa are a composition. **Scope** says where a column reaches. **Capabilities** say which features a role may use, and **grants** say which tables, columns and rows those features touch. Nothing else enters into it. No role bypasses the rules, and no domain is an exception.

Anyone can work out a role's rights from what is written down, whether they hold the role or audit it. Defaults and composition keep this from turning into a stream of manual grants.

## Scope

Every column has a scope, which says how far beyond its table's domain it reaches. There are three.

| Scope | Outside the domain | Inside the domain |
| --- | --- | --- |
| `domain` (the default) | The column does not exist. | Roles that reach the domain read and write it as their grants allow. |
| `listed` | Every role in the organisation sees the table's name and the column's name in the catalog. Nothing else is shown: no description, type, sample, value or count. | As for `domain`. |
| `public` | Every role in the organisation sees the names and descriptions, and roles of every domain are served the column as their grants allow. Masks, row rules and limits apply as they do inside the domain. | As for `domain`. |

Use `listed` to make a table discoverable. Someone in another domain can find its keys and ask its owners for a grant, and still reads nothing until a grant is given. A table appears outside its domain only through its listed or public columns.

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

Provisa maintains the standard roles, and you cannot edit them. You extend them with child roles of your own. When Provisa adds a capability to a standard role, every role built on it gets that capability.

### Inheritance

A role may have several parents. Its capabilities are the union of its own and those of every ancestor, with duplicates removed. Residency and grants are unions over the ancestors too. Least privilege is applied when you assign roles to people. Composing roles only adds.

Reach is the exception, because each role states its own. A role reaches ALL domains or a list of them, and its parents' reach does not pass to it. The standard roles reach ALL. A Finance steward, built on Data steward, states that it reaches the finance domain. To give a role reach to a domain you must reach that domain yourself.

Row rules resolve this way. A role's own rule for a table stands. Without one, the rules found among its ancestors are combined with OR, and a parent with no rule adds nothing, so adding a parent never lifts a row rule. A role with no rule anywhere in its ancestry is unrestricted. A property such as a rate limit comes from the role itself, or else from the first ancestor that sets it. Ancestors are searched depth-first, in the order the parents are declared.

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
| A role without one capability it would inherit | Compose the role from narrower parents. |
| A column for everyone except some people | Grant it to the roles that should read it instead of to Viewer. |
| A column whose name is visible but whose values are hidden | Add a mask. Unmasking is a grant. |
| Only some of a table's rows | Add a row rule. Adding a parent never lifts it. |

## What sits outside the rules

Two things stand outside the rules, and neither is a person. Registration is where data enters governance. While you register a source you see its raw schema, and once registration ends your grants apply like anyone else's. Provisa's service identity runs background work such as replica builds. It is not capped, and it never shows anything to a person.

## Seeing a role's effective rights

On the Security page, every role's detail panel shows the composition in two tabs. **Capabilities** draws the role's lineage from the standard roles down to it, with each node listing what it adds. **Data access** lists every table the role reaches once scope is applied. Each table expands to its columns, showing scope, read or write, mask, row rule and the role whose grant gives the access. A derived role carries no capabilities, so its panel has the Data access tab only.

A table's panel answers the same question from the other side. Choose a column and it lists every role that reads it, writes it, sees it masked or has a row rule on it. Each role comes with the chain behind its access: the granting role, any ancestor it inherits through, the column's scope and the role's reach. Only columns you can read can be chosen.

Both views are capped by your own read rights. They list only tables and columns you can read.

## Limits

A role can also carry limits. A rate limit caps how many requests per second the role may send. A row limit caps the rows any result returns, applied as a LIMIT on the statement. Session variables are named values that a statement reads with `current_setting('provisa.<name>')` when the request binds none of that name. Each is the role's own value, or else the first ancestor's that sets it, and each applies on every transport.

## Comparison with other products

Each entry cites that product's own documentation. An empty cell means the cited documentation does not address that question.

| Product | A newly created object | Seeing metadata without data access | Role composition | Who grants access | Deny |
| --- | --- | --- | --- | --- | --- |
| Provisa | The registering role and Viewer read every column. An empty grant list means no role. | `listed` scope shows table and column names to every role in the organisation. | Several parents; capabilities are a union. Standard roles are locked and extended by child roles. | Any holder, for columns it can read itself. | None. Grants and capabilities only add. |
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
