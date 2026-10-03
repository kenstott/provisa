# Externe Schemas

Eine Quelle für ein externes Schema (Remote Schema) verbindet eine externe API — GraphQL (einschließlich GitHub), gRPC oder REST (OpenAPI) — mit der semantischen Schicht von Provisa. Das Hinzufügen einer Quelle registriert keine Tabelle. Die Quelle bietet Tabellen an, und ein Data Steward registriert jede gewünschte Tabelle über die Auswahl „Tabelle registrieren“ (Register Table); diese Registrierung ist der Kurationsschritt. (REQ-308, REQ-316, REQ-322) Eine registrierte Tabelle ist eine vollwertige Provisa-Tabelle. (REQ-308, REQ-316, REQ-325) Jede Governance-Regel, jede Abfrageschnittstelle und jede Sicherheitsschicht gilt automatisch. (REQ-310, REQ-319, REQ-328) Der externe Dienst sieht die Governance-Regeln von Provisa niemals. (REQ-310, REQ-319, REQ-328)

---

## Drei Quellentypen

### GraphQL Remote Schema (REQ-307–313)

**Quelle hinzufügen.** POST an `/admin/sources/graphql-remote` mit der Endpunkt-URL, einem Namespace und optionaler Authentifizierung. Provisa löst eine Standard-`__schema`-Introspektionsabfrage gegen den externen Endpunkt aus, um Endpunkt und Zugangsdaten zu bestätigen. (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

Das Hinzufügen der Quelle registriert weder eine Tabelle noch einen Command. Jeder Quellentyp für externe Schemas beantwortet Hinzufügen und Aktualisieren mit denselben Zählern: `tables` (registrierte oder aktualisierte Tabellen; beim Hinzufügen 0), `available_tables` (angebotene Tabellen), `mutations` (immer 0) und `available_mutations` (angebotene Commands). [tool-verified: `provisa/api/admin/schema_common.py` `remote_source_counts`; `provisa/api/admin/graphql_remote_router.py` `register_graphql_remote_source`]

**Tabellen registrieren.** Öffnen Sie „Tables“, dann „Register Table“, wählen Sie die Quelle und das Schema `graphql` und wählen Sie die gewünschten Tabellen und Spalten. Über die Admin-GraphQL-API: `availableTables(sourceId, schemaName)` listet die angebotenen Tabellen auf, `availableColumns` listet die Spalten einer Tabelle auf, und `registerTable(input: TableInput)` registriert eine Tabelle mit den gewählten Spalten. Eine registrierte Tabelle unterliegt anschließend der Governance. (REQ-308) [tool-verified: `provisa/api/admin/introspect.py` `_native_tables_graphql` (`if schema_name != "graphql": return []`), `provisa/api/admin/_graphql_table_registration.py` `offered_tables`, `offered_columns`, `columns_to_register`] [tool-verified: `provisa/api/admin/schema_query.py` `available_tables`, `available_columns`; `registerTable` is from the task brief, not read]

Wie eine registrierte Tabelle gelesen wird (Wurzelfeld, Zeilenpfad, erforderliche Argumente, Seitenargumente), wird in `sources.mapping["tables"]` gespeichert, sodass ein neu gestarteter Prozess sie liest, ohne das externe Schema abzufragen. [tool-verified: `_graphql_table_registration.py` `TABLE_SPECS_KEY = "tables"`, `remember_table`]

```json
{
  "source_id": "petstore-gql",
  "url": "https://api.example.com/graphql",
  "namespace": "petstore",
  "domain_id": "veterinary",
  "auth": { "type": "bearer", "token": "..." },
  "cache_ttl": 300,
  "field_overrides": { "createPet": "query" },
  "relationships": [
    { "source_table": "petstore__pets", "source_column": "owner_id",
      "target_table": "owners__users", "target_column": "id" }
  ]
}
```

Authentifizierungsoptionen: `none`, `bearer` (Authorization-Header), `basic` (Base64-codiert, Benutzername:Passwort). (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:36–45`]

**Feld-Overrides.** `field_overrides` ist eine `{fieldName: "query" | "mutation"}`-Zuordnung, die nach der Introspektion angewendet wird. Sie hat Vorrang vor der strukturellen Klassifizierung. Nur Felder vom Typ Query können als Mutation neu klassifiziert werden; Felder vom Typ Mutation haben in GraphQL keinen Override-Pfad. (REQ-531) [tool-verified: `provisa/graphql_remote/mapper.py`]

**Beziehungen zum Registrierungszeitpunkt.** `relationships` deklariert FK/PK-Verknüpfungspfade zwischen Tabellen zum Registrierungszeitpunkt. Diese werden als manuell deklarierte Beziehungen gespeichert (ohne `remote_managed`-Flag). Bei einer Aktualisierung werden automatisch erkannte Beziehungen (solche mit `remote_managed: True`) erneut ausgeführt und können sich ändern; manuell deklarierte Beziehungen bleiben unverändert. (REQ-554) [tool-verified: `provisa/api/admin/graphql_remote_router.py`]

**Was die Quelle anbietet.** Jedes Feld des externen Typs `Query`, das ein Objekt oder eine Liste von Objekten zurückgibt, wird als Tabelle angeboten, ebenso jede Relay-Connection unter einem Einzelobjekt-Feld (siehe unten). Das Registrieren einer angebotenen Tabelle macht sie zu einer Tabelle. Jedes Feld des externen Typs `Mutation` ist ein angebotener Command und wird in `available_mutations` gezählt; das Hinzufügen der Quelle registriert keinen. Registrieren Sie die gewünschten als Commands; siehe [Schreiboperation einer externen Quelle](commands.md#schreiboperation-einer-externen-quelle-req-1924). (REQ-308, REQ-1924) [tool-verified: `provisa/graphql_remote/mapper.py:243–278`, `graphql_remote_router.py` `register_graphql_remote_source` (`"functions": 0`)]

**Benennung von Tabellen.** Tabellen werden `{namespace}__{field_name}` benannt. Mit dem Namespace `petstore` und einem Query-Feld `pets`: Der Tabellenname lautet `petstore__pets`. (REQ-312) [tool-verified: `provisa/graphql_remote/mapper.py:250`]

**Relay-Connections.** Viele APIs geben Listen als Relay-Connections zurück: ein Objekt mit `nodes` (oder `edges { node }`) neben `pageInfo`. Provisa bildet eine Connection auf eine Tabelle ihrer Knoten ab und liest sie seitenweise. (REQ-308, REQ-309) [tool-verified: `provisa/graphql_remote/mapper.py` `_is_connection`, `_map_connection_table`]

- Ein Wurzelfeld, das eine Connection zurückgibt (`securityAdvisories`), wird zu einer Tabelle seiner Knoten.
- Eine Connection auf dem einzelnen Objekt, das ein Wurzelfeld zurückgibt, wird zu einer eigenen Tabelle. Die Tabelle übernimmt die erforderlichen Argumente des Wurzelfelds. Bei `repository(owner, name)` und einer Connection `issues` auf `Repository` heißt die Tabelle `repositoryIssues`, der SQL-Name lautet `gh__repository_issues` unter dem Namespace `gh`. Filtern Sie sie über die Spalten `_nf_owner` und `_nf_name`: `WHERE _nf_owner = 'acme' AND _nf_name = 'widgets'`.
- Eine Connection ist nie eine Spalte. Eine Zeile würde sonst für jede Connection ihres Typs einen Lesevorgang tragen, den der externe Dienst pro Zeile berechnet.
- Eine Connection ist nur dann eine Tabelle, wenn ihr Feld `first` und `after` annimmt, sodass sie seitenweise gelesen werden kann. Eine Connection, die ein eigenes Argument benötigt, ist keine Tabelle. Das gilt auch für eine Connection einer Union und für jede Connection unter einem Wurzelfeld, das eine Liste zurückgibt.

[tool-verified: `provisa/graphql_remote/mapper.py` `_map_connection_table`, `_map_child_connection_tables`; `tests/unit/test_graphql_remote_relay.py` `test_child_connection_table_takes_the_root_fields_arguments`]

**Typzuordnung (REQ-308).** Skalare Felder werden direkt auf Provisa-Typen abgebildet. OBJECT-Felder unterteilen sich in zwei Fälle, abhängig davon, ob der Zieltyp governance-pflichtig ist (siehe „Governance-pflichtige Tabellen“ unten). [tool-verified: `provisa/graphql_remote/mapper.py:14–36`, `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]

| GraphQL-Typ | Provisa-Typ |
| --- | --- |
| `String` | `text` |
| `ID` | `text` |
| `Int` | `integer` |
| `Float` | `numeric` |
| `Boolean` | `boolean` |
| OBJECT (nicht governance-pflichtiger Inline-Typ, z. B. `ContactInfo`) | `jsonb`-Blob-Spalte |
| OBJECT (governance-pflichtiger Zieltyp) | vollständig von SDL und Abruf ausgeschlossen |
| Jedes ENUM | `jsonb` |
| Benutzerdefiniertes Skalar | `text` (Fallback) |

**Governance-pflichtige Tabellen.** Ein GQL-Typ ist governance-pflichtig, wenn er im externen Schema als Wurzelfeld von `Query` auftritt. `_collect_queryable_types` erfasst diese während der Registrierung und bevorzugt dabei Felder ohne erforderliche Argumente, damit sie als Join-Ziele im großen Umfang abgerufen werden können. [tool-verified: `provisa/graphql_remote/mapper.py:395–413`]

Wenn eine OBJECT-typisierte Spalte einer governance-pflichtigen Tabelle auf einen anderen governance-pflichtigen Typ verweist, unterliegt diese Spalte gleichzeitig drei Regeln [tool-verified: `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]:

1. **Vom GQL-Abruf ausgeschlossen** — das Feld wird beim Abrufen der Zeilen der übergeordneten Tabelle nicht angefragt.
2. **Von der SDL ausgeschlossen** — das Feld erscheint nicht am übergeordneten Typ im generierten Schema.
3. **Nur über eine deklarierte Beziehung zugänglich** — ein Data Steward muss einen JOIN zwischen den beiden materialisierten, governance-pflichtigen Tabellen registrieren. Ohne diesen fehlt das Feld schlicht; es gibt keinen Blob-Fallback.

OBJECT-Typen, die NICHT als Wurzel-Query-Felder erreichbar sind (Inline-Typen wie `ContactInfo` oder `Address`), folgen anderen Regeln: Sie werden als `jsonb`-Blob-Spalten abgerufen und erscheinen in der SDL als verschachtelte Objektfelder. Unterfelder sind über `-->>`-Extraktion in SQL zugänglich.

**Felder mit Argument sind keine Spalten.** Ein Feld mit einem erforderlichen Argument kann nicht ohne Weiteres ausgewählt werden und wird daher aus den Spalten der Tabelle und aus verschachtelten Auswahlen ausgelassen. [tool-verified: `provisa/graphql_remote/mapper.py` `_build_columns`, `_build_gql_field_selection`]

**Erforderliche Argumente.** Wenn ein Wurzel-Query-Feld Non-Null-Argumente ohne Standardwert besitzt, werden diese zu Spalten mit `native_filter_type: query_param` auf der Tabelle (mit dem Präfix `_nf_` zum Zeitpunkt der Injektion). Der Executor übergibt sie als GraphQL-Variablen. (REQ-555) [tool-verified: `provisa/graphql_remote/mapper.py:110–120`, `provisa/api/app.py:1280–1303`]

**Automatisch erkannte Beziehungen.** Provisa durchsucht die OBJECT-typisierten Spalten jeder registrierten Tabelle. Wenn der referenzierte GQL-Typ ebenfalls als Tabelle in derselben Quelle registriert ist und die Spalte, auf der die Beziehung beruht, zu den registrierten Spalten gehört, wird die Beziehung gespeichert. Eine noch nicht registrierte Tabelle erhält keine. [tool-verified: `_graphql_table_registration.py` `sync_detected_relationships`] n:1-Beziehungen leiten Quell- und Zielspalten aus Namenskonventionen ab (`breedName` am Quelltyp → `name` am Zieltyp `Breed`). 1:n-Felder (LIST) erzeugen Beziehungen mit leeren Spaltenreferenzen — der Fremdschlüssel befindet sich auf der Zielseite. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutationen.** Ein Mutation-Feld wird einzeln als Command der Art `source_operation` registriert. Seine Argumente sind jeweils als `json` typisiert und werden als typisierte Variablen an den externen Dienst übergeben; die Antwort ist das JSON, das der externe Dienst zurückgibt, ohne `return_schema`. Siehe [Schreiboperation einer externen Quelle](commands.md#schreiboperation-einer-externen-quelle-req-1924). (REQ-1924) [tool-verified: `provisa/api/admin/actions_router.py` `_as_source_operation` (`body.returns = ""`, arguments typed `json`); `provisa/executor/source_operation.py` `_call_graphql`]

**Aktualisierung.** POST an `/admin/sources/graphql-remote/{id}/refresh`. Führt eine erneute Introspektion des externen Schemas durch und bringt die bereits registrierten Tabellen auf den Stand dieses Schemas. Es fügt weder eine Tabelle noch eine Spalte hinzu: Eine Tabelle oder Spalte, die das Schema hinzugewonnen hat, bleibt im Angebot, und eine Spalte, die das Schema verloren hat, wird entfernt. Bestehende Governance-Regeln (RLS, Maskierung) bleiben erhalten. (REQ-311) [tool-verified: `provisa/api/admin/graphql_remote_router.py` `refresh_graphql_remote_source`; `_graphql_table_registration.py` `refreshed_registered_tables`: "a column the schema has lost is gone; one it has gained is on offer and is not added"]

**Einschränkungen.**

- Skalare und ENUM-Wurzel-Query-Felder (Rückgabetyp ist nicht OBJECT) werden zu nachverfolgten Funktionen, nicht zu virtuellen Tabellen. Ihr `return_schema` besteht aus einer einzelnen Spalte `value` des zugeordneten Skalartyps. [tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- Objektverschachtelung wird zum Registrierungszeitpunkt bis zu `graphql_remote.max_object_depth` (Standard: 5) aufgelöst. Sowohl die Auswahl beim externen Abruf als auch die Metadaten der Unterfelder werden bis zu dieser Tiefe erstellt; Felder jenseits des Limits werden nicht abgerufen und stehen für die SQL-Extraktion nicht zur Verfügung. Ein Typ wird entlang eines Pfades nur einmal betreten: Ein Feld, dessen Typ bereits auf dem Weg nach unten liegt, wird ausgelassen, sodass ein Schema, dessen Typen aufeinander verweisen, einmal pro Typ durchlaufen wird und nicht einmal pro Tiefenebene. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py` `_build_gql_field_selection`, `tests/unit/test_graphql_remote_relay.py` `test_a_type_is_entered_once_along_a_path`]
- LIST-typisierte verschachtelte OBJECT-Felder (z. B. `breed.awards: [Award]`) werden bis zu `graphql_remote.max_list_depth` Verschachtelungsebenen (Standard: 2) in die Abrufauswahl einbezogen. Innerhalb dieses Limits wird die Liste als `jsonb`-Array in der übergeordneten Spalte abgerufen. Deklariert das Listenfeld ein Argument `first` (Relay, PostGraphile, pg_graphql) oder ein Argument `limit` (Hasura), übergibt die Auswahl es als `first: N` bzw. `limit: N`, wobei N `graphql_remote.max_list_items` (Standard: 100) entspricht. Ein Listenfeld, das keines von beiden deklariert, erhält kein Argument, weil ein externer Dienst ein Argument ablehnt, das das Feld nicht deklariert. Jenseits von `max_list_depth` wird das LIST-Feld vollständig ausgeschlossen, um eine unbegrenzte Datenexpansion zu verhindern. In SQL wird auf das Array über `json_array_elements(column_name)` oder eine Index-Extraktion mit `->>` zugegriffen. Wenn der Elementtyp der Liste über eine eigene Wurzelabfrage verfügt, sollte er stattdessen als separate Tabelle registriert und eine Beziehung erstellt werden — der Join-Pfad ist effizienter und umgeht den Blob. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py` `_list_limit_arg`, `_build_gql_field_selection`; `tests/unit/test_graphql_remote_relay.py` `test_a_plain_list_takes_no_first_and_a_list_that_declares_first_gets_it`]
- Bei SQL-Abfragen werden nicht governance-pflichtige OBJECT-typisierte Spalten vollständig von der externen Quelle abgerufen (alle Unterfelder bis zur konfigurierten Tiefe) und als `jsonb` zwischengespeichert. Der Zugriff auf Unterfelder in SQL erfolgt über `->>`-Extraktion gegen den Blob; die externe Anfrage wird nicht auf die von der SQL-Abfrage ausgewählten Felder eingeschränkt. Wenn der Elementtyp der Liste keine Wurzelabfrage besitzt und die Blob-Darstellung nicht ausreicht, sollte die Abfrage direkt in GraphQL-SDL geschrieben werden — Provisa gibt die GQL-Feldauswahl originalgetreu wieder, sodass die externe Quelle genau die angeforderten Felder sieht. [tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- Falls der externe Server ein OBJECT-typisiertes Feld ablehnt, weil eine Unterfeldauswahl erforderlich ist (was nicht auftreten sollte, wenn `gql_selection` verfügbar ist), unternimmt der Executor einen erneuten Versuch ohne diese Felder, damit skalare Spalten dennoch zurückgegeben werden. Das gilt für Tabellen, die von einem Wurzelfeld gelesen werden. Eine Connection-Tabelle nimmt diesen Weg nicht. [tool-verified: `provisa/graphql_remote/executor.py` `execute_remote` (`for attempt in range(2)`), `_execute_connection`]

**Seitenweises Lesen.** Eine Connection-Tabelle wird per Cursor gelesen. Jede Seite fragt `first: N, after: $pageCursor` mit `pageInfo { hasNextPage endCursor }` an, und der Lesevorgang folgt `endCursor`, bis der externe Dienst keine weitere Seite meldet. (REQ-309) [tool-verified: `provisa/graphql_remote/executor.py` `_connection_query`, `_execute_connection`]

| Einstellung | Standard | Wirkung |
| --- | --- | --- |
| `graphql_remote.max_list_items` | `100` | Zeilen pro Seite. [tool-verified: `provisa/api/data/materialization.py` passes `limit=max_items` to `execute_remote`] |
| `graphql_remote.max_rows` | `10000` | Die meisten Zeilen, die ein Lesevorgang einer Connection-Tabelle übernimmt. Ein Lesevorgang, der diesen Wert erreicht, stoppt und protokolliert eine Warnung. [tool-verified: `provisa/core/models.py` `GraphQLRemoteConfig`] |

```yaml
graphql_remote:
  max_list_items: 100
  max_rows: 10000
```

Zwei Antworten veranlassen den Executor zu einem erneuten Versuch:

- **Seite zu schwer.** Antwortet der externe Dienst mit 502 oder 504, wird dieselbe Seite in halber Größe erneut angefragt, bis hinunter auf eine Zeile. [tool-verified: `_PAGE_TOO_HEAVY = (502, 504)`, `page_size = max(1, page_size // 2)`]
- **Ratenbegrenzung mit Wartezeit.** Antwortet der externe Dienst mit 403 oder 429 und einem `Retry-After` von höchstens 120 Sekunden, wartet der Executor so lange und sendet die Anfrage erneut, mit bis zu drei Versuchen. Eine Ablehnung ohne `Retry-After` oder mit einer längeren Wartezeit wird als Fehler ausgelöst. Das gilt für jeden Lesevorgang, ob Connection oder nicht. [tool-verified: `_post`, `_RETRY_AFTER_STATUSES`, `_RETRY_AFTER_ATTEMPTS`, `_RETRY_AFTER_MAX_SECONDS`]

Jeder andere Fehler in der Antwort lässt den Lesevorgang scheitern, sofern der Quellentyp nichts anderes festlegt (siehe GitHub unten). Eine Connection, deren übergeordnetes Objekt null zurückgab, hat keine Zeilen. [tool-verified: `_accept_row_field_errors`, `_execute_connection`]

---

### GitHub (REQ-1923)

GitHub ist ein gewöhnlicher Quellentyp. Seine API ist GraphQL, daher verhalten sich seine Tabellen wie oben beschrieben, einschließlich Connection-Tabellen wie `gh__repository_issues`. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["github"]`]

**Quelle hinzufügen.**

1. Öffnen Sie „Sources“ und fügen Sie eine Quelle vom Typ **GitHub** hinzu.
2. Geben Sie ein GitHub-Zugriffstoken ein. Optional geben Sie einen Namespace ein, das Präfix der Tabellennamen; der Standard ist `gh`.
3. Speichern. Provisa prüft das Token gegen GitHub. Ein Token, das GitHub ablehnt, lässt das Hinzufügen mit der Meldung von GitHub scheitern.

Das Hinzufügen der Quelle registriert keine Tabellen. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_register_branded_source` (`"tables": 0`, `verify_query="query { viewer { login } }"`)]

**Tabellen registrieren.** Öffnen Sie „Tables“, dann „Register Table“. Wählen Sie die GitHub-Quelle, wählen Sie das Schema `graphql` und wählen Sie dann die gewünschten Tabellen. Jede Tabelle, die GitHub anbietet, wird aufgelistet; die Registrierung ist Ihre Entscheidung, welche Tabellen Sie freigeben. [tool-verified: `provisa/api/admin/_graphql_table_registration.py` `offered_tables`] [inferred: picker labels and the `graphql` schema name from the task brief; the UI strings were not read]

**Token-Berechtigungen (Scopes).** Wenn Sie eine Tabelle registrieren, prüft Provisa sie einmal mit Ihrem Token gegen GitHub.

- Ein Feld, das die Scopes des Tokens nicht abdecken, wird aus der Tabelle ausgelassen. Das Ergebnis nennt jedes ausgelassene Feld: `Left out, because the source's credential may not read them: projectsV2`. [tool-verified: `provisa/api/admin/schema_mutation_ops.py`]
- Eine Tabelle, die das Token überhaupt nicht lesen kann, wird mit der Begründung von GitHub abgelehnt: `GitHub does not let this source's credential read gh__repository_issues: ...`. [tool-verified: `provisa/api/admin/_table_ops.py` `_branded_columns_for_input`]

**Zeilen, die das Token nicht sehen darf.** GitHub antwortet mit `FORBIDDEN` für ein Feld, das das Token in einer bestimmten Zeile nicht sehen darf, etwa die Mitwirkenden (Collaborators) eines Repositorys ohne Push-Zugriff, und mit `NOT_ORG_OWNED_REPO` für ein Feld, das nur bei Repositorys im Besitz einer Organisation existiert. Dieses Feld ist in dieser Zeile null, der übrige Lesevorgang bleibt bestehen, und Provisa protokolliert eine Warnung. Ein Fehler gegen die Tabelle selbst lässt den Lesevorgang scheitern. [tool-verified: `provisa/graphql_remote/brands.py` `error_policy`, `provisa/graphql_remote/executor.py` `_accept_row_field_errors`]

**Schwere Seiten.** Antwortet GitHub mit `RESOURCE_LIMITS_EXCEEDED`, weil die Berechnung einer Seite zu aufwendig ist, wird die Seite in halber Größe erneut angefragt. [tool-verified: `brands.py` `overload`, `executor.py` `_execute_connection`]

**Verschachtelte Objekte.** GitHub-Tabellen verwenden ihre eigene Verschachtelungstiefe von 0 (`max_object_depth=0` für diesen Quellentyp), nicht `graphql_remote.max_object_depth`. Eine Spalte mit verschachteltem Objekt wird nur mit ihren eigenen skalaren Feldern ausgewählt; Objekte darin erscheinen als `__typename`. [tool-verified: `brands.py`]

**Token-Speicherung.** Das Token wird im Secrets-Vault abgelegt, und die Quellzeile behält eine Referenz, sodass ein Neustart die Quelle erneut liest, ohne dass das Token neu eingegeben werden muss. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_persist_source` docstring: "The credential goes to the org's vault and the row carries the reference"]

**Funktionsweise (für Betreiber).** Das Schema von GitHub wird mit Provisa ausgeliefert, daher löst das Hinzufügen der Quelle keinen Introspektionsaufruf aus, und ein großes Schema kostet bei der Registrierung nichts. Tabellen werden daraus einzeln abgebildet, sobald Sie sie registrieren. Der Refresh-Endpunkt lehnt diesen Quellentyp ab; ein neues GitHub-Schema kommt mit einem Provisa-Release. [tool-verified: `brands.py` module docstring, `brand_schema`; REQ-1923 "there is no refresh" in `docs/arch/requirements.yaml` REQ-1875 supersession note] [tool-verified: refresh handler returns code `graphql_remote.branded_source_not_refreshed`]

---

### GitLab (REQ-1923)

GitLab ist ein gewöhnlicher Quellentyp und wird auf dieselbe Weise wie GitHub hinzugefügt und registriert: Fügen Sie eine Quelle vom Typ **GitLab** mit einem Zugriffstoken hinzu und registrieren Sie dann die gewünschten Tabellen aus dem Schema `graphql`. Das Standardpräfix für Tabellennamen ist `gl`. Die Quelle erreicht `gitlab.com`. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["gitlab"]`]

**Spalten auswählen.** GitLab bewertet jede Abfrage mit Kosten und lehnt eine zu teure ab: 200 Punkte für einen anonymen Aufrufer, 250 mit Token. Eine breite Tabelle mit allen ausgewählten Spalten liegt über diesem Preis, registrieren Sie daher eine GitLab-Tabelle mit den Spalten, die Sie wollen. [tool-verified: live against gitlab.com 2026-10-02, `project.issues` with all 63 columns answered "Query has complexity of 1733, which exceeds max complexity of 200"; with 14 chosen columns it registered and read]

- Wenn Sie eine Tabelle registrieren, fragt Provisa GitLab einmal, ob die Auswahl bei der Seitengröße der Lesevorgänge bedient wird. Antwortet GitLab, die Abfrage sei zu komplex oder zu groß, wird die Tabelle nicht registriert, und das Ergebnis enthält die Meldung von GitLab: `Table 'gl__project_issues' was not registered with the columns selected: Query has complexity of 1733, which exceeds max complexity of 200. Choose fewer columns.` [tool-verified: `provisa/graphql_remote/probe.py` `QueryTooComplex`; `provisa/api/admin/_table_ops.py` code `schema.table_too_complex`]
- Was eine Spalte kostet, hängt von ihrer Art ab. Ein einfacher Wert kostet etwa einen Punkt; eine Spalte mit verschachteltem Objekt kostet ein Vielfaches davon. Das Weglassen von Spalten mit verschachtelten Objekten spart am meisten. [tool-verified: live, five scalar columns scored 26 at 100 rows a page; two small object columns added 18]
- Die Seitengröße ist Teil des Preises. Sie entspricht `graphql_remote.max_list_items`. [tool-verified: live, the same five columns scored 15 at 5 rows a page and 26 at 100]

**Token-Prüfung.** GitLab beantwortet ein unbekanntes Token mit einem leeren Ergebnis, nicht mit einem Fehler. Provisa wertet das als abgelehntes Token und fügt die Quelle nicht hinzu. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_verify_live_auth`]

---

### gRPC Remote Schema (REQ-322–329)

**Quelle hinzufügen.** POST an `/admin/grpc-remote/register` mit der Serveradresse, einem Pfad oder einer URL zu einer `.proto`-Datei sowie optionaler TLS-Konfiguration. Das Hinzufügen der Quelle registriert keine Tabelle.

```json
{
  "source_id": "orders-grpc",
  "proto_path": "https://api.example.com/orders.proto",
  "server_address": "grpc.example.com:443",
  "namespace": "orders",
  "domain_id": "commerce",
  "tls": true,
  "cache_ttl": 300,
  "method_overrides": { "CreateOrder": "query" },
  "relationships": [
    { "source_table": "orders__OrderService__ListOrders", "source_column": "customer_id",
      "target_table": "customers__CustomerService__GetCustomer", "target_column": "id" }
  ]
}
```

Provisa ruft die Proto-Datei ab, analysiert sie mit einem reinen Textparser (ohne externe Proto-Abhängigkeiten zum Analysezeitpunkt), kompiliert Python-Stubs über `grpc_tools.protoc` und öffnet einen dauerhaften `grpc.aio.Channel`. (REQ-322) [tool-verified: `provisa/grpc_remote/loader.py:99–128`, `provisa/grpc_remote/loader.py:166–214`, `provisa/api/admin/grpc_remote_router.py:80–104`]

Proto-Dateien können auch als lokale Pfade angegeben werden. Importpfade für allgemein bekannte Typen (`google/protobuf/timestamp.proto`) werden zum Registrierungszeitpunkt gespeichert und bei der Aktualisierung wiederverwendet. (REQ-329) [tool-verified: `provisa/grpc_remote/loader.py:135–159`]

**Was die Quelle anbietet.** Jede `rpc`-Methode im Proto wird anhand von drei Signalen in Prioritätsreihenfolge als Query oder Mutation klassifiziert: (REQ-323) [tool-verified: `provisa/grpc_remote/mapper.py`]

1. **`method_overrides`** in der Registrierungs-Payload — `{"MethodName": "query"}` oder `{"MethodName": "mutation"}` hat Vorrang vor allem anderen.
2. **`server_streaming: true`** — der Server sendet einen Stream von Nachrichten; immer eine virtuelle Tabelle (sofern die Ausgabe kein Skalar ist).
3. **Die Ausgabemeldung besitzt ein wiederholtes Feld vom Typ Message** — z. B. wird `ListOrdersResponse { repeated Order items; }` als Listen-Wrapper behandelt und zu einer virtuellen Tabelle. Wiederholte skalare Felder (z. B. `repeated string tags`) lösen dies nicht aus — es handelt sich um Array-Eigenschaften einer einzelnen Entität, nicht um Zeilenquellen.

Methoden, die keinem dieser Signale entsprechen (unäres RPC mit Rückgabe einer einzelnen Entitätsmeldung oder jede skalare Ausgabe) werden zu nachverfolgten Funktionen.

**Tabellen registrieren.** Jede Query-Methode wird als eine Tabelle angeboten, benannt `{namespace}__{Service}__{Method}`, unter dem Auswahlschema `grpc_remote`. Registrieren Sie die gewünschten über die Register-Table-Auswahl (`availableTables`, `availableColumns`, `registerTable`, wie bei GraphQL-Quellen) und wählen Sie Antwortspalten. Anfragefelder werden zu `_nf_*`-Spalten für native Filter, und diese sind stets enthalten. (REQ-322) [tool-verified: `provisa/api/admin/introspect.py` `_native_tables_grpc` (`if schema_name != "grpc_remote": return []`), `provisa/api/admin/grpc_remote_router.py` `query_table_name`, `query_columns`, `_register_schema` (`if table_name not in registered: continue`), `provisa/api/admin/_table_ops.py` `_grpc_columns_for_input`]

Mutation-Methoden sind angebotene Commands und werden in `available_mutations` gezählt; das Hinzufügen der Quelle erfasst keine. Eine gRPC-Mutation wird auf der Seite „Commands“ registriert, indem man die Quelle und dann die Methode wählt, benannt `Service.Method`; die Art des Commands ist `source_operation`. Siehe [Schreiboperation einer externen Quelle](commands.md#schreiboperation-einer-externen-quelle-req-1924). (REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `grpc_operation_name`, `_grpc_operations`; `provisa/api/admin/actions_router.py` `_as_source_operation`]

**Benennung von Tabellen.** Der Standardname lautet `{namespace}__{ServiceName}__{MethodName}`. Ohne Namespace werden Dienst- und Methodenname direkt verbunden. Jeder registrierten Tabelle kann ein `alias` zugewiesen werden; ist dieser gesetzt, wird der Alias überall als Name verwendet (Abfragen, SDL, Beziehungen). Der automatisch generierte Name ist der Registrierungsschlüssel und ändert sich nie. (REQ-322) [tool-verified: `provisa/core/repositories/table.py:129–134`]

**Typzuordnung (REQ-324).** Proto-Skalartypen werden wie folgt auf SQL-Typen abgebildet. [tool-verified: `provisa/grpc_remote/mapper.py:31–47`]

| Proto-Typ | SQL-Typ |
| --- | --- |
| `string`, `bytes` | `text` |
| `int32` / `uint32` / `sint32` / `fixed32` / `sfixed32` | `integer` |
| `int64` / `uint64` / `sint64` / `fixed64` / `sfixed64` | `bigint` |
| `float` | `real` |
| `double` | `numeric` |
| `bool` | `boolean` |
| `repeated <T>` | `jsonb` |
| Verschachtelte Message | `jsonb` |
| Enum | `text` |

**Beziehungen zum Registrierungszeitpunkt.** `relationships` funktioniert identisch zum GQL-Adapter — es deklariert FK/PK-Verknüpfungspfade, die als manuell deklarierte Beziehungen gespeichert werden (ohne `remote_managed`-Flag). Bei einer Aktualisierung bleiben diese unverändert erhalten. (REQ-554) [tool-verified: `provisa/api/admin/grpc_remote_router.py:93–109`]

**Query-Methoden (REQ-325).** Felder der Ausgabemeldung werden zu Tabellenspalten. Felder der Eingabemeldung werden sowohl zu GraphQL-Argumenten, die an den externen Aufruf übergeben werden, *als auch* als Spalten mit dem Präfix `_nf_` und `native_filter_type: "grpc_input"` registriert — derselbe Mechanismus, den GQL und OpenAPI für die Injektion nativer Filter verwenden. (REQ-555) [tool-verified: `provisa/api/admin/grpc_remote_router.py:207–213`]

**Unterfelder verschachtelter Messages.** Bei Query-Methoden werden für nicht wiederholte, messagetypisierte Felder auf Tiefe 0 (direkte Ausgabespalten) deren Unterfelder eine Ebene tiefer aufgelöst und als `object_fields` im `ColumnDef` gespeichert. Diese Metadaten werden für die `jsonb`-Unterfeldextraktion in SQL sowie für die Schemadokumentation verwendet. Felder, die über Tiefe 1 hinaus verschachtelt sind, werden nicht rekursiv expandiert. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

Server-Streaming-Methoden sammeln alle gestreamten Meldungen in einer Liste, bevor Zeilen zurückgegeben werden. (REQ-325) [tool-verified: `provisa/grpc_remote/executor.py:86–119`]

**Mutation-Methoden (REQ-326).** Eine registrierte Mutation-Methode ist ein Command, dessen Argumente die Felder der Eingabemeldung sind, jeweils als `json` typisiert und unverändert durchgereicht. Die Antwort des externen Dienstes kommt als Zeilen zurück; ein abgelehnter Aufruf ist ein 422, `functions.remote_refused`. Siehe [Schreiboperation einer externen Quelle](commands.md#schreiboperation-einer-externen-quelle-req-1924). (REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `_grpc_operations`, `_call_grpc`, `_refused`]

**Kanalverwaltung.** Pro registrierter Quelle wird ein `grpc.aio.Channel` im Anwendungszustand gespeichert und für nachfolgende Anfragen wiederverwendet. Der alte Kanal wird geschlossen, bevor bei einer Aktualisierung ein neuer geöffnet wird. (REQ-327) [tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**Aktualisierung.** POST an `/admin/grpc-remote/refresh/{source_id}`. Lädt das Proto erneut vom gespeicherten Pfad, kompiliert die Stubs neu und bringt die bereits registrierten Tabellen mit den Spalten, mit denen jede registriert wurde, auf den Stand des Protos. Es registriert keine neue Tabelle; eine dem Proto hinzugefügte Query-Methode bleibt im Angebot. Alternativ: PUT an `/admin/grpc-remote/{source_id}/proto` mit neuem `proto_text`, um das Proto inline zu aktualisieren. (REQ-329) [tool-verified: `provisa/api/admin/grpc_remote_router.py` `refresh_grpc_remote_source`, `_load_and_register` and `put_grpc_proto` (both pass `registered=await registered_query_tables(conn, source_id)`)]

**Einschränkungen.**

- Die Extraktion von Objekt-Unterfeldern ist auf eine Ebene beschränkt. Verschachtelte Message-Felder jenseits von Tiefe 1 werden nicht rekursiv expandiert. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST (REQ-314–321)

**Quelle hinzufügen.** POST an `/admin/openapi/register` mit einer Quellen-ID und einer Spezifikation, geladen aus einer lokalen Datei oder URL. Die Spezifikation wird geparst und zusammen mit der Quelle aufbewahrt; es wird weder eine Tabelle noch ein Command registriert. Die Antwort meldet `tables: 0` und `mutations: 0`, die angebotenen Zähler stehen in `available_tables` und `available_mutations`. (REQ-314) [tool-verified: `provisa/openapi/loader.py:30–55`, `provisa/api/admin/openapi_router.py` `_load_and_register` docstring: "Tables and functions are NOT auto-registered here. Users register them individually via the Register Table / Register Action UI."]

**Tabellen registrieren.** Registrieren Sie jede gewünschte GET-Operation über die Register-Table-Auswahl (`availableTables`, `availableColumns`, `registerTable`) und wählen Sie Spalten. Registrieren Sie jede Nicht-GET-Operation einzeln als Command auf der Seite „Commands“, aufgelistet durch `availableFunctions`; siehe [Schreiboperation einer externen Quelle](commands.md#schreiboperation-einer-externen-quelle-req-1924). `PUT /admin/openapi/spec/{source_id}` speichert eine von Hand bearbeitete Spezifikation, registriert nichts und gibt `available_tables` und `available_mutations` zurück. (REQ-316) [tool-verified: `provisa/api/admin/openapi_router.py` `put_openapi_spec`; `provisa/api/admin/schema_query.py` `available_functions` ("returns non-GET operations")] [tool-verified: `provisa/api/admin/_table_ops.py` `_build_columns_for_input`; a registered OpenAPI table is read through the operation in the stored spec, `provisa/api/data/materialization.py` (`state.openapi_specs`)]

**Registrierungs-Payload.** Der Endpunkt `/admin/openapi/register` akzeptiert neben `source_id`, `spec_path` usw. zwei zusätzliche Felder:

```json
{
  "operation_overrides": { "createPet": "query", "listOrders": "mutation" },
  "relationships": [
    { "source_table": "pets__listPets", "source_column": "owner_id",
      "target_table": "owners__listOwners", "target_column": "id" }
  ]
}
```

**Was die Quelle anbietet.** Jede GET-Operation in der Spezifikation wird als Tabelle angeboten, sofern ihr Antwortschema nicht ein Skalartyp ist (`string`, `number`, `boolean`, `integer`) — GET-Operationen mit skalarer Rückgabe sind stattdessen Funktionen mit einer einzelnen Spalte `value`. Jede Nicht-GET-Operation (POST, PUT, PATCH, DELETE) wird als Command angeboten, benannt nach ihrer `operationId`. Registriert, nimmt sie die Pfadparameter der Operation und ein Argument `body` für den Request-Body entgegen, jeweils als `json` typisiert; jedes andere Argument kommt in den Query-String. Siehe [Schreiboperation einer externen Quelle](commands.md#schreiboperation-einer-externen-quelle-req-1924). (REQ-316, REQ-317, REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `_openapi_operations`, `_call_openapi`]

Priorität der Klassifizierung: `operation_overrides` (Payload) hat Vorrang vor `x-provisa-kind` (Spezifikationserweiterung), was wiederum Vorrang vor der GET-Heuristik hat. `operation_overrides` ist der empfohlene Override-Pfad; `x-provisa-kind` ist für Fälle gedacht, in denen die Spezifikation selbst die Klassifizierung tragen soll. (REQ-408) [tool-verified: `provisa/openapi/mapper.py:192–203`]

**Beziehungen zum Registrierungszeitpunkt.** `relationships` funktioniert identisch zu den anderen Adaptern — gespeichert als manuell deklarierte Beziehungen, bei Aktualisierungen erhalten. (REQ-554) [tool-verified: `provisa/api/admin/openapi_router.py:103–108`]

**Benennung von Tabellen.** Tabellen verwenden die `operationId` der Operation. Ist keine `operationId` definiert, erzeugt Provisa einen Slug aus `{method}_{path}`. Ein Alias wird abgeleitet, indem das führende Verb-Segment entfernt und das Substantiv in den Singular gesetzt wird (`findPetsByStatus` → `pet_by_status`). (REQ-557) [tool-verified: `provisa/openapi/register.py:39–56`]

**Typzuordnung.** JSON-Schema-Typen werden wie folgt auf Provisa-Typen abgebildet. [tool-verified: `provisa/openapi/register.py:59–70`]

| JSON-Schema-Typ | Provisa-Typ |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**Parameter als native Filterspalten.** Pfad- und Query-Parameter, die nicht bereits Antwortfelder sind, werden zu Spalten mit `native_filter_type` auf `path_param` oder `query_param`, mit dem Präfix `_nf_`. Stimmt der Name eines Parameters mit dem Namen eines Antwortfelds überein, werden die Parameter-Metadaten in den vorhandenen Spalteneintrag zusammengeführt, statt ein Duplikat zu erzeugen. (REQ-555) [tool-verified: `provisa/openapi/register.py:116–122`, `provisa/openapi/register.py:172–196`]

**Auflösung des Antwortschemas.** Der Mapper prüft `responses.200`, dann `responses.2xx`, dann `responses.default`. Array-typisierte Antworten werden auf ihr Elementschema zurückgeführt. `$ref`-Referenzen werden eine Ebene tief aufgelöst. (REQ-316) [tool-verified: `provisa/openapi/mapper.py:83–101`]

**Objekt-Unterfelder.** Antwort-Properties vom `type: object` mit eigenen `properties` werden als `object_fields` auf der Spalte gespeichert. Diese Unterfelder sind in der SDL sichtbar und werden für die `jsonb`-Extraktion in Abfragen verwendet. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]

**Zwischenspeicherung von Antworten (REQ-318).** Ergebnisse von GET-Operationen werden von `pg_cache.py` in PostgreSQL zwischengespeichert. Jede Kombination von Anfrageparametern erhält eine eigene `_params_hash`-Gruppe. Zeilen eines bestimmten Hash werden ersetzt, sobald der TTL abläuft. Endpunkte mit Pfadparameter (`/pets/{id}`) überspringen den anfänglichen Massenabruf — die Cache-Tabelle wird für die Schema-Introspektion leer erstellt und anschließend anfragenweise pro Primärschlüssel befüllt. [tool-verified: `provisa/openapi/pg_cache.py:181–234`, `provisa/openapi/pg_cache.py:307–360`]

**Aktualisierung (REQ-321).** POST an `/admin/openapi/refresh/{source_id}`. Parst die Spezifikation erneut über `_load_and_register`, das nichts registriert: Es fügt weder eine Tabelle noch eine Spalte hinzu. Bestehende Governance-Regeln bleiben erhalten. [tool-verified: `provisa/api/admin/openapi_router.py` `refresh_openapi_source`, `_load_and_register`] Eine registrierte Tabelle behält ihre Spalten; sie wird über die Operation der aktualisierten Spec gelesen. [tool-verified: `provisa/api/admin/openapi_router.py` `_load_and_register` (replaces `state.openapi_specs[source_id]`), `provisa/api/data/materialization.py`]

**Einschränkungen.**

- Die Extraktion von Objekt-Unterfeldern ist auf eine Ebene beschränkt. In `object_fields` verschachtelte Properties werden nicht rekursiv expandiert. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]
- Header- und Cookie-Parameter werden ignoriert; nur `path`- und `query`-Parameter werden registriert. (REQ-555) [tool-verified: `provisa/openapi/mapper.py:144–158`]
- Die Auflösung von `$ref` auf Spezifikationsebene ist bei Property-Schemas auf eine Ebene beschränkt; tief verschachtelte Komponentenreferenzen lassen sich möglicherweise nicht auflösen. [tool-verified: `provisa/openapi/mapper.py:51–60`]

---

## Auswirkung der Registrierung einer externen Tabelle

Eine aus einer beliebigen Remote-Schema-Quelle registrierte Tabelle ist eine vollwertige Provisa-Tabelle. Zur Laufzeit wird sie in keiner Weise anders behandelt als eine lokal verbundene relationale Tabelle. (REQ-308, REQ-313)

**Abfrageschnittstellen.** Die Tabelle ist sofort über GraphQL, SQL (pgwire oder direkt), Cypher (GQL), JSON:API und Arrow Flight abfragbar. (REQ-001, REQ-267, REQ-345, REQ-257, REQ-051) Die Schemagenerierung synthetisiert `ColumnMetadata` für externe Tabellen, da diese keinen Katalog besitzen — die Typzuordnung wird beim Schema-Build angewendet. (REQ-602) [tool-verified: `provisa/api/app.py:1367–1386`]

**Sicherheitsmodell.** Alle fünf Governance-Schichten gelten:

1. Domänenzugriffskontrolle — die `domain_id` der Tabelle steuert, welche Rollen sie sehen können. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1064–1076`]
2. Sicherheit auf Zeilenebene (RLS) — auf der Tabelle konfigurierte Zeilenfilter werden unabhängig von der Schnittstelle in jede Abfrage injiziert. (REQ-040, REQ-041)
3. Spaltensichtbarkeit — die `visible_to`-Liste jeder Spalte steuert die Feldfreigabe pro Rolle. (REQ-039)
4. Spaltenmaskierung — Maskierungsregeln werden in Stufe 2 der Governance-Pipeline angewendet. (REQ-040, REQ-263)
5. Prädikatschutz — maskierte Spalten werden in WHERE- und HAVING-Klauseln abgelehnt. (REQ-603)

Ad-hoc-Abfragen gegen externe Tabellen sind allein auf Grundlage der Rechte des Benutzers zulässig — der Zugriff basiert einheitlich auf Rechten (Tabellen-/Spaltenrechte + genehmigte Beziehungen), ohne tabellenspezifischen Governance-Modus. (REQ-001, REQ-003)

**Beziehungsgovernance (V002).** JOIN-Bedingungen gegen externe Tabellen — bei Abfrage über SQL oder Cypher — müssen einer registrierten, genehmigten Beziehung entsprechen. (REQ-604) Die V002-Prüfung wird bei GraphQL-Abfragen übersprungen, da in der SDL definierte Beziehungen konstruktionsbedingt bereits genehmigt sind. Siehe [docs/security.md](security.md#governance-der-beziehungen-v002).

**OBJECT-typisierte Spalten.** Wenn eine Spalte einem nicht governance-pflichtigen Inline-GQL-OBJECT oder einem OpenAPI-Objekttyp entspricht, ist ihr Provisa-Typ `jsonb`. Die Spalte speichert den vollständigen verschachtelten JSON-Blob. Sind Unterfelder deklariert (`gql_object_fields` oder `object_fields`), wird die `gql_object_columns`-Zuordnung beim Schema-Build befüllt. Der SQL-Generator verwendet diese Zuordnung, um `->>`-Extraktionsausdrücke für Unterfelder zu erzeugen, wenn eine Abfrage diese auswählt. [tool-verified: `provisa/api/app.py:1305–1315`, `provisa/compiler/schema_gen.py:80–82`]

**Erforderliche Argumente als native Filterparameter.** Wurzel-Query-Felder mit Non-Null-Argumenten ohne Standardwert injizieren zusätzliche Spalten in die registrierte Tabelle. Diese Spalten tragen `native_filter_type: query_param`. Der Cypher-Übersetzer schreibt `WHERE n.id = $val` zu `WHERE n._nf_id = $val` um, und der GraphQL-Executor übernimmt sie als Variablen, die an den externen Endpunkt übergeben werden. (REQ-555) [tool-verified: `provisa/api/app.py:1280–1303`]

---

## Auswirkung der Erstellung einer abdeckenden Beziehung

Wenn ein Data Steward eine Beziehung zwischen zwei externen Tabellen (oder zwischen einer externen und einer lokalen Tabelle) registriert, wird diese Beziehung zum Join-Pfad, der zur Abfragezeit verwendet wird.

**Wie sich der Join durchsetzt.** Bei der Abfragekompilierung löst Provisa den Join-Pfad über die registrierte Beziehung auf. `source_column` und `target_column` der Beziehung werden zur Join-Bedingung im generierten SQL. Der Join ersetzt jeden pro-Tabelle-Aufruf an die externe Quelle, der andernfalls für den verbundenen Typ erforderlich wäre.

**Der rohe Blob wird in SQL nie offengelegt.** Die Spalte `breed` auf `petstore__pets` ist in SQL-Abfragen nicht als roher jsonb-Wert auswählbar. Wenn eine Beziehung zwischen `petstore__pets` und `petstore__breeds` registriert ist, durchlaufen SQL-Abfragen den Join — `SELECT breed.name FROM petstore__pets` wird über den FK-Join aufgelöst, nicht über einen Blob. Ist keine Beziehung registriert, verfügt die Spalte aber über deklarierte Unterfelder (`gql_object_fields`), werden SQL-Unterfeldreferenzen zu `->>`-Extraktion gegen den gespeicherten Blob umgeschrieben. Dieser Pfad steht nur für nicht governance-pflichtige Inline-Typen zur Verfügung — Felder mit governance-pflichtigem Zieltyp sind vollständig von der SDL ausgeschlossen und besitzen keinen Blob, aus dem extrahiert werden könnte. Der rohe Blob selbst wird nie als bloßer Spaltenwert ausgegeben. [tool-verified: `provisa/compiler/sql_gen.py:1156`, `tests/unit/test_sql_gen.py:TestGqlJsonBlobExtraction`]

In der GraphQL-SDL wird ein nicht governance-pflichtiges Inline-OBJECT-Feld als der verschachtelte Objekttyp typisiert. Ob es zur Laufzeit über einen Join oder über Blob-Extraktion bedient wird, ist ein Implementierungsdetail — die SDL-Form ist in beiden Fällen identisch. Wird der untergeordnete Typ als eigene Tabelle registriert (und damit governance-pflichtig), gelten alle fünf Governance-Schichten unabhängig für ihn: eigene RLS-Regeln, Spaltensichtbarkeit, Maskierungsregeln, Prädikatschutz und Domänenzugriffskontrolle. (REQ-039, REQ-040, REQ-041, REQ-263) Blob-Extraktion umgeht dies — die untergeordneten Daten treffen bereits eingebettet in der übergeordneten Zeile ein und unterliegen nur den Regeln der übergeordneten Tabelle. Das Registrieren des untergeordneten Typs als Tabelle und das Erstellen einer Beziehung ist der Weg zu feingranularer Governance auf dem untergeordneten Typ.

**`graphql_alias` auf der Beziehung.** Das Feld `graphql_alias` benennt das SDL-Feld, das die Beziehung auf dem übergeordneten Typ freigibt. Fehlt es, wird der Name aus dem `field_name` der Zieltabelle und der Kardinalität der Beziehung über `rel_field_name(target.field_name, cardinality)` abgeleitet. (REQ-605) [tool-verified: `provisa/compiler/schema_gen.py:1050`]

**V002 auf dem Join-Pfad.** SQL- und Cypher-Abfragen, die die Beziehung durchlaufen, unterliegen der V002-Beziehungsgovernance. Die Beziehung muss registriert und genehmigt sein, damit der Join zulässig ist. (REQ-604) Der GraphQL-Durchlauf über das SDL-Beziehungsfeld ist stets im Voraus genehmigt. [tool-verified: `docs/security.md:41–54`]

**Remote-managed-Flag.** Beziehungen, die während der Registrierung eines GraphQL Remote Schema automatisch erkannt werden, werden mit `remote_managed: True` gespeichert. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:199`] Dies ist ein Metadaten-Marker; er verändert das Governance-Verhalten nicht.

---

## Verhalten reiner Typdefinitionen

Nicht jeder Typ in einem externen Schema muss eine abfragbare Tabelle sein.

Wenn `root_table_ids` auf einem `SchemaInput` gesetzt ist, werden Tabellen, deren ID in dieser Menge fehlt, aus den Wurzel-Query-Feldern der generierten SDL ausgeschlossen. Sie bleiben als GraphQL-Typen vorhanden und sind über Beziehungsfelder auf Tabellen erreichbar, die selbst Wurzeleinträge besitzen. (REQ-601) [tool-verified: `provisa/compiler/schema_gen.py:1062–1069`]

Derselbe Mechanismus gilt für domänengefilterte Schema-Builds: Tabellen in Domänen, auf die die Rolle keinen Zugriff hat, sind reine Typdefinitionen — ihre Typdefinition existiert in der SDL für den Beziehungsdurchlauf, aber es wird kein Wurzel-Query-Feld für sie generiert. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1068–1076`]

Eine Tabelle mit reiner Typdefinition:

- Besitzt kein Wurzel-Query-Feld — Clients können sie nicht direkt namentlich abfragen.
- Ist über Beziehungsfelder auf Tabellen erreichbar, die Wurzeleinträge besitzen.
- Erscheint weiterhin als benannter Typ in der Schema-Introspektion.
- Behält alle Governance-Regeln bei, wenn auf Daten über eine Beziehung zugegriffen wird. (REQ-039, REQ-040)

Eine vollständige Entfernung aus dem Schema — einschließlich der Typdefinition — erfolgt nur, wenn die Tabellenregistrierung vollständig gelöscht wird. Das Markieren einer Tabelle als reine Typdefinition (durch Entfernen ihrer ID aus `root_table_ids` oder durch Filterung nach Domänenzugriff) entfernt den Typ nicht.

Dieses Design ermöglicht es Data Stewards, navigierbare Objektgraphen offenzulegen, in denen manche Typen ausschließlich durch Durchlauf erreichbar sind, nicht durch eigenständige Abfrage.
