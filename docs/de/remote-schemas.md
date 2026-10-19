# Externe Schemas {: #remote-schemas }

Eine externe Schema-Quelle verbindet eine externe API, ob GraphQL (GitHub eingeschlossen), gRPC oder REST (OpenAPI), mit dem Provisa-Modell. Das Hinzufügen der Quelle registriert keine Tabelle. Die Quelle bietet Tabellen an, und ein Steward registriert jede gewünschte über die Auswahl Register Table; diese Registrierung ist der Kuratierungsschritt. Eine registrierte Tabelle ist eine vollwertige Provisa-Tabelle, also gelten jede Governance-Regel, jede Abfrageschnittstelle und jede Sicherheitsschicht automatisch für sie. Der externe Dienst sieht die Governance-Regeln von Provisa nie.

---

## Drei Quellentypen {: #three-source-types }

### GraphQL Remote Schema

**Quelle hinzufügen.** POST an `/admin/sources/graphql-remote` mit der Endpunkt-URL, einem Namespace und optionaler Authentifizierung. Provisa löst eine Standard-`__schema`-Introspektionsabfrage gegen den externen Endpunkt aus, um Endpunkt und Zugangsdaten zu bestätigen.

Das Hinzufügen der Quelle registriert weder eine Tabelle noch einen Command. Jeder Quellentyp für externe Schemas beantwortet Hinzufügen und Aktualisieren mit denselben Zählern: `tables` (registrierte oder aktualisierte Tabellen; beim Hinzufügen 0), `available_tables` (angebotene Tabellen), `mutations` (immer 0) und `available_mutations` (angebotene Commands).

**Tabellen registrieren.** Öffnen Sie „Tables“, dann „Register Table“, wählen Sie die Quelle und das Schema `graphql` und wählen Sie die gewünschten Tabellen und Spalten. Über die Admin-GraphQL-API: `availableTables(sourceId, schemaName)` listet die angebotenen Tabellen auf, `availableColumns` listet die Spalten einer Tabelle auf, und `registerTable(input: TableInput)` registriert eine Tabelle mit den gewählten Spalten. Eine registrierte Tabelle unterliegt anschließend der Governance.

Wie eine registrierte Tabelle gelesen wird (Wurzelfeld, Zeilenpfad, erforderliche Argumente, Seitenargumente), wird in `sources.mapping["tables"]` gespeichert, sodass ein neu gestarteter Prozess sie liest, ohne das externe Schema abzufragen.

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

Authentifizierungsoptionen: `none`, `bearer` (Authorization-Header), `basic` (Base64-codiert, Benutzername:Passwort).

**Feld-Overrides.** `field_overrides` ist eine `{fieldName: "query" | "mutation"}`-Zuordnung, die nach der Introspektion angewendet wird. Sie hat Vorrang vor der strukturellen Klassifizierung. Nur Felder vom Typ Query können als Mutation neu klassifiziert werden; Felder vom Typ Mutation haben in GraphQL keinen Override-Pfad.

**Beziehungen zum Registrierungszeitpunkt.** `relationships` deklariert FK/PK-Verknüpfungspfade zwischen Tabellen zum Registrierungszeitpunkt. Diese werden als manuell deklarierte Beziehungen gespeichert (ohne `remote_managed`-Flag). Bei einer Aktualisierung werden automatisch erkannte Beziehungen (solche mit `remote_managed: True`) erneut ausgeführt und können sich ändern; manuell deklarierte Beziehungen bleiben unverändert.

**Was die Quelle anbietet.** Jedes Feld des externen Typs `Query`, das ein Objekt oder eine Liste von Objekten zurückgibt, wird als Tabelle angeboten, ebenso jede Relay-Connection unter einem Einzelobjekt-Feld (siehe unten). Das Registrieren einer angebotenen Tabelle macht sie zu einer Tabelle. Jedes Feld des externen Typs `Mutation` ist ein angebotener Command und wird in `available_mutations` gezählt; das Hinzufügen der Quelle registriert keinen. Registrieren Sie die gewünschten als Commands; siehe [Schreiboperation einer externen Quelle](commands.md#a-remote-sources-write-operation).

**Benennung von Tabellen.** Tabellen werden `{namespace}__{field_name}` benannt. Mit dem Namespace `petstore` und einem Query-Feld `pets`: Der Tabellenname lautet `petstore__pets`.

**Relay-Connections.** Viele APIs geben Listen als Relay-Connections zurück: ein Objekt mit `nodes` (oder `edges { node }`) neben `pageInfo`. Provisa bildet eine Connection auf eine Tabelle ihrer Knoten ab und liest sie seitenweise.

- Ein Wurzelfeld, das eine Connection zurückgibt (`securityAdvisories`), wird zu einer Tabelle seiner Knoten.
- Eine Connection auf dem einzelnen Objekt, das ein Wurzelfeld zurückgibt, wird zu einer eigenen Tabelle. Die Tabelle übernimmt die erforderlichen Argumente des Wurzelfelds. Bei `repository(owner, name)` und einer Connection `issues` auf `Repository` heißt die Tabelle `repositoryIssues`, der SQL-Name lautet `gh__repository_issues` unter dem Namespace `gh`. Filtern Sie sie über die Spalten `_nf_owner` und `_nf_name`: `WHERE _nf_owner = 'acme' AND _nf_name = 'widgets'`.
- Eine Connection ist nie eine Spalte. Eine Zeile würde sonst für jede Connection ihres Typs einen Lesevorgang tragen, den der externe Dienst pro Zeile berechnet.
- Eine Connection ist nur dann eine Tabelle, wenn ihr Feld `first` und `after` annimmt, sodass sie seitenweise gelesen werden kann. Eine Connection, die ein eigenes Argument benötigt, ist keine Tabelle. Das gilt auch für eine Connection einer Union und für jede Connection unter einem Wurzelfeld, das eine Liste zurückgibt.

**Typzuordnung.** Skalare Felder werden direkt auf Provisa-Typen abgebildet. OBJECT-Felder unterteilen sich in zwei Fälle, abhängig davon, ob der Zieltyp governance-pflichtig ist (siehe „Governance-pflichtige Tabellen“ unten).

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

**Governance-pflichtige Tabellen.** Ein GQL-Typ ist governance-pflichtig, wenn er im externen Schema als Wurzelfeld von `Query` auftritt. `_collect_queryable_types` erfasst diese während der Registrierung und bevorzugt dabei Felder ohne erforderliche Argumente, damit sie als Join-Ziele im großen Umfang abgerufen werden können.

Wenn eine OBJECT-typisierte Spalte einer governance-pflichtigen Tabelle auf einen anderen governance-pflichtigen Typ verweist, unterliegt diese Spalte gleichzeitig drei Regeln:

1. **Vom GQL-Abruf ausgeschlossen** — das Feld wird beim Abrufen der Zeilen der übergeordneten Tabelle nicht angefragt.
2. **Von der SDL ausgeschlossen** — das Feld erscheint nicht am übergeordneten Typ im generierten Schema.
3. **Nur über eine deklarierte Beziehung zugänglich** — ein Data Steward muss einen JOIN zwischen den beiden materialisierten, governance-pflichtigen Tabellen registrieren. Ohne diesen fehlt das Feld schlicht; es gibt keinen Blob-Fallback.

OBJECT-Typen, die NICHT als Wurzel-Query-Felder erreichbar sind (Inline-Typen wie `ContactInfo` oder `Address`), folgen anderen Regeln: Sie werden als `jsonb`-Blob-Spalten abgerufen und erscheinen in der SDL als verschachtelte Objektfelder. Unterfelder sind über `->>`-Extraktion in SQL zugänglich.

**Felder mit Argument sind keine Spalten.** Ein Feld mit einem erforderlichen Argument kann nicht ohne Weiteres ausgewählt werden und wird daher aus den Spalten der Tabelle und aus verschachtelten Auswahlen ausgelassen.

**Erforderliche Argumente.** Wenn ein Wurzel-Query-Feld Non-Null-Argumente ohne Standardwert besitzt, werden diese zu Spalten mit `native_filter_type: query_param` auf der Tabelle (mit dem Präfix `_nf_` zum Zeitpunkt der Injektion). Der Executor übergibt sie als GraphQL-Variablen.

**Automatisch erkannte Beziehungen.** Provisa durchsucht die OBJECT-typisierten Spalten jeder registrierten Tabelle. Ist der GQL-Typ, auf den eine Spalte verweist, ebenfalls eine registrierte Tabelle derselben Quelle und gehört die Spalte, an der die Beziehung hängt, zu den registrierten Spalten, wird die Beziehung gespeichert. Eine noch nicht registrierte Tabelle bekommt keine. Many-to-one-Beziehungen leiten Quell- und Zielspalte aus Namenskonventionen ab (`breedName` am Quelltyp → `name` am Zieltyp `Breed`). One-to-many-Felder (LIST) erzeugen Beziehungen mit leeren Spaltenverweisen, weil der FK auf der Zielseite liegt.

**Mutationen.** Ein Mutation-Feld wird einzeln als Command der Art `source_operation` registriert. Seine Argumente sind jeweils als `json` typisiert und werden als typisierte Variablen an den externen Dienst übergeben; die Antwort ist das JSON, das der externe Dienst zurückgibt, ohne `return_schema`. Siehe [Schreiboperation einer externen Quelle](commands.md#a-remote-sources-write-operation).

**Aktualisierung.** POST an `/admin/sources/graphql-remote/{id}/refresh`. Führt eine erneute Introspektion des externen Schemas durch und bringt die bereits registrierten Tabellen auf den Stand dieses Schemas. Es fügt weder eine Tabelle noch eine Spalte hinzu: Eine Tabelle oder Spalte, die das Schema hinzugewonnen hat, bleibt im Angebot, und eine Spalte, die das Schema verloren hat, wird entfernt. Bestehende Governance-Regeln (RLS, Maskierung) bleiben erhalten.

**Einschränkungen.**

- Skalare und ENUM-Wurzel-Query-Felder (Rückgabetyp ist nicht OBJECT) werden zu nachverfolgten Funktionen, nicht zu virtuellen Tabellen. Ihr `return_schema` besteht aus einer einzelnen Spalte `value` des zugeordneten Skalartyps.
- Objektverschachtelung wird zum Registrierungszeitpunkt bis zu `graphql_remote.max_object_depth` (Standard: 5) aufgelöst. Sowohl die Auswahl beim externen Abruf als auch die Metadaten der Unterfelder werden bis zu dieser Tiefe erstellt; Felder jenseits des Limits werden nicht abgerufen und stehen für die SQL-Extraktion nicht zur Verfügung. Ein Typ wird entlang eines Pfades nur einmal betreten: Ein Feld, dessen Typ bereits auf dem Weg nach unten liegt, wird ausgelassen, sodass ein Schema, dessen Typen aufeinander verweisen, einmal pro Typ durchlaufen wird und nicht einmal pro Tiefenebene.
- LIST-typisierte verschachtelte OBJECT-Felder (z. B. `breed.awards: [Award]`) werden bis zu `graphql_remote.max_list_depth` Verschachtelungsebenen (Standard: 2) in die Abrufauswahl einbezogen. Innerhalb dieses Limits wird die Liste als `jsonb`-Array in der übergeordneten Spalte abgerufen. Deklariert das Listenfeld ein Argument `first` (Relay, PostGraphile, pg_graphql) oder ein Argument `limit` (Hasura), übergibt die Auswahl es als `first: N` bzw. `limit: N`, wobei N `graphql_remote.max_list_items` (Standard: 100) entspricht. Ein Listenfeld, das keines von beiden deklariert, erhält kein Argument, weil ein externer Dienst ein Argument ablehnt, das das Feld nicht deklariert. Jenseits von `max_list_depth` wird das LIST-Feld vollständig ausgeschlossen, um eine unbegrenzte Datenexpansion zu verhindern. In SQL wird auf das Array über `json_array_elements(column_name)` oder eine Index-Extraktion mit `->>` zugegriffen. Wenn der Elementtyp der Liste über eine eigene Wurzelabfrage verfügt, sollte er stattdessen als separate Tabelle registriert und eine Beziehung erstellt werden. Der Join-Pfad ist effizienter und umgeht den Blob.
- Bei SQL-Abfragen werden nicht governance-pflichtige OBJECT-typisierte Spalten vollständig von der externen Quelle abgerufen (alle Unterfelder bis zur konfigurierten Tiefe) und als `jsonb` zwischengespeichert. Der Zugriff auf Unterfelder in SQL erfolgt über `->>`-Extraktion gegen den Blob; die externe Anfrage wird nicht auf die von der SQL-Abfrage ausgewählten Felder eingeschränkt. Wenn der Elementtyp der Liste keine Wurzelabfrage besitzt und die Blob-Darstellung nicht ausreicht, sollte die Abfrage direkt in GraphQL-SDL geschrieben werden. Provisa gibt die GQL-Feldauswahl originalgetreu wieder, sodass die externe Quelle genau die angeforderten Felder sieht.
- Falls der externe Server ein OBJECT-typisiertes Feld ablehnt, weil eine Unterfeldauswahl erforderlich ist (was nicht auftreten sollte, wenn `gql_selection` verfügbar ist), unternimmt der Executor einen erneuten Versuch ohne diese Felder, damit skalare Spalten dennoch zurückgegeben werden. Das gilt für Tabellen, die von einem Wurzelfeld gelesen werden. Eine Connection-Tabelle nimmt diesen Weg nicht.

**Seitenweises Lesen.** Eine Connection-Tabelle wird per Cursor gelesen. Jede Seite fragt `first: N, after: $pageCursor` mit `pageInfo { hasNextPage endCursor }` an, und der Lesevorgang folgt `endCursor`, bis der externe Dienst keine weitere Seite meldet.

| Einstellung | Standard | Wirkung |
| --- | --- | --- |
| `graphql_remote.max_list_items` | `100` | Zeilen pro Seite. |
| `graphql_remote.max_rows` | `10000` | Die meisten Zeilen, die ein Lesevorgang einer Connection-Tabelle übernimmt. Ein Lesevorgang, der diesen Wert erreicht, stoppt und protokolliert eine Warnung. |

```yaml
graphql_remote:
  max_list_items: 100
  max_rows: 10000
```

Zwei Antworten veranlassen den Executor zu einem erneuten Versuch:

- **Seite zu schwer.** Antwortet der externe Dienst mit 502 oder 504, wird dieselbe Seite in halber Größe erneut angefragt, bis hinunter auf eine Zeile.
- **Ratenbegrenzung mit Wartezeit.** Antwortet der externe Dienst mit 403 oder 429 und einem `Retry-After` von höchstens 120 Sekunden, wartet der Executor so lange und sendet die Anfrage erneut, mit bis zu drei Versuchen. Eine Ablehnung ohne `Retry-After` oder mit einer längeren Wartezeit wird als Fehler ausgelöst. Das gilt für jeden Lesevorgang, ob Connection oder nicht.

Jeder andere Fehler in der Antwort lässt den Lesevorgang scheitern, sofern der Quellentyp nichts anderes festlegt (siehe GitHub unten). Eine Connection, deren übergeordnetes Objekt null zurückgab, hat keine Zeilen.

---

### GitHub

GitHub ist ein gewöhnlicher Quellentyp. Seine API ist GraphQL, daher verhalten sich seine Tabellen wie oben beschrieben, einschließlich Connection-Tabellen wie `gh__repository_issues`.

**Quelle hinzufügen.**

1. Öffnen Sie „Sources“ und fügen Sie eine Quelle vom Typ **GitHub** hinzu.
2. Geben Sie ein GitHub-Zugriffstoken ein. Optional geben Sie einen Namespace ein, das Präfix der Tabellennamen; der Standard ist `gh`.
3. Speichern. Provisa prüft das Token gegen GitHub. Ein Token, das GitHub ablehnt, lässt das Hinzufügen mit der Meldung von GitHub scheitern.

Das Hinzufügen der Quelle registriert keine Tabellen.

**Tabellen registrieren.** Öffnen Sie „Tables“, dann „Register Table“. Wählen Sie die GitHub-Quelle, wählen Sie das Schema `graphql` und wählen Sie dann die gewünschten Tabellen. Jede Tabelle, die GitHub anbietet, wird aufgelistet; die Registrierung ist Ihre Entscheidung, welche Tabellen Sie freigeben.

**Token-Berechtigungen (Scopes).** Wenn Sie eine Tabelle registrieren, prüft Provisa sie einmal mit Ihrem Token gegen GitHub.

- Ein Feld, das die Scopes des Tokens nicht abdecken, wird aus der Tabelle ausgelassen. Das Ergebnis nennt jedes ausgelassene Feld: `Left out, because the source's credential may not read them: projectsV2`.
- Eine Tabelle, die das Token überhaupt nicht lesen kann, wird mit der Begründung von GitHub abgelehnt: `GitHub does not let this source's credential read gh__repository_issues: ...`.

**Zeilen, die das Token nicht sehen darf.** GitHub antwortet mit `FORBIDDEN` für ein Feld, das das Token in einer bestimmten Zeile nicht sehen darf, etwa die Mitwirkenden (Collaborators) eines Repositorys ohne Push-Zugriff, und mit `NOT_ORG_OWNED_REPO` für ein Feld, das nur bei Repositorys im Besitz einer Organisation existiert. Dieses Feld ist in dieser Zeile null, der übrige Lesevorgang bleibt bestehen, und Provisa protokolliert eine Warnung. Ein Fehler gegen die Tabelle selbst lässt den Lesevorgang scheitern.

**Schwere Seiten.** Antwortet GitHub mit `RESOURCE_LIMITS_EXCEEDED`, weil die Berechnung einer Seite zu aufwendig ist, wird die Seite in halber Größe erneut angefragt.

**Verschachtelte Objekte.** GitHub-Tabellen nutzen eine eigene Verschachtelungstiefe von 0 (`max_object_depth=0` für diesen Quelltyp) statt `graphql_remote.max_object_depth`. Eine verschachtelte Objektspalte wird nur mit ihren eigenen Skalarfeldern ausgewählt, und Objekte darin erscheinen als `__typename`.

**Token-Speicherung.** Das Token wird im Secrets-Vault abgelegt, und die Quellzeile behält eine Referenz, sodass ein Neustart die Quelle erneut liest, ohne dass das Token neu eingegeben werden muss.

**Funktionsweise (für Betreiber).** Das Schema von GitHub wird mit Provisa ausgeliefert, daher löst das Hinzufügen der Quelle keinen Introspektionsaufruf aus, und ein großes Schema kostet bei der Registrierung nichts. Tabellen werden daraus einzeln abgebildet, sobald Sie sie registrieren. Der Refresh-Endpunkt lehnt diesen Quellentyp ab; ein neues GitHub-Schema kommt mit einem Provisa-Release.

---

### GitLab

GitLab ist ein gewöhnlicher Quellentyp und wird auf dieselbe Weise wie GitHub hinzugefügt und registriert: Fügen Sie eine Quelle vom Typ **GitLab** mit einem Zugriffstoken hinzu und registrieren Sie dann die gewünschten Tabellen aus dem Schema `graphql`. Das Standardpräfix für Tabellennamen ist `gl`. Die Quelle erreicht `gitlab.com`.

**Spalten auswählen.** GitLab bewertet jede Abfrage mit Kosten und lehnt eine zu teure ab: 200 Punkte für einen anonymen Aufrufer, 250 mit Token. Eine breite Tabelle mit allen ausgewählten Spalten liegt über diesem Preis, registrieren Sie daher eine GitLab-Tabelle mit den Spalten, die Sie wollen.

- Wenn Sie eine Tabelle registrieren, fragt Provisa GitLab einmal, ob die Auswahl bei der Seitengröße der Lesevorgänge bedient wird. Antwortet GitLab, die Abfrage sei zu komplex oder zu groß, wird die Tabelle nicht registriert, und das Ergebnis enthält die Meldung von GitLab: `Table 'gl__project_issues' was not registered with the columns selected: Query has complexity of 1733, which exceeds max complexity of 200. Choose fewer columns.`
- Was eine Spalte kostet, hängt von ihrer Art ab. Ein einfacher Wert kostet etwa einen Punkt; eine Spalte mit verschachteltem Objekt kostet ein Vielfaches davon. Das Weglassen von Spalten mit verschachtelten Objekten spart am meisten.
- Die Seitengröße ist Teil des Preises. Sie entspricht `graphql_remote.max_list_items`.

**Tokenprüfung.** GitLab beantwortet ein unbekanntes Token mit einem leeren Ergebnis statt mit einem Fehler. Provisa wertet das als abgelehntes Token und fügt die Quelle nicht hinzu.

---

### gRPC Remote Schema

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

Provisa ruft die Proto-Datei ab, analysiert sie mit einem reinen Textparser (ohne externe Proto-Abhängigkeiten zum Analysezeitpunkt), kompiliert Python-Stubs über `grpc_tools.protoc` und öffnet einen dauerhaften `grpc.aio.Channel`.

Proto-Dateien können auch als lokale Pfade angegeben werden. Importpfade für allgemein bekannte Typen (`google/protobuf/timestamp.proto`) werden zum Registrierungszeitpunkt gespeichert und bei der Aktualisierung wiederverwendet.

**Was die Quelle anbietet.** Jede `rpc`-Methode im Proto wird anhand von drei Signalen in Prioritätsreihenfolge als Query oder Mutation klassifiziert:

1. **`method_overrides`** in der Registrierungs-Payload — `{"MethodName": "query"}` oder `{"MethodName": "mutation"}` hat Vorrang vor allem anderen.
2. **`server_streaming: true`** — der Server sendet einen Stream von Nachrichten; immer eine virtuelle Tabelle (sofern die Ausgabe kein Skalar ist).
3. **Die Ausgabemeldung besitzt ein wiederholtes Feld vom Typ Message** — z. B. wird `ListOrdersResponse { repeated Order items; }` als Listen-Wrapper behandelt und zu einer virtuellen Tabelle. Wiederholte skalare Felder (z. B. `repeated string tags`) lösen dies nicht aus, denn es handelt sich um Array-Eigenschaften einer einzelnen Entität, nicht um Zeilenquellen.

Methoden, die keinem dieser Signale entsprechen (unäres RPC mit Rückgabe einer einzelnen Entitätsmeldung oder jede skalare Ausgabe) werden zu nachverfolgten Funktionen.

**Tabellen registrieren.** Jede Query-Methode wird als eine Tabelle angeboten, benannt `{namespace}__{Service}__{Method}`, unter dem Auswahlschema `grpc_remote`. Registrieren Sie die gewünschten über die Register-Table-Auswahl (`availableTables`, `availableColumns`, `registerTable`, wie bei GraphQL-Quellen) und wählen Sie Antwortspalten. Anfragefelder werden zu `_nf_*`-Spalten für native Filter, und diese sind stets enthalten.

Mutation-Methoden sind angebotene Commands und werden in `available_mutations` gezählt; das Hinzufügen der Quelle erfasst keine. Eine gRPC-Mutation wird auf der Seite „Commands“ registriert, indem man die Quelle und dann die Methode wählt, benannt `Service.Method`; die Art des Commands ist `source_operation`. Siehe [Schreiboperation einer externen Quelle](commands.md#a-remote-sources-write-operation).

**Benennung von Tabellen.** Der Standardname lautet `{namespace}__{ServiceName}__{MethodName}`. Ohne Namespace werden Dienst- und Methodenname direkt verbunden. Jeder registrierten Tabelle kann ein `alias` zugewiesen werden; ist dieser gesetzt, wird der Alias überall als Name verwendet (Abfragen, SDL, Beziehungen). Der automatisch generierte Name ist der Registrierungsschlüssel und ändert sich nie.

**Typzuordnung.** Proto-Skalartypen werden wie folgt auf SQL-Typen abgebildet.

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

**Beziehungen bei der Registrierung.** `relationships` funktioniert genau wie beim GQL-Adapter: Es deklariert FK/PK-Join-Pfade, gespeichert als manuell deklarierte Beziehungen (ohne Flag `remote_managed`). Eine Aktualisierung lässt sie unverändert.

**Query-Methoden.** Felder der Ausgabemeldung werden zu Tabellenspalten. Felder der Eingabemeldung werden sowohl zu GraphQL-Argumenten, die an den externen Aufruf übergeben werden, *als auch* als Spalten mit dem Präfix `_nf_` und `native_filter_type: "grpc_input"` registriert, derselbe Mechanismus, den GQL und OpenAPI für die Injektion nativer Filter verwenden.

**Unterfelder verschachtelter Messages.** Bei Query-Methoden werden für nicht wiederholte, messagetypisierte Felder auf Tiefe 0 (direkte Ausgabespalten) deren Unterfelder eine Ebene tiefer aufgelöst und als `object_fields` im `ColumnDef` gespeichert. Diese Metadaten werden für die `jsonb`-Unterfeldextraktion in SQL sowie für die Schemadokumentation verwendet. Felder, die über Tiefe 1 hinaus verschachtelt sind, werden nicht rekursiv expandiert.

Server-Streaming-Methoden sammeln alle gestreamten Meldungen in einer Liste, bevor Zeilen zurückgegeben werden.

**Mutation-Methoden.** Eine registrierte Mutation-Methode ist ein Command, dessen Argumente die Felder der Eingabemeldung sind, jeweils als `json` typisiert und unverändert durchgereicht. Die Antwort des externen Dienstes kommt als Zeilen zurück; ein abgelehnter Aufruf ist ein 422, `functions.remote_refused`. Siehe [Schreiboperation einer externen Quelle](commands.md#a-remote-sources-write-operation).

**Kanalverwaltung.** Pro registrierter Quelle wird ein `grpc.aio.Channel` im Anwendungszustand gespeichert und für nachfolgende Anfragen wiederverwendet. Der alte Kanal wird geschlossen, bevor bei einer Aktualisierung ein neuer geöffnet wird.

**Aktualisierung.** POST an `/admin/grpc-remote/refresh/{source_id}`. Lädt das Proto erneut vom gespeicherten Pfad, kompiliert die Stubs neu und bringt die bereits registrierten Tabellen mit den Spalten, mit denen jede registriert wurde, auf den Stand des Protos. Es registriert keine neue Tabelle; eine dem Proto hinzugefügte Query-Methode bleibt im Angebot. Alternativ: PUT an `/admin/grpc-remote/{source_id}/proto` mit neuem `proto_text`, um das Proto inline zu aktualisieren.

**Einschränkungen.**

- Die Extraktion von Objekt-Unterfeldern ist auf eine Ebene beschränkt. Verschachtelte Message-Felder jenseits von Tiefe 1 werden nicht rekursiv expandiert.

---

### OpenAPI / REST

**Quelle hinzufügen.** POST an `/admin/openapi/register` mit einer Quellen-ID und einer Spezifikation, geladen aus einer lokalen Datei oder URL. Die Spezifikation wird geparst und zusammen mit der Quelle aufbewahrt; es wird weder eine Tabelle noch ein Command registriert. Die Antwort meldet `tables: 0` und `mutations: 0`, die angebotenen Zähler stehen in `available_tables` und `available_mutations`.

**Tabellen registrieren.** Registrieren Sie jede gewünschte GET-Operation über die Register-Table-Auswahl (`availableTables`, `availableColumns`, `registerTable`) und wählen Sie Spalten. Registrieren Sie jede Nicht-GET-Operation einzeln als Command auf der Seite „Commands“, aufgelistet durch `availableFunctions`; siehe [Schreiboperation einer externen Quelle](commands.md#a-remote-sources-write-operation). `PUT /admin/openapi/spec/{source_id}` speichert eine von Hand bearbeitete Spezifikation, registriert nichts und gibt `available_tables` und `available_mutations` zurück.

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

**Was die Quelle anbietet.** Jede GET-Operation der Spezifikation wird als Tabelle angeboten, außer ihr Antwortschema ist ein Skalartyp (`string`, `number`, `boolean`, `integer`); ein GET, das einen Skalar liefert, ist eine Funktion mit einer einzigen Spalte `value`. Jede andere Operation (POST, PUT, PATCH, DELETE) wird als Command angeboten, benannt nach ihrer `operationId`. Registriert nimmt sie die Pfadparameter der Operation und ein Argument `body` für den Request-Body, jeweils vom Typ `json`, und jedes andere Argument kommt in den Query-String. Siehe [Schreiboperation einer externen Quelle](commands.md#a-remote-sources-write-operation).

Priorität der Klassifizierung: `operation_overrides` (Payload) hat Vorrang vor `x-provisa-kind` (Spezifikationserweiterung), was wiederum Vorrang vor der GET-Heuristik hat. `operation_overrides` ist der empfohlene Override-Pfad; `x-provisa-kind` ist für Fälle gedacht, in denen die Spezifikation selbst die Klassifizierung tragen soll.

**Beziehungen bei der Registrierung.** `relationships` funktioniert wie bei den anderen Adaptern: gespeichert als manuell deklarierte Beziehungen und bei einer Aktualisierung behalten.

**Benennung von Tabellen.** Tabellen verwenden die `operationId` der Operation. Ist keine `operationId` definiert, erzeugt Provisa einen Slug aus `{method}_{path}`. Ein Alias wird abgeleitet, indem das führende Verb-Segment entfernt und das Substantiv in den Singular gesetzt wird (`findPetsByStatus` → `pet_by_status`).

**Typzuordnung.** JSON-Schema-Typen werden wie folgt auf Provisa-Typen abgebildet.

| JSON-Schema-Typ | Provisa-Typ |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**Parameter als native Filterspalten.** Pfad- und Abfrageparameter, die nicht schon Antwortfelder sind, werden zu Spalten mit `native_filter_type` gleich `path_param` oder `query_param` und dem Präfix `_nf_`. Hat ein Parameter denselben Namen wie ein Antwortfeld, werden seine Metadaten in den Eintrag dieser Spalte übernommen, sodass kein Duplikat entsteht.

**Auflösung des Antwortschemas.** Der Mapper prüft `responses.200`, dann `responses.2xx`, dann `responses.default`. Array-typisierte Antworten werden auf ihr Elementschema zurückgeführt. `$ref`-Referenzen werden eine Ebene tief aufgelöst.

**Objekt-Unterfelder.** Antwort-Properties vom `type: object` mit eigenen `properties` werden als `object_fields` auf der Spalte gespeichert. Diese Unterfelder sind in der SDL sichtbar und werden für die `jsonb`-Extraktion in Abfragen verwendet.

**Zwischenspeicherung von Antworten.** Ergebnisse von GET-Operationen werden von `pg_cache.py` in PostgreSQL zwischengespeichert. Jede Kombination von Anfrageparametern erhält eine eigene `_params_hash`-Gruppe. Zeilen eines bestimmten Hash werden ersetzt, sobald der TTL abläuft. Endpunkte mit Pfadparameter (`/pets/{id}`) überspringen den anfänglichen Massenabruf. Die Cache-Tabelle wird für die Schema-Introspektion leer erstellt und anschließend anfragenweise pro Primärschlüssel befüllt.

**Aktualisierung.** POST an `/admin/openapi/refresh/{source_id}`. Parst die Spezifikation erneut über `_load_and_register`, das nichts registriert: Es fügt weder eine Tabelle noch eine Spalte hinzu. Bestehende Governance-Regeln bleiben erhalten. Eine registrierte Tabelle behält ihre Spalten; sie wird über die Operation der aktualisierten Spec gelesen.

**Einschränkungen.**

- Die Extraktion von Objekt-Unterfeldern ist auf eine Ebene beschränkt. In `object_fields` verschachtelte Properties werden nicht rekursiv expandiert.
- Header- und Cookie-Parameter werden ignoriert; nur `path`- und `query`-Parameter werden registriert.
- Die Auflösung von `$ref` auf Spezifikationsebene ist bei Property-Schemas auf eine Ebene beschränkt; tief verschachtelte Komponentenreferenzen lassen sich möglicherweise nicht auflösen.

---

## Auswirkung der Registrierung einer externen Tabelle {: #impact-of-registering-a-remote-table }

Eine aus einer beliebigen Remote-Schema-Quelle registrierte Tabelle ist eine vollwertige Provisa-Tabelle. Zur Laufzeit wird sie in keiner Weise anders behandelt als eine lokal verbundene relationale Tabelle.

**Abfrageschnittstellen.** Die Tabelle ist sofort über GraphQL, SQL (pgwire oder direkt), Cypher (GQL), JSON:API und Arrow Flight abfragbar. Die Schemagenerierung synthetisiert `ColumnMetadata` für externe Tabellen, da diese keinen Katalog besitzen; die Typzuordnung wird beim Schema-Build angewendet.

**Sicherheitsmodell.** Alle fünf Governance-Schichten gelten:

1. Domänenzugriffskontrolle — die `domain_id` der Tabelle steuert, welche Rollen sie sehen können.
2. Sicherheit auf Zeilenebene (RLS) — auf der Tabelle konfigurierte Zeilenfilter werden unabhängig von der Schnittstelle in jede Abfrage injiziert.
3. Spaltensichtbarkeit — die `visible_to`-Liste jeder Spalte steuert die Feldfreigabe pro Rolle.
4. Spaltenmaskierung — Maskierungsregeln werden in Stufe 2 der Governance-Pipeline angewendet.
5. Prädikatschutz — maskierte Spalten werden in WHERE- und HAVING-Klauseln abgelehnt.

Ad-hoc-Abfragen gegen externe Tabellen sind allein aufgrund der Rechte des Benutzers erlaubt. Der Zugriff folgt überall demselben rechtebasierten Modell (Tabellen- und Spaltenrechte plus genehmigte Beziehungen), ohne Governance-Modus pro Tabelle.

**Beziehungs-Governance (V002).** Eine JOIN-Bedingung gegen externe Tabellen muss in einer SQL- oder Cypher-Abfrage zu einer registrierten, genehmigten Beziehung passen. GraphQL-Abfragen überspringen die Prüfung V002, weil in der SDL definierte Beziehungen von vornherein genehmigt sind. Siehe [docs/security.md](security.md#relationship-governance-v002).

**OBJECT-typisierte Spalten.** Wenn eine Spalte einem nicht governance-pflichtigen Inline-GQL-OBJECT oder einem OpenAPI-Objekttyp entspricht, ist ihr Provisa-Typ `jsonb`. Die Spalte speichert den vollständigen verschachtelten JSON-Blob. Sind Unterfelder deklariert (`gql_object_fields` oder `object_fields`), wird die `gql_object_columns`-Zuordnung beim Schema-Build befüllt. Der SQL-Generator verwendet diese Zuordnung, um `->>`-Extraktionsausdrücke für Unterfelder zu erzeugen, wenn eine Abfrage diese auswählt.

**Erforderliche Argumente als native Filterparameter.** Wurzel-Query-Felder mit Non-Null-Argumenten ohne Standardwert injizieren zusätzliche Spalten in die registrierte Tabelle. Diese Spalten tragen `native_filter_type: query_param`. Der Cypher-Übersetzer schreibt `WHERE n.id = $val` zu `WHERE n._nf_id = $val` um, und der GraphQL-Executor übernimmt sie als Variablen, die an den externen Endpunkt übergeben werden.

---

## Auswirkung der Erstellung einer abdeckenden Beziehung {: #impact-of-creating-a-covering-relationship }

Wenn ein Data Steward eine Beziehung zwischen zwei externen Tabellen (oder zwischen einer externen und einer lokalen Tabelle) registriert, wird diese Beziehung zum Join-Pfad, der zur Abfragezeit verwendet wird.

**Wie sich der Join durchsetzt.** Bei der Abfragekompilierung löst Provisa den Join-Pfad über die registrierte Beziehung auf. `source_column` und `target_column` der Beziehung werden zur Join-Bedingung im generierten SQL. Der Join ersetzt jeden pro-Tabelle-Aufruf an die externe Quelle, der andernfalls für den verbundenen Typ erforderlich wäre.

**Der rohe Blob ist in SQL nie sichtbar.** Die Spalte `breed` an `petstore__pets` lässt sich in einer SQL-Abfrage nicht als roher jsonb-Wert auswählen. Sobald eine Beziehung zwischen `petstore__pets` und `petstore__breeds` registriert ist, traversieren SQL-Abfragen den Join: `SELECT breed.name FROM petstore__pets` wird über den FK-Join aufgelöst. Ist keine Beziehung registriert, hat die Spalte aber deklarierte Unterfelder (`gql_object_fields`), werden SQL-Verweise auf Unterfelder in eine `->>`-Extraktion aus dem gespeicherten Blob umgeschrieben. Diesen Weg gibt es nur für nicht gesteuerte Inline-Typen; Felder mit gesteuertem Ziel fehlen ganz in der SDL und haben keinen Blob, aus dem man extrahieren könnte. Der rohe Blob selbst wird nie als bloßer Spaltenwert ausgegeben.

In der GraphQL-SDL hat ein nicht gesteuertes Inline-Feld vom Typ OBJECT den verschachtelten Objekttyp. Ob es zur Ausführungszeit ein Join oder eine Blob-Extraktion bedient, ändert an der SDL-Form nichts. Sobald der Kindtyp als eigene Tabelle registriert und damit gesteuert ist, gelten für ihn alle fünf Governance-Schichten unabhängig: eigene RLS-Regeln, Spaltensichtbarkeit, Maskierungsregeln, Prädikat-Guards und Domänenzugriffskontrolle. Die Blob-Extraktion umgeht all das, weil die Kinddaten eingebettet in der Elternzeile ankommen und nur die Regeln der Elterntabelle gelten. Um den Kindtyp im Detail zu steuern, registrieren Sie ihn als Tabelle und legen eine Beziehung an.

**`graphql_alias` auf der Beziehung.** Das Feld `graphql_alias` benennt das SDL-Feld, das die Beziehung auf dem übergeordneten Typ freigibt. Fehlt es, wird der Name aus dem `field_name` der Zieltabelle und der Kardinalität der Beziehung über `rel_field_name(target.field_name, cardinality)` abgeleitet.

**V002 auf dem Join-Pfad.** SQL- und Cypher-Abfragen, die die Beziehung durchlaufen, unterliegen der V002-Beziehungsgovernance. Die Beziehung muss registriert und genehmigt sein, damit der Join zulässig ist. Der GraphQL-Durchlauf über das SDL-Beziehungsfeld ist stets im Voraus genehmigt.

**Remote-managed-Flag.** Beziehungen, die während der Registrierung eines GraphQL Remote Schema automatisch erkannt werden, werden mit `remote_managed: True` gespeichert. Dies ist ein Metadaten-Marker; er verändert das Governance-Verhalten nicht.

---

## Verhalten reiner Typdefinitionen {: #type-def-only-behavior }

Nicht jeder Typ in einem externen Schema muss eine abfragbare Tabelle sein.

Wenn `root_table_ids` auf einem `SchemaInput` gesetzt ist, werden Tabellen, deren ID in dieser Menge fehlt, aus den Wurzel-Query-Feldern der generierten SDL ausgeschlossen. Sie bleiben als GraphQL-Typen vorhanden und sind über Beziehungsfelder auf Tabellen erreichbar, die selbst Wurzeleinträge besitzen.

Derselbe Mechanismus gilt, wenn das Schema nach Domänen gefiltert gebaut wird. Tabellen in Domänen, auf die die Rolle keinen Zugriff hat, sind reine Typdefinitionen: Ihre Typdefinition steht für das Traversieren von Beziehungen in der SDL, aber für sie wird kein Root-Abfragefeld erzeugt.

Eine Tabelle mit reiner Typdefinition:

- Besitzt kein Wurzel-Query-Feld, also können Clients sie nicht direkt namentlich abfragen.
- Ist über Beziehungsfelder auf Tabellen erreichbar, die Wurzeleinträge besitzen.
- Erscheint weiterhin als benannter Typ in der Schema-Introspektion.
- Behält alle Governance-Regeln bei, wenn auf Daten über eine Beziehung zugegriffen wird.

Eine Tabelle verschwindet samt Typdefinition nur dann ganz aus dem Schema, wenn ihre Registrierung gelöscht wird. Eine Tabelle zur reinen Typdefinition zu machen (indem ihre ID aus `root_table_ids` entfernt wird oder über den Domänenzugriffsfilter) entfernt den Typ nicht.

So können Stewards navigierbare Objektgraphen bereitstellen, in denen manche Typen nur über Traversierung erreichbar sind und sich nicht eigenständig abfragen lassen.
