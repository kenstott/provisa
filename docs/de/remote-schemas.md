# Externe Schemas

Eine Quelle für ein externes Schema (Remote Schema) verbindet eine externe API — GraphQL, gRPC oder REST (OpenAPI) — mit der semantischen Schicht von Provisa. Nach der Registrierung werden die Operationen der externen API zu vollwertigen Provisa-Tabellen und -Funktionen. (REQ-308, REQ-316, REQ-325) Jede Governance-Regel, jede Abfrageschnittstelle und jede Sicherheitsschicht gilt automatisch. (REQ-310, REQ-319, REQ-328) Der externe Dienst sieht die Governance-Regeln von Provisa niemals. (REQ-310, REQ-319, REQ-328)

---

## Drei Quellentypen

### GraphQL Remote Schema (REQ-307–313)

**Registrierung.** POST an `/admin/sources/graphql-remote` mit der Endpunkt-URL, einem Namespace und optionaler Authentifizierung. Provisa löst eine Standard-`__schema`-Introspektionsabfrage gegen den externen Endpunkt aus. (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

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

**Was automatisch erkannt wird.** Jedes Feld des externen Typs `Query`, das ein OBJECT zurückgibt, wird zu einer virtuellen Tabelle. Jedes Feld des externen Typs `Mutation` wird zu einer nachverfolgten Funktion. (REQ-308) [tool-verified: `provisa/graphql_remote/mapper.py:243–278`]

**Benennung von Tabellen.** Tabellen werden `{namespace}__{field_name}` benannt. Mit dem Namespace `petstore` und einem Query-Feld `pets`: Der Tabellenname lautet `petstore__pets`. (REQ-312) [tool-verified: `provisa/graphql_remote/mapper.py:250`]

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

**Erforderliche Argumente.** Wenn ein Wurzel-Query-Feld Non-Null-Argumente ohne Standardwert besitzt, werden diese zu Spalten mit `native_filter_type: query_param` auf der Tabelle (mit dem Präfix `_nf_` zum Zeitpunkt der Injektion). Der Executor übergibt sie als GraphQL-Variablen. (REQ-555) [tool-verified: `provisa/graphql_remote/mapper.py:110–120`, `provisa/api/app.py:1280–1303`]

**Automatisch erkannte Beziehungen.** Provisa durchsucht die OBJECT-typisierten Spalten jeder Tabelle. Wenn der referenzierte GQL-Typ ebenfalls als Tabelle in derselben Quelle registriert ist, wird eine Beziehung erzeugt. n:1-Beziehungen leiten Quell- und Zielspalten aus Namenskonventionen ab (`breedName` am Quelltyp → `name` am Zieltyp `Breed`). 1:n-Felder (LIST) erzeugen Beziehungen mit leeren Spaltenreferenzen — der Fremdschlüssel befindet sich auf der Zielseite. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutationen.** Mutation-Felder erzeugen nachverfolgte Funktionen mit Argumenttypen, die aus den Argumenten der Mutation abgeleitet werden, sowie ein `return_schema`, das aus dem Rückgabetyp der Mutation abgeleitet wird. (REQ-308) [tool-verified: `provisa/graphql_remote/mapper.py:261–278`]

**Aktualisierung.** POST an `/admin/sources/graphql-remote/{id}/refresh`. Führt eine erneute Introspektion des externen Schemas durch und aktualisiert die Registrierungen von Tabellen und Funktionen. Bestehende Governance-Regeln (RLS, Maskierung) bleiben erhalten. (REQ-311) [tool-verified: `provisa/api/admin/graphql_remote_router.py:217–257`]

**Einschränkungen.**

- Skalare und ENUM-Wurzel-Query-Felder (Rückgabetyp ist nicht OBJECT) werden zu nachverfolgten Funktionen, nicht zu virtuellen Tabellen. Ihr `return_schema` besteht aus einer einzelnen Spalte `value` des zugeordneten Skalartyps. [tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- Objektverschachtelung wird zum Registrierungszeitpunkt bis zu `graphql_remote.max_object_depth` (Standard: 5) aufgelöst. Sowohl die Auswahl beim externen Abruf als auch die Metadaten der Unterfelder werden bis zu dieser Tiefe erstellt; Felder jenseits des Limits werden nicht abgerufen und stehen für die SQL-Extraktion nicht zur Verfügung. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py:38–52`]
- LIST-typisierte verschachtelte OBJECT-Felder (z. B. `breed.awards: [Award]`) werden bis zu `graphql_remote.max_list_depth` Verschachtelungsebenen (Standard: 2) in die Abrufauswahl einbezogen. Innerhalb dieses Limits wird die Liste als `jsonb`-Array in der übergeordneten Spalte abgerufen, und die GQL-Auswahl injiziert `first: N`, wobei N `graphql_remote.max_list_items` (Standard: 100) entspricht, um die Array-Größe zu begrenzen. Jenseits von `max_list_depth` wird das LIST-Feld vollständig ausgeschlossen, um eine unbegrenzte Datenexpansion zu verhindern. In SQL wird auf das Array über `json_array_elements(column_name)` oder eine Index-Extraktion mit `->>` zugegriffen. Wenn der Elementtyp der Liste über eine eigene Wurzelabfrage verfügt, sollte er stattdessen als separate Tabelle registriert und eine Beziehung erstellt werden — der Join-Pfad ist effizienter und umgeht den Blob. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py:43–70`]
- Bei SQL-Abfragen werden nicht governance-pflichtige OBJECT-typisierte Spalten vollständig von der externen Quelle abgerufen (alle Unterfelder bis zur konfigurierten Tiefe) und als `jsonb` zwischengespeichert. Der Zugriff auf Unterfelder in SQL erfolgt über `->>`-Extraktion gegen den Blob; die externe Anfrage wird nicht auf die von der SQL-Abfrage ausgewählten Felder eingeschränkt. Wenn der Elementtyp der Liste keine Wurzelabfrage besitzt und die Blob-Darstellung nicht ausreicht, sollte die Abfrage direkt in GraphQL-SDL geschrieben werden — Provisa gibt die GQL-Feldauswahl originalgetreu wieder, sodass die externe Quelle genau die angeforderten Felder sieht. [tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- Falls der externe Server ein OBJECT-typisiertes Feld ablehnt, weil eine Unterfeldauswahl erforderlich ist (was nicht auftreten sollte, wenn `gql_selection` verfügbar ist), unternimmt der Executor einen erneuten Versuch ohne diese Felder, damit skalare Spalten dennoch zurückgegeben werden. [tool-verified: `provisa/graphql_remote/executor.py:76–80`]

---

### gRPC Remote Schema (REQ-322–329)

**Registrierung.** POST an `/admin/grpc-remote/register` mit der Serveradresse, einem Pfad oder einer URL zu einer `.proto`-Datei sowie optionaler TLS-Konfiguration.

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

**Was automatisch erkannt wird.** Jede `rpc`-Methode im Proto wird anhand von drei Signalen in Prioritätsreihenfolge als Query oder Mutation klassifiziert: (REQ-323) [tool-verified: `provisa/grpc_remote/mapper.py`]

1. **`method_overrides`** in der Registrierungs-Payload — `{"MethodName": "query"}` oder `{"MethodName": "mutation"}` hat Vorrang vor allem anderen.
2. **`server_streaming: true`** — der Server sendet einen Stream von Nachrichten; immer eine virtuelle Tabelle (sofern die Ausgabe kein Skalar ist).
3. **Die Ausgabemeldung besitzt ein wiederholtes Feld vom Typ Message** — z. B. wird `ListOrdersResponse { repeated Order items; }` als Listen-Wrapper behandelt und zu einer virtuellen Tabelle. Wiederholte skalare Felder (z. B. `repeated string tags`) lösen dies nicht aus — es handelt sich um Array-Eigenschaften einer einzelnen Entität, nicht um Zeilenquellen.

Methoden, die keinem dieser Signale entsprechen (unäres RPC mit Rückgabe einer einzelnen Entitätsmeldung oder jede skalare Ausgabe) werden zu nachverfolgten Funktionen.

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

**Mutation-Methoden (REQ-326).** Felder der Eingabemeldung werden zu Eingabeargumenten der Mutation. Das Schema der Ausgabemeldung wird zum `return_schema`. [tool-verified: `provisa/grpc_remote/executor.py:122–143`]

**Kanalverwaltung.** Pro registrierter Quelle wird ein `grpc.aio.Channel` im Anwendungszustand gespeichert und für nachfolgende Anfragen wiederverwendet. Der alte Kanal wird geschlossen, bevor bei einer Aktualisierung ein neuer geöffnet wird. (REQ-327) [tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**Aktualisierung.** POST an `/admin/grpc-remote/refresh/{source_id}`. Lädt das Proto erneut vom gespeicherten Pfad, kompiliert die Stubs neu und registriert Tabellen und Funktionen erneut. Alternativ: PUT an `/admin/grpc-remote/{source_id}/proto` mit neuem `proto_text`, um das Proto inline zu aktualisieren. (REQ-329) [tool-verified: `provisa/api/admin/grpc_remote_router.py:241–268`, `provisa/api/admin/grpc_remote_router.py:300–358`]

**Einschränkungen.**

- Die Extraktion von Objekt-Unterfeldern ist auf eine Ebene beschränkt. Verschachtelte Message-Felder jenseits von Tiefe 1 werden nicht rekursiv expandiert. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST (REQ-314–321)

**Registrierung.** Aufruf von `auto_register_openapi_source` mit einer Quellen-ID, einer geparsten Spezifikation und Verbindungsmetadaten. Die Spezifikation wird aus einer lokalen Datei oder URL geladen. (REQ-314) [tool-verified: `provisa/openapi/loader.py:30–55`, `provisa/openapi/register.py:249–264`]

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

**Was automatisch erkannt wird.** Jede GET-Operation in der Spezifikation wird zu einer virtuellen Tabelle, sofern ihr Antwortschema nicht ein Skalartyp ist (`string`, `number`, `boolean`, `integer`) — GET-Operationen mit skalarer Rückgabe werden stattdessen zu nachverfolgten Funktionen mit einer einzelnen Spalte `value`. Jede Nicht-GET-Operation (POST, PUT, PATCH, DELETE) wird zu einer nachverfolgten Funktion. (REQ-316, REQ-317)

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

**Aktualisierung (REQ-321).** Die Spezifikation erneut parsen und `auto_register_openapi_source` erneut aufrufen. Bestehende Governance-Regeln bleiben erhalten; Registrierungen werden per ON-CONFLICT-Upsert aktualisiert. [tool-verified: `provisa/openapi/register.py:249–264`]

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
