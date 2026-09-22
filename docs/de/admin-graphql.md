# Admin-GraphQL-API-Referenz

Die Admin-GraphQL-API ist Provisas Konfigurationsebene. Sie ist die API, die die Admin-Web-App für jede Verwaltungsoperation aufruft — Quellen anlegen, Tabellen registrieren, Beziehungen definieren, RLS-Regeln konfigurieren und alles andere, was das Modell formt.

**Mount-Point:** `POST /admin/graphql`

Dies ist nicht dieselbe API wie die Datenebene unter `/data/graphql`. Die Datenebene bedient Endnutzerabfragen über registrierte Domänen und wird durch das SDL unter `/data/sdl` beschrieben. Die Admin-API konfiguriert, wie dieses Schema aussieht und wer was sehen darf.

---

## Wie die UI mit dieser API spricht

Die Admin-Web-App nutzt Apollo Client, gerichtet auf `${API_BASE}/admin/graphql`. [tool-verified: `provisa-ui/src/apolloClient.ts:19`]

Jede Anfrage trägt ein Bearer-Token (bei jedem Aufruf frisch vom Auth-Provider geholt), einen `X-Org-Id`-Header bei Mandantenfähigkeit und einen `X-Env`-Header, wenn eine Branch-Umgebung bedient wird. [tool-verified: `provisa-ui/src/apolloClient.ts:24-42`]

Das Schema wird aus zwei `@strawberry.type`-Klassen zusammengesetzt — `Query` aus `schema_query.py` und `Mutation` aus `schema_mutation.py` — und in eine `ModelCommitExtension` eingehüllt, die jede Mutation gegen den aktuellen Umgebungs-Branch protokolliert (REQ-1524). [tool-verified: `provisa/api/admin/schema.py:44`]

---

## Autorisierung

**Dev-Modus:** Wenn keine Auth konfiguriert ist und jede Anfrage als anonymer Principal ankommt, werden alle Capability-Prüfungen übersprungen. Dies hält eine lokale Installation ohne Auth-Setup funktionsfähig. [tool-verified: `provisa/api/admin/capabilities.py:98-99`]

**Capability-Gates:** Produktions-Deployments erzwingen benannte Capabilities. Das jeweils erforderliche Recht für jedes Feld ist inline vermerkt. Der Aufruf einer Mutation ohne die erforderliche Capability löst einen `PermissionError` aus. Die Rolle des Plattform-Administrators umgeht alle Capability-Prüfungen (REQ-1297). [tool-verified: `provisa/api/admin/capabilities.py:80-110`]

**Domain-Gates:** Mehrere Mutationen prüfen zusätzlich die Domäne, zu der das Objekt gehört. Ein Aufrufer mit Geltungsbereich `sales` kann keine Tabelle in `finance` registrieren, keine RLS-Regel dafür einreihen und keine Beziehung erstellen, deren Quelltabelle in einer Domäne liegt, die er nicht hält (REQ-1530, REQ-1531). Sichten sind weiter eingeschränkt: Jede Tabelle, die das View-SQL liest, muss innerhalb der Domänen des Aufrufers liegen, da freihändiges SQL einem Mitglied sonst Zugriff auf Daten außerhalb seines Geltungsbereichs gäbe. [tool-verified: `provisa/api/admin/domain_guard.py:1-133`]

**Rollenvererbung:** Die Capabilities einer übergeordneten Rolle werden von untergeordneten Rollen geerbt (REQ-1677). `createRole` und `deleteRole` lehnen Zyklen ab und verhindern das Löschen einer Rolle, die Erben hat.

---

## Gemeinsamer Rückgabetyp

Die meisten Mutationen geben `MutationResult` zurück. [tool-verified: `provisa/api/admin/types.py:1181-1188`]

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization (REQ-1350)
}
```

Wenn eine Mutation fehlschlägt, ist `success` `false` und `message` trägt den englischen Grund. `code` ist ein stabiler Bezeichner, den die UI zum Rendern einer lokalisierten Meldung nutzt.

---

## Queries

### Quellen

#### `sources → [SourceType!]!`

Alle registrierten Datenquellen. [tool-verified: `provisa/api/admin/schema_query.py:327-331`]

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl preferMaterialized
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc { bootstrapServers topicPrefix schemaRegistryUrl consumerGroupId }
  }
}
```

`passwordRef` ist eine `${secret:NAME}`-Referenz in den Org-Vault — niemals das Klartext-Credential. [tool-verified: `provisa/api/admin/types.py:105`]

#### `source(id: String!) → SourceType`

Eine einzelne Quelle nach ID. Gibt `null` zurück, wenn nicht gefunden. [tool-verified: `provisa/api/admin/schema_query.py:334-339`]

#### `availableSchemas(sourceId: String!) → [String!]!`

In einer Quelle sichtbare Schemas, gefiltert um Provisa-interne auszuschließen. Nutzt zuerst native Introspection; fällt zurück auf den Engine-Katalog, wenn der Quellentyp keinen direkten Pool hat. [tool-verified: `provisa/api/admin/schema_query.py:628-667`]

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

Tabellen in einem Schema einer Quelle, mit ihren Kommentaren. Für OpenAPI-Quellen werden GET-Operationen zurückgegeben, deren Antwort ein Array oder ein Paginierungs-Wrapper ist. Für GraphQL-Quellen werden Query-Felder zurückgegeben, die eine Liste liefern. Für gRPC werden serverseitig streamende RPCs zurückgegeben. [tool-verified: `provisa/api/admin/schema_query.py:669-731`]

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

Spaltennamen für eine Tabelle im Engine-Katalog. Für GovData-Quellen wird ein separater Resolver genutzt. [tool-verified: `provisa/api/admin/schema_query.py:845-866`]

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

Spaltennamen mit Datentypen, Kommentaren, nativen Filtertypen und Primärschlüssel-Kennzeichnungen. Für OpenAPI-Quellen wird die Form aus dem Antwortschema und den Parametern der Operation abgeleitet. [tool-verified: `provisa/api/admin/schema_query.py:869-876`]

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

Nicht-GET-Operationen einer OpenAPI-Quelle (POST, PUT, PATCH, DELETE). Gibt für Nicht-OpenAPI-Quellen eine leere Liste zurück. [tool-verified: `provisa/api/admin/schema_query.py:822-843`]

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

Vorschau, was ein Datei-Connector-Crawl entdecken würde — Dateien, Tabellen und Spalten — bevor eine Quelle erstellt wird. Die HTTP-only-Einstellungen (`simpleLinks`, `sameDomain`, `excludePattern`) werden für lokale, S3-, FTP- und SFTP-Wurzeln ignoriert. (REQ-1785) [tool-verified: `provisa/api/admin/schema_query.py:733-790`]

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

Gibt den Alias zurück, der bei der Registrierung von `tableName` in `domainId` aus `sourceId` zu verwenden ist. Gibt einen einfachen Snake-Case-Alias zurück, wenn kein Konflikt besteht, oder einen quellpräfixierten Alias (`sqlite_b_orders`), wenn der effektive Name bereits von einer anderen Quelle in derselben Domäne belegt ist. [tool-verified: `provisa/api/admin/schema_query.py:879-922`]

---

### Tabellen

#### `tables → [RegisteredTableType!]!`

Alle registrierten Tabellen, jeweils mit ihrer vollständigen Spaltenliste. Die Spaltensichtbarkeit in der Antwort respektiert die `table_registration`-Capability des Aufrufers — `canDeployToDb` ist davon abhängig, ob der Aufrufer dieses Recht hält. (REQ-016, REQ-021, REQ-042) [tool-verified: `provisa/api/admin/schema_query.py:502-536`]

Jeder `RegisteredTableType` exponiert berechnete Unterfelder:

- **`refreshPolicySummary → RefreshPolicySummaryType`** — die effektive Refresh-/Serving-Richtlinie als Klartext, serverseitig aus derselben Planner-Auflösung abgeleitet, die die Engine nutzt. Gibt während des Starts `null` zurück. (REQ-1143) [tool-verified: `provisa/api/admin/types.py:319-327`]
- **`graphqlFieldName → String`** — der Feldname, den diese Tabelle im kompilierten Schema der Datenebene hat, damit das Data-Product-Panel ein lauffähiges Beispiel erstellen kann, ohne den Namensgebungs-Algorithmus zu duplizieren. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:330-339`]
- **`dqDataset → String`** — diese Tabelle als Data-Quality-Vertrags-Dataset, in der Form, die der Checker scannt. (REQ-1443) [tool-verified: `provisa/api/admin/types.py:374-387`]
- **`productId → String`** — das Datenprodukt, zu dem diese Tabelle gehört. Eine DQ-Checker-Tabelle erbt das Produkt der Tabelle, deren Vertrag sie scannt. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:342-372`]

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

Vorschau der effektiven Refresh-/Serving-Zusammenfassung für *Entwurfs*-(ungespeicherte) Tabellenparameter, sodass sich die Zusammenfassung am Formularanfang bei Feldänderungen aktualisiert, ohne etwas zu persistieren. Gleiche Ableitung wie `refreshPolicySummary` oben. (REQ-1143) [tool-verified: `provisa/api/admin/schema_query.py:947-979`]

Argumente: `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `preferMaterialized`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

Artefakte, die eine anstehende Alias-Umbenennung oder ein Spalten-Drop brechen würde. Beratend — die Admin-UI zeigt dies vor dem Speichern an, und der Administrator entscheidet. Muss *vor* dem Speichern aufgerufen werden, da die Abhängigen gegen den aktuell von der Spalte getragenen exponierten Namen verfasst wurden. (REQ-1484) [tool-verified: `provisa/api/admin/schema_query.py:1301-1331`]

---

### Beziehungen

#### `relationships → [RelationshipType!]!`

Alle benutzerdefinierten Beziehungen (schließt automatisch generierte `gql_auto__`-Einträge und die synthetischen `meta:%`-Einträge aus, die vom ERD genutzt werden). [tool-verified: `provisa/api/admin/schema_query.py:539-569`]

#### `allRelationships → [RelationshipType!]!`

Wie `relationships`, umfasst aber `meta:%`-synthetische Einträge. Wird vom Graph-ERD genutzt, das jede Kante inklusive der impliziten `HAS_TABLE`-Verknüpfungen zwischen Datentabellen und der Metadaten-Registry anzeigen muss. [tool-verified: `provisa/api/admin/schema_query.py:572-601`]

Jeder `RelationshipType` exponiert:

- **`autoSuggested → Boolean`** — ob die Beziehung durch FK-Analyse vorgeschlagen wurde (`id` beginnt mit `fk__`). [tool-verified: `provisa/api/admin/types.py:523-525`]
- **`physicalName → String`** — der Name der Beziehung auf der SQL- und gRPC-Ebene (der `?include=`-Parameter). Serverseitig abgeleitet; Clients dürfen den GraphQL-Alias nicht transliterieren. (REQ-471, REQ-1417) [tool-verified: `provisa/api/admin/types.py:527-536`]

---

### Domänen, Rollen & Benutzer

#### `domains → [DomainType!]!`

Alle Domänen in der Mandanten-Datenbank des aktiven Orgs. Die Mandanten-Datenbank ist auf Schema-Ebene isoliert, sodass die Domänenliste eines Org-Admins nur die Zeilen seines eigenen Orgs enthält. (REQ-021, REQ-042, REQ-1293) [tool-verified: `provisa/api/admin/schema_query.py:342-357`]

#### `roles → [RoleType!]!`

Für den Aufrufer sichtbare Rollen. Ein Admin sieht jede Rolle; ein Nicht-Admin sieht nur Rollen ohne `org_id` oder Rollen, die zu seinem Org gehören. (REQ-042, REQ-059, REQ-060, REQ-215) [tool-verified: `provisa/api/admin/schema_query.py:603-618`]

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

Löst Rollen-IDs oder Benutzer-IDs zu einzelnen Benutzern auf. Wird genutzt, um `DataProduct.ownerRole`, `Domain.steward` und `Column.visibleTo` zu einer menschenlesbaren Liste zu erweitern. Unbekannte Referenzen werden unverändert zurückgegeben, damit die UI die rohe ID statt nichts anzeigt. [tool-verified: `provisa/api/admin/schema_query.py:388-444`]

---

### RLS-Regeln

#### `rlsRules → [RLSRuleType!]!`

Alle Row-Level-Security-Regeln. Das zugrundeliegende Repository entschlüsselt `filterExpr` an der Grenze. (REQ-041, REQ-402, REQ-686) [tool-verified: `provisa/api/admin/schema_query.py:621-625`]

---

### Datenprodukte

#### `dataProducts → [DataProductType!]!`

Alle Datenprodukte. Erfordert die Capability `data_product_read`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_query.py:360-386`]

---

### Tags

#### `tags → [TagType!]!`

Alle Tag-Definitionen, einschließlich ihrer pro Tag zulässigen Parameterwerte. (REQ-1373, REQ-1467) [tool-verified: `provisa/api/admin/schema_query.py:447-475`]

#### `tagAssignments → [TagAssignmentType!]!`

Alle Tag-Zuweisungen über Quellen, Tabellen, Spalten und Beziehungen hinweg. (REQ-1377) [tool-verified: `provisa/api/admin/schema_query.py:478-499`]

---

### Materialisierte Sichten

#### `mvList → [MVType!]!`

Alle materialisierten Sichten mit ihrem Laufzeitstatus: aktiviert/deaktiviert, Zeitstempel der letzten Aktualisierung, Zeilenzahl und letzter Fehler. [tool-verified: `provisa/api/admin/schema_query.py:927-944`]

---

### Cache

#### `cacheStats → CacheStatsType`

Cache-Statistiken. Gibt `storeType: "redis"` mit vollständigen Betriebsmetriken zurück, wenn Redis konfiguriert ist, `storeType: "memory"` für den eingebetteten fakeredis-Store und `storeType: "noop"`, wenn kein Cache konfiguriert ist. [tool-verified: `provisa/api/admin/schema_query.py:1106-1140`]

#### `cacheTableStats → [CacheTableStatType!]!`

Anzahl gecachter Einträge pro Tabelle. Leer, wenn kein Cache-Store konfiguriert ist. [tool-verified: `provisa/api/admin/schema_query.py:1143-1148`]

#### `hotTables → [HotTableStatType!]!`

Tabellen, von denen Provisa eine Kopie vorhält, über zwei Stufen: `hot` (in den Response-Store gespiegelt für JOIN-Inlining) und `warm` (als Iceberg-Kopie gelandet). Eine Tabelle ist in höchstens einer Stufe (REQ-241). [tool-verified: `provisa/api/admin/schema_query.py:1151-1175`]

#### `materializeStoreInfo → MaterializeStoreInfoType`

Identität des dauerhaften Materialisierungs-Stores: Engine-Name, Store-DSN-Referenz, MV-Anzahl und ob der Store instanzlokal ist (eine lokale Datei wie DuckDB oder SQLite, was bedeutet, dass jede Instanz hinter einem Load Balancer ihre eigene Kopie hält). [tool-verified: `provisa/api/admin/schema_query.py:1178-1189`]

---

### Systemzustand

#### `systemHealth → SystemHealthType`

Engine-Verbindungsstatus, Worker-Pool-Anzahlen, Metadaten-DB-Pool-Zustand, Cache-Modus und Liveness jedes Protokoll-Listeners (pgwire, gRPC, Arrow Flight, Bolt). [tool-verified: `provisa/api/admin/schema_query.py:1194-1198`]

```graphql
query {
  systemHealth {
    engineConnected engineWorkerCount engineActiveWorkers
    metadataPoolSize metadataPoolFree metadataDialect
    cacheMode cacheConnected mvRefreshLoopRunning
    protocols { name status port }
  }
}
```

---

### Geplante Tasks

#### `scheduledTasks → [ScheduledTaskType!]!`

Geplante Trigger aus der Konfiguration mit Laufzeitstatus. Jeder Eintrag trägt seinen Cron-Ausdruck, `kind` (`webhook` oder `sql`), ob er aktuell aktiviert ist, den Zeitstempel des letzten Laufs (in diesem Release immer `null` — wird vom Scheduler verfolgt) und die nächste geplante Laufzeit von APScheduler. [tool-verified: `provisa/api/admin/schema_query.py:1203-1245`]

---

### Data Quality

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

Parst rohen Vertragstext in die editierbaren Zeilen des Builder-Panels. Wird bei jeder Bearbeitung aufgerufen; ein Parse-Fehlschlag kommt als `error` zurück statt als GraphQL-Fehler, da halbgeschriebener Text normal ist, während der Betreiber tippt. (REQ-1443 Klausel 7) [tool-verified: `provisa/api/admin/schema_query.py:1007-1022`]

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

Die Checks, die `checker` anbietet, eingeschränkt auf die Spalten von `dataset`. Das Dataset ist das beobachtete Ziel des Vertrags, aufgelöst auf dieselbe Weise wie der Scanner es auflöst — sodass die angebotenen Checks zu den Spalten passen, die der Checker wirklich sehen wird. (REQ-1443 Klausel 7) [tool-verified: `provisa/api/admin/schema_query.py:1025-1050`]

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

Der Text eines Checks aus den Editoren des Panels. Serverseitig, weil der Dialekt eine Implementierung hat; ein per Builder erstellter Check und ein handgetippter müssen ununterscheidbar sein. (REQ-1443 Klausel 7) [tool-verified: `provisa/api/admin/schema_query.py:1053-1075`]

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

Serialisiert bearbeitete Check-Zeilen zurück in Vertragstext. Das Inverse von `dqContractParse`. Serverseitig aus demselben Grund: Das Panel darf keinen Text ausgeben, den der Checker ablehnen würde. (REQ-1443 Klausel 7) [tool-verified: `provisa/api/admin/schema_query.py:1078-1101`]

---

### Quellenvorschauen

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

Vorschau einer Cypher-Projektion auf einer Neo4j-Quelle: bis zu fünf Zeilen und die Spaltentypen, die die Registrierung tragen wird. Fehlschläge kommen als `error` zurück. (REQ-1670) [tool-verified: `provisa/api/admin/schema_query.py:984-992`]

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

Vorschau eines SPARQL-SELECT auf einer SPARQL-Quelle: bis zu fünf Zeilen, alle Spalten als Text. (REQ-1683) [tool-verified: `provisa/api/admin/schema_query.py:995-1002`]

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

Live-Prüfung gegen die Kaggle-API. Gibt nur `true` zurück, wenn das Token authentifiziert. Unterstützt den Token-Gate-Schritt im Kaggle-Quellen-Formular. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:793-799`]

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

Durchsucht Kaggles vollständigen öffentlichen Dataset-Katalog. Token-gated. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:802-819`]

---

### Kalender

#### `calendars → [CalendarType!]!`

Alle registrierten Snapshot-Grenz-Kalenderversionen. Speist den Konfigurationspicker für Snapshot-Zeitpläne und bestätigt, welche Kalender eine periodische MV referenzieren darf. (REQ-962) [tool-verified: `provisa/api/admin/schema_query.py:218-242`]

---

### Metriken

#### `metrics → [MetricType!]!`

Alle regierten Metrikdefinitionen. Fakt-abgeleitete Metriken tragen `fromFact`. (REQ-1317, REQ-1320) [tool-verified: `provisa/api/admin/schema_query.py:245-264`]

---

### Schema-Version

#### `schemaVersion → String!`

SHA-256-Hash des aktuellen Schemazustands (Domänen, Tabellen-IDs, Beziehungs-IDs). Der Apollo-Client liest dies aus dem `X-Schema-Version`-Antwort-Header und ruft alle aktiven Abfragen erneut ab, wenn er sich ändert. [tool-verified: `provisa/api/admin/schema_query.py:291-324`]

---

### KI-Hilfsfunktionen

#### `generateTableDescription(tableId: String!) → String!`

Nutzt das konfigurierte LLM, um eine ein- bis zweisätzige Beschreibung für eine registrierte Tabelle zu generieren. Speichern Sie die Tabelle zuerst; der Aufruf hierfür auf einer ungespeicherten Tabelle gibt eine Anleitungsmeldung zurück. [tool-verified: `provisa/api/admin/schema_query.py:1250-1298`]

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

Nutzt das konfigurierte LLM, um eine einsätzige Beschreibung für eine einzelne Spalte zu generieren. [tool-verified: `provisa/api/admin/schema_query.py:1334-1383`]

---

### Erstellungsanfragen

#### `creationRequests → [CreationRequestType!]!`

Ausstehende Erstellungsanfragen, sichtbar für Aufrufer mit der relevanten Erstellungs-Capability. Wird genutzt, wenn ein Mitglied ohne `create_relationship` oder `create_view` eine Anfrage einreicht, die ein Rechteinhaber genehmigen muss. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_query.py:267-289`]

---

## Mutationen

### Quellen

#### `createSource(input: SourceInput!) → MutationResult`

Registriert eine neue Datenquelle. Validiert die Verbindung vor der Persistierung — eine abgelehnte Quelle hinterlässt keinen Vault-Eintrag. Speichert Zugangsdaten im Org-Vault und protokolliert die Referenz; der Klartext landet nie in der Datenbank. (REQ-012, REQ-013) Erfordert die Capability `source_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:616-781`]

#### `updateSource(input: SourceInput!) → MutationResult`

Aktualisiert die Verbindungsdetails, Beschreibung und Konfiguration einer bestehenden Quelle. Baut den pgwire-Endpunkt für Datei-/SharePoint-Quellen ab und neu auf, sodass eine Pfadänderung sofort wirksam wird. Erfordert `source_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:922-1073`]

#### `deleteSource(id: String!) → MutationResult`

Entfernt eine Quelle und ihren Vault-Eintrag. Löscht den Engine-Katalog und baut Schemas neu auf. [tool-verified: `provisa/api/admin/schema_mutation.py:1101-1132`]

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

Benennt eine Quellen-ID um. [tool-verified: `provisa/api/admin/schema_mutation.py:1076-1098`]

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

Aktiviert oder deaktiviert das Caching von Abfrageergebnissen für eine Quelle und setzt die TTL in Sekunden. [tool-verified: `provisa/api/admin/schema_mutation.py:2327-2350`]

#### `updateSourcePreferMaterialized(sourceId: String!, preferMaterialized: Boolean!) → MutationResult`

Erzwingt (oder löst) materialisierte Föderation für alle Tabellen einer Quelle. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2379-2402`]

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Markiert eine Quelle als lastgeschützt (nur geplante Aktualisierung). Erfordert mindestens ein Gate — Off-Peak-Fenster, Cache-TTL-Kadenz oder ein prüfendes Änderungssignal — sonst schlägt der Aufruf fehl. (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2431-2480`]

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

Setzt die GraphQL-Namenskonvention pro Quelle. [tool-verified: `provisa/api/admin/schema_mutation.py:2595-2619`]

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

Legt fest, welche Domänen eine Quelle nutzen dürfen (leere Liste = uneingeschränkt). Erfordert `source_registration`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2622-2658`]

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

Lädt ein Kaggle-Dataset herunter und entpackt es lokal. Gibt den Pfad des bereitgestellten Verzeichnisses zurück; der Aufrufer erstellt danach eine Quelle vom Typ `files`, die darauf zeigt. Bundles mit SQLite werden vollständig abgelehnt. Erfordert `source_registration`. (REQ-1780–1782) [tool-verified: `provisa/api/admin/schema_mutation.py:784-824`]

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

Holt das Dataset einer von Kaggle abgeleiteten Quelle an Ort und Stelle erneut ab. Überspringt den Download, wenn Kaggle nichts Neueres hat als das, was auf der Festplatte liegt. Erfordert `source_registration`. (REQ-1787) [tool-verified: `provisa/api/admin/schema_mutation.py:827-920`]

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

Führt `ANALYZE` auf allen registrierten Tabellen einer Quelle aus. Verbessert Join-Reihenfolge- und Broadcast-Entscheidungen für föderierte Abfragen. (REQ-276) [tool-verified: `provisa/api/admin/schema_mutation.py:2944-3008`]

---

### Tabellen

#### `registerTable(input: TableInput!) → MutationResult`

Registriert eine neue Tabelle (oder Sicht) in einer Domäne. Erfordert die Capability `table_registration` und Mitgliedschaft in der Zieldomäne. Ein Aufrufer ohne `create_relationship`, der eine Sicht einreicht, wird als Erstellungsanfrage für einen Rechteinhaber zur Genehmigung eingereiht. (REQ-013, REQ-016, REQ-252, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:1714-1718`]

#### `updateTable(input: TableInput!) → MutationResult`

Aktualisiert Alias, Beschreibung, Spaltenmetadaten, MV-Einstellungen und Live-Delivery-Konfiguration einer bestehenden Tabelle. (REQ-016, REQ-020) Erfordert `table_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:1858-1995`]

#### `deleteTable(id: Int!) → MutationResult`

Löscht eine registrierte Tabelle. Ermittelt die Domäne der Tabelle für das Domain-Gate vor dem Löschen. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1998-2029`]

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

Überschreibt die Cache-TTL für eine Tabelle. [tool-verified: `provisa/api/admin/schema_mutation.py:2353-2376`]

#### `updateTablePreferMaterialized(tableId: Int!, preferMaterialized: Boolean) → MutationResult`

Überschreibt materialisierte Föderation für eine Tabelle. `null` = erbt den Quellenstandard. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2405-2428`]

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Überschreibt den Lastschutz für eine Tabelle. `null` für `loadProtected` erbt den Quellenstandard. Validiert die effektive (Tabelle → Quelle) Gate-Kombination. (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2483-2566`]

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

Setzt die GraphQL-Namenskonvention pro Tabelle. [tool-verified: `provisa/api/admin/schema_mutation.py:2661-2685`]

#### `deployViewToDb(tableId: Int!) → MutationResult`

Befördert eine virtuelle Provisa-Sicht zu einer echten Datenbank-Sicht auf ihrer zugrundeliegenden nativen Quelle. [tool-verified: `provisa/api/admin/schema_mutation.py:3058-3060`]

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

Berechnet die gelandeten Zeilen einer Tabelle bei Bedarf neu und umgeht dabei das normale Änderungs-Gate. `reason` ist eine erforderliche Audit-Anmerkung. Wird für live-föderierte Tabellen abgelehnt (keine gelandeten Zeilen). (REQ-968) [tool-verified: `provisa/api/admin/schema_mutation.py:2689-2782`]

#### `invalidateFileSource(tableId: Int!) → MutationResult`

Erzwingt, dass der nächste Zugriff auf eine SQLite-Datei-Connector-Tabelle erneut von der Festplatte synchronisiert. [tool-verified: `provisa/api/admin/schema_mutation.py:2877-2880`]

#### `registerEntity(input: EntityInput!) → MutationResult`

Zucker für die Registrierung einer Dimensions-/Hub-Entität. Reduziert sich zu einer (bitemporalen, wenn historisiert) MV und ruft `registerTable` auf. (REQ-1164) [tool-verified: `provisa/api/admin/schema_mutation.py:1721-1725`]

#### `registerFact(input: FactInput!) → MutationResult`

Zucker für die Registrierung eines Star-Schema-Fakts. Reduziert sich zu einer aggregierten MV, erstellt Dimensionsbeziehungen und registriert Fakt-Kennzahlen automatisch als regierte Metriken. (REQ-1164, REQ-1320) [tool-verified: `provisa/api/admin/schema_mutation.py:1728-1776`]

---

### Beziehungen

#### `upsertRelationship(input: RelationshipInput!) → MutationResult`

Erstellt oder aktualisiert eine Beziehung. Das Gate prüft die Domäne der Quelltabelle (nicht die des Ziels). Eine domänenübergreifende Kante wird mit `needsReview: true` gespeichert. Ein Aufrufer ohne `create_relationship` wird als Erstellungsanfrage eingereiht. Junction-(Viele-zu-viele-)Kanten erfordern übereinstimmende Längen der Schlüssellisten. (REQ-019, REQ-020, REQ-366, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:2297-2300`]

#### `deleteRelationship(id: String!) → MutationResult`

Löscht eine Beziehung nach ID und baut Schemas neu auf. [tool-verified: `provisa/api/admin/schema_mutation.py:2303-2322`]

---

### Domänen

#### `createDomain(input: DomainInput!) → MutationResult`

Erstellt eine Domäne. Reservierte Segmentwörter (`tables`, `relationships` und andere URI-Pfadsegmente) werden abgelehnt, ebenso das Wildcard-Literal `*`. Erfordert die Capability `org_settings`. (REQ-021, REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1135-1189`]

#### `deleteDomain(id: String!) → MutationResult`

Löscht eine Domäne. Erfordert `org_settings`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1192-1213`]

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

Setzt die globale GraphQL-Namenskonvention und baut Schemas für alle Rollen neu auf. Nur erkannte Konventionsnamen werden akzeptiert. (REQ-253, REQ-416) [tool-verified: `provisa/api/admin/schema_mutation.py:2571-2592`]

---

### Rollen

#### `createRole(input: RoleInput!) → MutationResult`

Erstellt oder ersetzt eine Rolle mit Capabilities, Domänenzugriff, optionalen Rate Limits und einer optionalen übergeordneten Rolle. Validiert, dass die übergeordnete Rolle existiert und die Elternkette zyklenfrei ist. Erfordert `user_management`. (REQ-042, REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:1657-1712`]

#### `deleteRole(id: String!) → MutationResult`

Löscht eine Rolle. Schlägt fehl, wenn andere Rollen von ihr erben — diese müssen zuerst umgehängt werden. Erfordert `user_management`. (REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:2032-2063`]

---

### RLS-Regeln

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

Erstellt oder aktualisiert eine Row-Level-Security-Regel. Der Filterausdruck wird beim Speichern gegen die Spalten der Ziel-Tabelle oder -Domäne validiert, sodass eine Regel, die der Administrator nicht abfragen kann, mit dem Grund abgelehnt wird, statt zur Abfragezeit stillschweigend zu scheitern. Erfordert `masking_config`. (REQ-041, REQ-402, REQ-1531, REQ-1676) [tool-verified: `provisa/api/admin/schema_mutation.py:2066-2136`]

Ziele schließen sich gegenseitig aus: Setzen Sie `tableId` für eine Regel auf Tabellenebene, `domainId` für eine Regel auf Domänenebene, oder `actionName` für eine überwachte Funktion/Webhook. (REQ-1679)

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

Löscht eine RLS-Regel. Erfordert `masking_config`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2139-2178`]

---

### Datenprodukte

#### `createDataProduct(input: DataProductInput!) → MutationResult`

Erstellt oder ersetzt ein Datenprodukt. Erfordert die Capability `data_product_rw`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1216-1259`]

#### `deleteDataProduct(id: String!) → MutationResult`

Löscht ein Datenprodukt. Erfordert `data_product_rw`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1262-1285`]

---

### Tags

#### `upsertTag(input: TagInput!) → MutationResult`

Erstellt oder aktualisiert eine Tag-Definition. System-Tags und abgeleitete Tags können nicht neu definiert werden. `appliesTo` muss eine nicht-leere Teilmenge von `["source", "table", "column", "relationship", "command"]` sein. (REQ-1373, REQ-1375) [tool-verified: `provisa/api/admin/schema_mutation.py:1288-1361`]

#### `deleteTag(id: String!) → MutationResult`

Löscht einen Tag. Lehnt System- und abgeleitete Tags ab. (REQ-1373) [tool-verified: `provisa/api/admin/schema_mutation.py:1364-1393`]

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

Weist einer Quelle, Tabelle, Spalte, Beziehung oder einem Command einen Tag zu. Erzwingt die Feldrichtlinien des Tags (`reason_policy`, `expires_policy`) und validiert bei parametrisierten Tags den Parameterwert gegen die zulässige Liste des Tags. (REQ-1376, REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1396-1527`]

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

Entfernt eine Tag-Zuweisung. (REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1530-1564`]

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

Fügt einen zulässigen Parameterwert für einen parametrisierten Tag hinzu oder beschreibt ihn neu. Die Liste zulässiger Werte ist geschlossen: Jede Zuweisung muss einen Wert daraus benennen. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1567-1616`]

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

Entfernt einen zulässigen Wert. Wird abgelehnt, solange eine Zuweisung ihn noch trägt, da diese Zuweisungen dann einen Typ benennen würden, den die Liste nicht mehr zulässt. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1619-1655`]

---

### Metriken

#### `upsertMetric(input: MetricInput!) → MutationResult`

Erstellt oder ersetzt eine regierte Metrikdefinition. Der Ausdruck muss unter sqlglot parsen und mindestens eine Aggregatfunktion enthalten. Regeneriert alle metrik-komponierten Sichten, die diese Metrik referenzieren. Erfordert `table_registration`. (REQ-1317, REQ-1318) [tool-verified: `provisa/api/admin/schema_mutation.py:1779-1831`]

#### `deleteMetric(name: String!) → MutationResult`

Löscht eine regierte Metrik. Baut Schemas neu auf. Erfordert `table_registration`. (REQ-1317) [tool-verified: `provisa/api/admin/schema_mutation.py:1834-1856`]

---

### Kalender

#### `createCalendar(input: CalendarInput!) → MutationResult`

Erstellt oder ersetzt einen versionierten Snapshot-Grenz-Kalender. Validiert durch Konstruktion des In-Memory-`Calendar` vor der Persistierung — schlägt fehl bei unbekanntem Basissystem, falscher Zeitzone oder falschem Fiskalanker. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:525-579`]

#### `deleteCalendar(name: String!) → MutationResult`

Löscht einen Kalender (alle Versionen). Wird abgelehnt, wenn eine materialisierte Sicht ihn referenziert. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:582-613`]

---

### Materialisierte Sichten

#### `refreshMv(mvId: String!) → MutationResult`

Löst eine manuelle Aktualisierung einer materialisierten Sicht aus. Koordiniert über die Flotte hinweg, wenn der MV-Konsistenzmodus `shared` ist. (REQ-133, REQ-158, REQ-879) [tool-verified: `provisa/api/admin/schema_mutation.py:2787-2813`]

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

Aktiviert oder deaktiviert eine materialisierte Sicht. [tool-verified: `provisa/api/admin/schema_mutation.py:2816-2839`]

---

### Cache

#### `purgeCache → MutationResult`

Löscht alle gecachten Abfrageergebnisse. [tool-verified: `provisa/api/admin/schema_mutation.py:2844-2858`]

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

Löscht gecachte Ergebnisse für eine Tabelle. [tool-verified: `provisa/api/admin/schema_mutation.py:2861-2875`]

---

### Geplante Tasks

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

Erstellt einen geplanten Trigger — entweder einen Webhook-Aufruf oder eine SQL-Anweisung — und registriert ihn live in APScheduler. `kind` ist `"webhook"` oder `"sql"`. (REQ-1003, REQ-1004) [tool-verified: `provisa/api/admin/schema_mutation.py:2923-2936`]

#### `deleteScheduledTask(taskId: String!) → MutationResult`

Entfernt einen geplanten Trigger aus der Konfiguration und dem laufenden Scheduler. (REQ-1003) [tool-verified: `provisa/api/admin/schema_mutation.py:2939-2941`]

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

Aktiviert oder deaktiviert einen geplanten Task in der Konfigurationsdatei. [tool-verified: `provisa/api/admin/schema_mutation.py:2885-2920`]

---

### Data Quality

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

Führt einen Vertrag gegen die Live-Tabelle aus und gibt Ergebnisse zurück, ohne etwas zu landen. Eine Mutation statt einer Query, weil dies einen echten Scan kostet. Was sie beweist, ist, ob sich der Dataset-Bezeichner zur regierten Tabelle auflöst, die der Betreiber meint. (REQ-1443 Klausel 7) [tool-verified: `provisa/api/admin/schema_mutation.py:463-487`]

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

Löst sofort den Poll-Job einer Checker-Tabelle aus. Landet Zeilen auf normalem Weg, sodass Ergebnisse persistieren und die DQ-Historie den neuen Scan anzeigt. (REQ-1443) [tool-verified: `provisa/api/admin/schema_mutation.py:490-522`]

---

### Schema-Wartung

#### `rebuildSchemas → MutationResult`

Baut das In-Memory-Schema aus dem Datenbankzustand neu auf. Nützlich nach externen Datenbankänderungen. [tool-verified: `provisa/api/admin/schema_mutation.py:456-461`]

---

### Erstellungsanfragen

#### `executeCreationRequest(requestId: Int!) → MutationResult`

Ein Rechteinhaber führt eine eingereihte Erstellungsanfrage aus — Beziehung, Sicht oder Webhook. Erfordert die Capability, auf die die Anfrage wartet. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2181-2255`]

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

Lehnt eine eingereihte Anfrage mit einem umsetzbaren Grund ab. `reason` ist erforderlich. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2258-2294`]

---

### Abfragekompilierung

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

Kompiliert eine GraphQL-Abfrage der Datenebene gegen das Schema einer Rolle und gibt die vollständige Routing-Entscheidung zurück: semantisches SQL, Engine-SQL, direktes SQL, Route, Enforcement-Metadaten (angewendete RLS-Filter, ausgeschlossene Spalten, angewendete Maskierung) und kompiliertes Cypher. Gibt ein Ergebnis pro Root-Feld in der Abfrage zurück. (REQ-161) [tool-verified: `provisa/api/admin/schema_mutation.py:3011-3055`]

```graphql
mutation {
  compileQuery(input: {
    role: "analyst"
    query: "{ orders { id customer_id total } }"
  }) {
    sql
    semanticSql
    engineSql
    route
    routeReason
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      maskingApplied
    }
  }
}
```

Felder von `CompileQueryInput`:

| Feld | Typ | Beschreibung |
|-------|------|-------------|
| `query` | `String!` | Zu kompilierende GraphQL-Abfrage der Datenebene |
| `role` | `String!` | Rolle, gegen deren Schema kompiliert wird |
| `variables` | `JSON` | Variablenbindungen |
| `flatSql` | `Boolean` | Gibt eine einzelne abgeflachte SQL-Zeichenkette statt eines semantisch/Engine-Paars zurück |
| `flatCypher` | `Boolean` | Flacht die Cypher-Ausgabe ab |
| `nodeOnlyCypher` | `Boolean` | Erzeugt Nur-Knoten-Cypher (keine Kantenmuster) |

---

## Wichtige Eingabetypen

### `SourceInput`

[tool-verified: `provisa/api/admin/types.py:590-611`]

| Feld | Typ | Hinweise |
|-------|------|-------|
| `id` | `String!` | Quellen-Identifikator |
| `type` | `String!` | Connector-Typ (z. B. `postgres`, `files`, `openapi`) |
| `host` | `String` | |
| `port` | `Int` | |
| `database` | `String` | |
| `username` | `String` | |
| `password` | `String` | Klartext oder `${secret:NAME}`-Referenz |
| `path` | `String` | Dateisystempfad für Datei-/CSV-Quellen |
| `federationHintsJson` | `String` | JSON-Objekt für Warehouse-Extras (Snowflake warehouse/role, Databricks http_path) |
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` (REQ-929) |
| `loadProtected` | `Boolean` | Nur geplante Aktualisierung (REQ-1141) |
| `offPeakWindow` | `String` | Wartungsfenster `HH:MM-HH:MM` |
| `offPeakTz` | `String` | IANA-Zeitzone |
| `cdc` | `SourceCdcConfigInput` | Kafka-CDC-Transportkonfiguration (REQ-824) |

### `TableInput`

[tool-verified: `provisa/api/admin/types.py:745-800`]

Die zentrale Eingabe zur Tabellenregistrierung. Wichtige Felder über die Grundlagen hinaus:

| Feld | Hinweise |
|-------|-------|
| `materialize` | Landet eine Kopie im Materialisierungs-Store |
| `mvRefreshInterval` | Sekunden zwischen Aktualisierungen |
| `mvPersist` | `replace` \| `append` \| `upsert` (REQ-965) |
| `mvIncremental` | Inkrementelle Wartung (REQ-969) |
| `mvBitemporalMode` | `snapshot` \| `delta` für bitemporale Tabellen (REQ-1162) |
| `mvCalendar` | Snapshot-Kalendername (REQ-962) |
| `mvGrain` | Snapshot-Granularität: `daily`, `weekly`, `monthly`, `annual` oder benutzerdefiniert `3WE` / `LFR` (REQ-962) |
| `viewSql` | SQL für eine abgeleitete Sicht |
| `viewMetrics` | Deklarative metrik-komponierte Sicht-Spezifikation — schließt sich mit `viewSql` gegenseitig aus (REQ-1318) |
| `dqContract` | YAML-/JSON-Data-Quality-Vertragstext (REQ-1443) |
| `queryTemplate` | Cypher für eine Neo4j-Tabelle (REQ-1670) |
| `live` | Live-Delivery-Konfiguration für SSE-/Kafka-Push (REQ-565, REQ-813) |
| `discover` | Leitet Spalten zur Registrierungszeit aus der Live-Quelle ab (REQ-252) |

### `RelationshipInput`

[tool-verified: `provisa/api/admin/types.py:804-826`]

| Feld | Hinweise |
|-------|-------|
| `id` | Beziehungs-Identifikator |
| `sourceTableId` | Virtueller Tabellenname (Alias, falls gesetzt, sonst Tabellenname) |
| `targetTableId` | Virtueller Tabellenname; leer bei berechneten Beziehungen |
| `sourceColumn` | Join-Spalte auf der Quellseite |
| `targetColumn` | Join-Spalte auf der Zielseite |
| `cardinality` | `one-to-one` \| `one-to-many` \| `many-to-one` \| `many-to-many` |
| `alias` | Cypher-Kantenbezeichnung (z. B. `WORKS_FOR`) |
| `graphqlAlias` | GraphQL-Feldname auf dem Quelltyp |
| `viaTable` | Junction-Tabellenname für Viele-zu-viele-Kanten (REQ-1586) |
| `recordCandidate` | Schreibt zusätzlich eine `accepted`-Zeile in relationship_candidates |

---

## Beispiel: eine Tabelle registrieren

```graphql
mutation {
  registerTable(input: {
    sourceId: "sales-pg"
    domainId: "sales"
    schemaName: "public"
    tableName: "orders"
    alias: "orders"
    columns: [
      { name: "id", visibleTo: ["public"], isPrimaryKey: true }
      { name: "customer_id", visibleTo: ["public"] }
      { name: "total", visibleTo: ["public"] }
    ]
  }) {
    success
    message
    code
  }
}
```

## Beispiel: eine RLS-Regel erstellen

```graphql
mutation {
  upsertRlsRule(input: {
    tableId: "orders"
    roleId: "regional-analyst"
    filterExpr: "region = '{{user.region}}'"
  }) {
    success
    message
  }
}
```
