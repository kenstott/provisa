# Admin-GraphQL-API-Referenz {: #admin-graphql-api-reference }

Die Admin-GraphQL-API ist Provisas Konfigurationsebene. Sie ist die API, die die Admin-Web-App für jede Verwaltungsoperation aufruft: Quellen anlegen, Tabellen registrieren, Beziehungen definieren, RLS-Regeln konfigurieren und alles andere, was das Modell formt.

**Mount-Point:** `POST /admin/graphql`

Dies ist nicht dieselbe API wie die Datenebene unter `/data/graphql`. Die Datenebene bedient Endnutzerabfragen über registrierte Domänen und wird durch das SDL unter `/data/sdl` beschrieben. Die Admin-API konfiguriert, wie dieses Schema aussieht und wer was sehen darf.

---

## Wie die UI mit dieser API spricht {: #how-the-ui-talks-to-this-api }

Die Admin-Web-App nutzt Apollo Client, gerichtet auf `${API_BASE}/admin/graphql`.

Jede Anfrage trägt ein Bearer-Token (bei jedem Aufruf frisch vom Auth-Provider geholt), einen `X-Org-Id`-Header bei Mandantenfähigkeit und einen `X-Env`-Header, wenn eine Branch-Umgebung bedient wird.

Das Schema wird aus zwei `@strawberry.type`-Klassen zusammengesetzt — `Query` aus `schema_query.py` und `Mutation` aus `schema_mutation.py` — und in eine `ModelCommitExtension` eingehüllt, die jede Mutation gegen den aktuellen Umgebungs-Branch protokolliert.

---

## Autorisierung {: #authorization }

**Dev-Modus:** Wenn keine Auth konfiguriert ist und jede Anfrage als anonymer Principal ankommt, werden alle Capability-Prüfungen übersprungen. Dies hält eine lokale Installation ohne Auth-Setup funktionsfähig.

**Capability-Gates:** Produktions-Deployments erzwingen benannte Capabilities. Das jeweils erforderliche Recht für jedes Feld ist inline vermerkt. Der Aufruf einer Mutation ohne die erforderliche Capability löst einen `PermissionError` aus. Die Rolle des Plattform-Administrators umgeht alle Capability-Prüfungen.

**Domain-Gates:** Mehrere Mutationen prüfen zusätzlich die Domäne, zu der das Objekt gehört. Ein Aufrufer mit Geltungsbereich `sales` kann keine Tabelle in `finance` registrieren, keine RLS-Regel dafür einreihen und keine Beziehung erstellen, deren Quelltabelle in einer Domäne liegt, die er nicht hält. Sichten sind weiter eingeschränkt: Jede Tabelle, die das View-SQL liest, muss innerhalb der Domänen des Aufrufers liegen, da freihändiges SQL einem Mitglied sonst Zugriff auf Daten außerhalb seines Geltungsbereichs gäbe.

**Rollenvererbung:** Die Capabilities einer übergeordneten Rolle werden von untergeordneten Rollen geerbt. `createRole` und `deleteRole` lehnen Zyklen ab und verhindern das Löschen einer Rolle, die Erben hat.

---

## Gemeinsamer Rückgabetyp {: #common-return-type }

Die meisten Mutationen geben `MutationResult` zurück.

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization
}
```

Wenn eine Mutation fehlschlägt, ist `success` `false` und `message` trägt den englischen Grund. `code` ist ein stabiler Bezeichner, den die UI zum Rendern einer lokalisierten Meldung nutzt.

---

## Queries

### Quellen {: #sources }

#### `sources → [SourceType!]!`

Alle registrierten Datenquellen.

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl replicate
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc {
      bootstrapServers topicPrefix consumerGroupId
      schemaRegistryUrl schemaRegistryAuth schemaRegistryUsername schemaRegistryPassword
      schemaRegistryToken schemaRegistryClientCert schemaRegistryClientKey schemaRegistryCa
    }
  }
}
```

`passwordRef` ist eine `${secret:NAME}`-Referenz in den Org-Vault, niemals das Klartext-Credential.

#### `source(id: String!) → SourceType`

Eine einzelne Quelle nach ID. Gibt `null` zurück, wenn nicht gefunden.

#### `availableSchemas(sourceId: String!) → [String!]!`

In einer Quelle sichtbare Schemas, gefiltert um Provisa-interne auszuschließen. Nutzt zuerst native Introspection; fällt zurück auf den Engine-Katalog, wenn der Quellentyp keinen direkten Pool hat.

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

Tabellen in einem Schema einer Quelle, mit ihren Kommentaren. Für OpenAPI-Quellen werden GET-Operationen zurückgegeben, deren Antwort ein Array oder ein Paginierungs-Wrapper ist. Für GraphQL-Quellen werden Query-Felder zurückgegeben, die eine Liste liefern. Für gRPC werden serverseitig streamende RPCs zurückgegeben.

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

Spaltennamen für eine Tabelle im Engine-Katalog. Für GovData-Quellen wird ein separater Resolver genutzt.

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

Spaltennamen mit Datentypen, Kommentaren, nativen Filtertypen und Primärschlüssel-Kennzeichnungen. Für OpenAPI-Quellen wird die Form aus dem Antwortschema und den Parametern der Operation abgeleitet.

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

Nicht-GET-Operationen einer OpenAPI-Quelle (POST, PUT, PATCH, DELETE). Gibt für Nicht-OpenAPI-Quellen eine leere Liste zurück.

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

Vorschau, was ein Datei-Connector-Crawl entdecken würde — Dateien, Tabellen und Spalten — bevor eine Quelle erstellt wird. Die HTTP-only-Einstellungen (`simpleLinks`, `sameDomain`, `excludePattern`) werden für lokale, S3-, FTP- und SFTP-Wurzeln ignoriert.

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

Gibt den Alias zurück, der bei der Registrierung von `tableName` in `domainId` aus `sourceId` zu verwenden ist. Gibt einen einfachen Snake-Case-Alias zurück, wenn kein Konflikt besteht, oder einen quellpräfixierten Alias (`sqlite_b_orders`), wenn der effektive Name bereits von einer anderen Quelle in derselben Domäne belegt ist.

---

### Tabellen {: #tables }

#### `tables → [RegisteredTableType!]!`

Alle registrierten Tabellen, jeweils mit ihrer vollständigen Spaltenliste. Die Spaltensichtbarkeit in der Antwort respektiert die `table_registration`-Capability des Aufrufers: `canDeployToDb` ist davon abhängig, ob der Aufrufer dieses Recht hält.

Jeder `RegisteredTableType` exponiert berechnete Unterfelder:

- **`refreshPolicySummary → RefreshPolicySummaryType`** — die effektive Refresh-/Serving-Richtlinie als Klartext, serverseitig aus derselben Planner-Auflösung abgeleitet, die die Engine nutzt. Gibt während des Starts `null` zurück.
- **`graphqlFieldName → String`** — der Feldname, den diese Tabelle im kompilierten Schema der Datenebene hat, damit das Data-Product-Panel ein lauffähiges Beispiel erstellen kann, ohne den Namensgebungs-Algorithmus zu duplizieren.
- **`dqDataset → String`** — diese Tabelle als Data-Quality-Vertrags-Dataset, in der Form, die der Checker scannt.
- **`productId → String`** — das Datenprodukt, zu dem diese Tabelle gehört. Eine DQ-Checker-Tabelle erbt das Produkt der Tabelle, deren Vertrag sie scannt.

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

Vorschau der effektiven Refresh-/Serving-Zusammenfassung für *Entwurfs*-(ungespeicherte) Tabellenparameter, sodass sich die Zusammenfassung am Formularanfang bei Feldänderungen aktualisiert, ohne etwas zu persistieren. Gleiche Ableitung wie `refreshPolicySummary` oben.

Argumente: `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `replicate`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

Artefakte, die eine anstehende Alias-Umbenennung oder ein Spalten-Drop brechen würde. Das Ergebnis ist ein Hinweis: Die Admin-UI zeigt dies vor dem Speichern an, und der Administrator entscheidet. Muss *vor* dem Speichern aufgerufen werden, da die Abhängigen gegen den aktuell von der Spalte getragenen exponierten Namen verfasst wurden.

---

### Beziehungen {: #relationships }

#### `relationships → [RelationshipType!]!`

Alle benutzerdefinierten Beziehungen (schließt automatisch generierte `gql_auto__`-Einträge und die synthetischen `meta:%`-Einträge aus, die vom ERD genutzt werden).

#### `allRelationships → [RelationshipType!]!`

Wie `relationships`, umfasst aber `meta:%`-synthetische Einträge. Wird vom Graph-ERD genutzt, das jede Kante inklusive der impliziten `HAS_TABLE`-Verknüpfungen zwischen Datentabellen und der Metadaten-Registry anzeigen muss.

Jeder `RelationshipType` exponiert:

- **`autoSuggested → Boolean`** — ob die Beziehung durch FK-Analyse vorgeschlagen wurde (`id` beginnt mit `fk__`).
- **`physicalName → String`** — der Name der Beziehung auf der SQL- und gRPC-Ebene (der `?include=`-Parameter). Serverseitig abgeleitet; Clients dürfen den GraphQL-Alias nicht transliterieren.

---

### Domänen, Rollen & Benutzer {: #domains-roles-users }

#### `domains → [DomainType!]!`

Alle Domänen in der Mandanten-Datenbank des aktiven Orgs. Die Mandanten-Datenbank ist auf Schema-Ebene isoliert, sodass die Domänenliste eines Org-Admins nur die Zeilen seines eigenen Orgs enthält.

#### `roles → [RoleType!]!`

Für den Aufrufer sichtbare Rollen. Ein Admin sieht jede Rolle; ein Nicht-Admin sieht nur Rollen ohne `org_id` oder Rollen, die zu seinem Org gehören.

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

Löst Rollen-IDs oder Benutzer-IDs zu einzelnen Benutzern auf. Wird genutzt, um `DataProduct.ownerRole`, `Domain.steward` und `Column.visibleTo` zu einer menschenlesbaren Liste zu erweitern. Unbekannte Referenzen werden unverändert zurückgegeben, damit die UI die rohe ID statt nichts anzeigt.

---

### RLS-Regeln {: #rls-rules }

#### `rlsRules → [RLSRuleType!]!`

Alle Row-Level-Security-Regeln. Das zugrundeliegende Repository entschlüsselt `filterExpr` an der Grenze.

---

### Datenprodukte {: #data-products }

#### `dataProducts → [DataProductType!]!`

Alle Datenprodukte. Erfordert die Capability `data_product_read`.

---

### Tags

#### `tags → [TagType!]!`

Alle Tag-Definitionen, einschließlich ihrer pro Tag zulässigen Parameterwerte.

#### `tagAssignments → [TagAssignmentType!]!`

Alle Tag-Zuweisungen über Quellen, Tabellen, Spalten und Beziehungen hinweg.

---

### Materialisierte Sichten {: #materialized-views }

#### `mvList → [MVType!]!`

Alle materialisierten Sichten mit ihrem Laufzeitstatus: aktiviert/deaktiviert, Zeitstempel der letzten Aktualisierung, Zeilenzahl und letzter Fehler.

---

### Cache

#### `cacheStats → CacheStatsType`

Cache-Statistiken. Gibt `storeType: "redis"` mit vollständigen Betriebsmetriken zurück, wenn Redis konfiguriert ist, `storeType: "memory"` für den eingebetteten fakeredis-Store und `storeType: "noop"`, wenn kein Cache konfiguriert ist.

#### `cacheTableStats → [CacheTableStatType!]!`

Anzahl gecachter Einträge pro Tabelle. Leer, wenn kein Cache-Store konfiguriert ist.

#### `hotTables → [HotTableStatType!]!`

Tabellen, von denen Provisa eine Kopie vorhält, über zwei Stufen: `hot` (in den Response-Store gespiegelt für JOIN-Inlining) und `warm` (als Iceberg-Kopie gelandet). Eine Tabelle ist in höchstens einer Stufe.

#### `materializeStoreInfo → MaterializeStoreInfoType`

Identität des dauerhaften Materialisierungs-Stores: Engine-Name, Store-DSN-Referenz, MV-Anzahl und ob der Store instanzlokal ist (eine lokale Datei wie DuckDB oder SQLite, was bedeutet, dass jede Instanz hinter einem Load Balancer ihre eigene Kopie hält).

---

### Systemzustand {: #system-health }

#### `systemHealth → SystemHealthType`

Engine-Verbindungsstatus, Worker-Pool-Anzahlen, Metadaten-DB-Pool-Zustand, Cache-Modus und Liveness jedes Protokoll-Listeners (pgwire, gRPC, Arrow Flight, Bolt).

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

### Geplante Tasks {: #scheduled-tasks }

#### `scheduledTasks → [ScheduledTaskType!]!`

Geplante Trigger aus der Konfiguration mit Laufzeitstatus. Jeder Eintrag trägt seinen Cron-Ausdruck, `kind` (`webhook` oder `sql`), ob er aktuell aktiviert ist, den Zeitstempel des letzten Laufs (immer `null`; der Scheduler verfolgt ihn) und die nächste geplante Laufzeit von APScheduler.

---

### Data Quality

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

Parst rohen Vertragstext in die editierbaren Zeilen des Builder-Panels. Wird bei jeder Bearbeitung aufgerufen; ein Parse-Fehlschlag kommt als `error` zurück statt als GraphQL-Fehler, da halbgeschriebener Text normal ist, während der Betreiber tippt.

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

Die Checks, die `checker` anbietet, eingeschränkt auf die Spalten von `dataset`. Das Dataset ist das beobachtete Ziel des Vertrags, aufgelöst auf dieselbe Weise wie der Scanner es auflöst, sodass die angebotenen Checks zu den Spalten passen, die der Checker wirklich sehen wird.

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

Der Text eines Checks aus den Editoren des Panels. Serverseitig, weil der Dialekt eine Implementierung hat; ein per Builder erstellter Check und ein handgetippter müssen ununterscheidbar sein.

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

Serialisiert bearbeitete Check-Zeilen zurück in Vertragstext. Das Inverse von `dqContractParse`. Serverseitig aus demselben Grund: Das Panel darf keinen Text ausgeben, den der Checker ablehnen würde.

---

### Quellenvorschauen {: #source-previews }

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

Vorschau einer Cypher-Projektion auf einer Neo4j-Quelle: bis zu fünf Zeilen und die Spaltentypen, die die Registrierung tragen wird. Fehlschläge kommen als `error` zurück.

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

Vorschau eines SPARQL-SELECT auf einer SPARQL-Quelle: bis zu fünf Zeilen, alle Spalten als Text.

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

Live-Prüfung gegen die Kaggle-API. Gibt nur `true` zurück, wenn das Token authentifiziert. Unterstützt den Token-Gate-Schritt im Kaggle-Quellen-Formular.

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

Durchsucht Kaggles vollständigen öffentlichen Dataset-Katalog. Token-gated.

---

### Kalender {: #calendars }

#### `calendars → [CalendarType!]!`

Alle registrierten Snapshot-Grenz-Kalenderversionen. Speist den Konfigurationspicker für Snapshot-Zeitpläne und bestätigt, welche Kalender eine periodische MV referenzieren darf.

---

### Metriken {: #metrics }

#### `metrics → [MetricType!]!`

Alle governten Metrikdefinitionen. Fakt-abgeleitete Metriken tragen `fromFact`.

---

### Schema-Version

#### `schemaVersion → String!`

SHA-256-Hash des aktuellen Schemazustands (Domänen, Tabellen-IDs, Beziehungs-IDs). Der Apollo-Client liest dies aus dem `X-Schema-Version`-Antwort-Header und ruft alle aktiven Abfragen erneut ab, wenn er sich ändert.

---

### KI-Hilfsfunktionen {: #ai-helpers }

#### `generateTableDescription(tableId: String!) → String!`

Nutzt das konfigurierte LLM, um eine ein- bis zweisätzige Beschreibung für eine registrierte Tabelle zu generieren. Speichern Sie die Tabelle zuerst; der Aufruf hierfür auf einer ungespeicherten Tabelle gibt eine Anleitungsmeldung zurück.

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

Nutzt das konfigurierte LLM, um eine einsätzige Beschreibung für eine einzelne Spalte zu generieren.

---

### Erstellungsanfragen {: #creation-requests }

#### `creationRequests → [CreationRequestType!]!`

Ausstehende Erstellungsanfragen, sichtbar für Aufrufer mit der relevanten Erstellungs-Capability. Wird genutzt, wenn ein Mitglied ohne `create_relationship` oder `create_view` eine Anfrage stellt, die ein Rechteinhaber genehmigen muss. Eine Beziehungsanfrage wird den Benutzern angezeigt, deren Recht `create_relationship` die Domäne ihrer Quell- oder Zieltabelle erreicht, und dem Benutzer, der sie gestellt hat.

---

## Mutationen {: #mutations }

### Quellen {: #sources_1 }

#### `createSource(input: SourceInput!) → MutationResult`

Registriert eine neue Datenquelle. Eine bereits vorhandene ID wird abgelehnt (`schema.source_id_taken`), nach den Prüfungen der Definition und bevor irgendetwas vorbereitet oder geschrieben wird; eine Quelle wird mit `updateSource` geändert. Validiert die Verbindung vor der Persistierung. Eine abgelehnte Quelle hinterlässt keinen Vault-Eintrag. Speichert Zugangsdaten im Org-Vault und protokolliert die Referenz; der Klartext landet nie in der Datenbank. Erfordert die Capability `source_registration`.

#### `updateSource(input: SourceInput!) → MutationResult`

Aktualisiert Verbindungsdetails, Beschreibung und Konfiguration einer bestehenden Quelle, stellt sie dann erneut auf der gebundenen Engine bereit, baut die Schemas neu und gleicht ihre gelandeten Tabellen ab, sodass eine unter einer Engine gespeicherte Quelle von einer seither gewechselten Engine bedient wird. Baut den pgwire-Endpunkt für Datei-/SharePoint-Quellen ab und hängt ihn neu an, damit eine Pfadänderung sofort wirkt. Erfordert `source_registration`.

#### `deleteSource(id: String!) → MutationResult`

Entfernt eine Quelle und ihren Vault-Eintrag. Löscht den Engine-Katalog und baut Schemas neu auf.

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

Benennt eine Quell-ID um. Eine neue ID, die eine andere Quelle hält, wird abgelehnt (`schema.source_id_taken`), und nichts wird geschrieben; eine Quelle auf ihre eigene ID umzubenennen ändert nichts.

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

Aktiviert oder deaktiviert das Caching von Abfrageergebnissen für eine Quelle und setzt die TTL in Sekunden.

#### `updateSourceReplicate(sourceId: String!, replicate: Int) → MutationResult`

Legt fest, wann die Tabellen einer Quelle aus ihren Replikaten bedient werden: `null` Default, `-1` Never, `N` über 0, sobald eine Tabelle N regulierte Anweisungen pro Intervall überschreitet, `0` Always. Eine Tabelle ohne eigenen Wert erbt ihn. Abgelehnt, wenn er `load_protected` mit `-1` kombinieren oder eine `ttl`-Tabelle, die er repliziert, ohne `cache_ttl` lassen würde. Erfordert `source_registration`.

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Markiert eine Quelle als lastgeschützt (nur geplante Aktualisierung). Erfordert mindestens ein Gate — Off-Peak-Fenster, Cache-TTL-Kadenz oder ein prüfendes Änderungssignal — sonst schlägt der Aufruf fehl.

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

Setzt die GraphQL-Namenskonvention pro Quelle.

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

Legt fest, welche Domänen eine Quelle nutzen dürfen (leere Liste = uneingeschränkt). Erfordert `source_registration`.

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

Lädt ein Kaggle-Dataset herunter und entpackt es lokal. Gibt den Pfad des bereitgestellten Verzeichnisses zurück; der Aufrufer erstellt danach eine Quelle vom Typ `files`, die darauf zeigt. Bundles mit SQLite werden vollständig abgelehnt. Erfordert `source_registration`.

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

Holt das Dataset einer von Kaggle abgeleiteten Quelle an Ort und Stelle erneut ab. Überspringt den Download, wenn Kaggle nichts Neueres hat als das, was auf der Festplatte liegt. Erfordert `source_registration`.

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

Führt `ANALYZE` auf allen registrierten Tabellen einer Quelle aus. Verbessert Join-Reihenfolge- und Broadcast-Entscheidungen für föderierte Abfragen.

---

### Tabellen {: #tables_1 }

#### `registerTable(input: TableInput!) → MutationResult`

Registriert eine neue Tabelle (oder Sicht) in einer Domäne, mit jeder Einstellung, die die Eingabe mitführt, darunter `gqlNamingConvention`, `roleTtl`, `replicate`, `writeGrants`, `cacheTtl`, `pagination`, `region`, `rowRules` und `draft`, in einer Transaktion: Wird eine davon abgelehnt, wird nichts registriert. Erfordert die Capability `table_registration` und Mitgliedschaft in der Zieldomäne; wer `writeGrants` mitführt, braucht dort auch `masking_config`. Ein Aufrufer ohne `create_relationship`, der eine Sicht einreicht, wird als Erstellungsanfrage für einen Rechteinhaber zur Genehmigung eingereiht.

#### `updateTable(input: TableInput!) → MutationResult`

Aktualisiert Alias, Beschreibung, Spaltenmetadaten, MV-Einstellungen und Live-Delivery-Konfiguration einer bestehenden Tabelle sowie die Einstellungen, die die Eingabe mitführt (`gqlNamingConvention`, `roleTtl`, `replicate`, `writeGrants`, `cacheTtl`, `pagination`, `region`, `rowRules`, `draft`), in einer Transaktion: Wird eine davon abgelehnt, wird nichts gespeichert. Eine weggelassene Einstellung behält ihren gespeicherten Wert. Erfordert `table_registration`.

#### `deleteTable(id: Int!) → MutationResult`

Löscht eine registrierte Tabelle. Ermittelt die Domäne der Tabelle für das Domain-Gate vor dem Löschen.

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

Überschreibt die Cache-TTL für eine Tabelle.

#### `updateTableReplicate(tableId: Int!, replicate: Int) → MutationResult`

Legt fest, wann eine Tabelle aus ihrem Replikat bedient wird. `null` = den Wert der Quelle erben. Dieselben Werte und Ablehnungen wie bei `updateSourceReplicate`. Erfordert `table_registration`.

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Überschreibt den Lastschutz für eine Tabelle. `null` für `loadProtected` erbt den Quellenstandard. Validiert die effektive (Tabelle → Quelle) Gate-Kombination.

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

Setzt die GraphQL-Namenskonvention pro Tabelle.

#### `deployViewToDb(tableId: Int!) → MutationResult`

Befördert eine virtuelle Provisa-Sicht zu einer echten Datenbank-Sicht auf ihrer zugrundeliegenden nativen Quelle.

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

Berechnet die gelandeten Zeilen einer Tabelle bei Bedarf neu und umgeht dabei das normale Änderungs-Gate. `reason` ist eine erforderliche Audit-Anmerkung. Wird für live-föderierte Tabellen abgelehnt (keine gelandeten Zeilen).

#### `invalidateFileSource(tableId: Int!) → MutationResult`

Erzwingt, dass der nächste Zugriff auf eine SQLite-Datei-Connector-Tabelle erneut von der Festplatte synchronisiert.

#### `registerEntity(input: EntityInput!) → MutationResult`

Zucker für die Registrierung einer Dimensions-/Hub-Entität. Reduziert sich zu einer (bitemporalen, wenn historisiert) MV und ruft `registerTable` auf.

#### `registerFact(input: FactInput!) → MutationResult`

Zucker für die Registrierung eines Star-Schema-Fakts. Reduziert sich zu einer aggregierten MV, erstellt Dimensionsbeziehungen und registriert Fakt-Kennzahlen automatisch als governte Metriken.

---

### Beziehungen {: #relationships_1 }

#### `createRelationship(input: RelationshipInput!) → MutationResult`

Erstellt eine Beziehung. Das Gate prüft die Domäne der Quelltabelle (nicht die des Ziels). Eine domänenübergreifende Kante wird mit `needsReview: true` gespeichert. Ein Aufrufer ohne `create_relationship` wird als Erstellungsanfrage eingereiht. Junction-(Viele-zu-viele-)Kanten erfordern übereinstimmende Längen der Schlüssellisten. Namentlich abgelehnt, bevor irgendetwas geschrieben wird: eine bereits vorhandene ID (`schema.relationship_id_taken`); ein Tabellenname, der auf keine oder mehr als eine registrierte Tabelle auflöst (`schema.relationship_unknown_source`, `_target`, `_junction`, `schema.relationship_ambiguous_table`); ein Cypher-Alias oder GraphQL-Alias, den eine andere Beziehung derselben Quelltabelle hält (`schema.relationship_alias_taken`, `schema.relationship_graphql_alias_taken`). Eine Tabelle wird mit ihrem virtuellen Namen benannt: ihrem Alias, wenn sie einen hat.

#### `updateRelationship(input: RelationshipInput!) → MutationResult`

Ändert eine bestehende Beziehung, mit denselben Prüfungen wie `createRelationship`. Eine ID, die keine Beziehung hält, wird abgelehnt (`schema.relationship_not_found`). Eine Änderung braucht das Recht selbst; sie wird nie als Anfrage eingereiht.

Ein Speichern, das Schlüsselspalten zwischen einer Eigenschaft und einer Klasse umsiedelt, kann ihnen `pii` entziehen. Es wird dann mit `tag.move_loses` abgelehnt, und `params.columns` listet jede Spalte mit den Tags, die sie verliert (`loses`) und gewinnt (`gains`). Speichern Sie erneut, mit `confirmTagLoss`, das jeden verlorenen Tag nennt, oder mit `carryTags: true`, um sie auf den Begriff zu setzen, zu dem die jeweilige Spalte wechselt.

#### `deleteRelationship(id: String!, confirmTagLoss: [String!], carryTags: Boolean) → MutationResult`

Löscht eine Beziehung nach ID und baut Schemas neu auf. Ein Löschen, das Schlüsselspalten umsiedelt, nimmt dieselben `confirmTagLoss` und `carryTags` wie ein Speichern.

---

### Domänen {: #domains }

#### `createDomain(input: DomainInput!) → MutationResult`

Erstellt eine Domäne. Eine bereits vorhandene ID wird abgelehnt (`schema.domain_id_taken`). Reservierte Segmentwörter (`tables`, `relationships` und andere URI-Pfadsegmente) werden abgelehnt, ebenso das Wildcard-Literal `*`. Erfordert die Capability `org_settings`.

#### `deleteDomain(id: String!) → MutationResult`

Löscht eine Domäne. Erfordert `org_settings`.

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

Setzt die globale GraphQL-Namenskonvention und baut Schemas für alle Rollen neu auf. Nur erkannte Konventionsnamen werden akzeptiert.

---

### Rollen {: #roles }

#### `createRole(input: RoleInput!) → MutationResult`

Erstellt eine Rolle mit Capabilities, Domänenzugriff, optionalen Rate Limits und einer optionalen übergeordneten Rolle. Validiert, dass die übergeordnete Rolle existiert und die Elternkette zyklenfrei ist. Nach den Prüfungen der Definition wird eine bereits vorhandene ID abgelehnt (`schema.role_id_taken`). Erfordert `user_management`.

#### `updateRole(input: RoleInput!) → MutationResult`

Ändert eine bestehende Rolle, mit denselben Prüfungen wie `createRole`. Eine ID, die keine Rolle hält, wird abgelehnt (`schema.role_not_found`). Eine Rolle neu zu definieren ist ein Akt in jeder Domäne, die sie erreicht.

#### `deleteRole(id: String!) → MutationResult`

Löscht eine Rolle. Schlägt fehl, wenn andere Rollen von ihr erben; diese müssen zuerst umgehängt werden. Erfordert `user_management`.

---

### RLS-Regeln {: #rls-rules_1 }

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

Erstellt oder aktualisiert eine Row-Level-Security-Regel. Der Filterausdruck wird beim Speichern gegen die Spalten der Ziel-Tabelle oder -Domäne validiert, sodass eine Regel, die der Administrator nicht abfragen kann, mit dem Grund abgelehnt wird, statt zur Abfragezeit stillschweigend zu scheitern. Erfordert `masking_config`.

Ziele schließen sich gegenseitig aus: Setzen Sie `tableId` für eine Regel auf Tabellenebene, `domainId` für eine Regel auf Domänenebene, oder `actionName` für eine überwachte Funktion/Webhook.

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

Löscht eine RLS-Regel. Erfordert `masking_config`.

---

### Datenprodukte {: #data-products_1 }

#### `createDataProduct(input: DataProductInput!) → MutationResult`

Erstellt ein Datenprodukt. Eine bereits vorhandene ID wird abgelehnt (`schema.data_product_id_taken`). Erfordert die Capability `data_product_rw`.

#### `updateDataProduct(input: DataProductInput!) → MutationResult`

Ändert ein bestehendes Datenprodukt. Eine ID, die kein Produkt hält, wird abgelehnt (`schema.data_product_not_found`). Erfordert `data_product_rw`.

#### `deleteDataProduct(id: String!) → MutationResult`

Löscht ein Datenprodukt. Erfordert `data_product_rw`.

---

### Tags

#### `createTag(input: TagInput!) → MutationResult`

Erstellt eine Tag-Definition. Eine bereits vorhandene ID wird abgelehnt (`schema.tag_id_taken`). System-Tags und abgeleitete Tags können nicht neu definiert werden. `appliesTo` muss eine nicht-leere Teilmenge von `["source", "table", "column", "relationship", "command"]` sein.

#### `updateTag(input: TagInput!) → MutationResult`

Ändert eine bestehende Tag-Definition, mit denselben Prüfungen wie `createTag`. Eine ID, die kein Tag hält, wird abgelehnt (`schema.tag_not_found`).

#### `deleteTag(id: String!) → MutationResult`

Löscht einen Tag. Lehnt System- und abgeleitete Tags ab.

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

Weist einer Quelle, Tabelle, Spalte, Beziehung oder einem Command einen Tag zu. Erzwingt die Feldrichtlinien des Tags (`reason_policy`, `expires_policy`) und prüft bei einem parametrisierten Tag den Parameterwert gegen die zulässige Liste des Tags.

`pii` kann auch auf einen Glossarbegriff gesetzt werden, sodass jede Spalte mit dieser Bedeutung den Tag trägt, auch später registrierte Spalten. Setzen Sie `objectType: "term"` und `termId`:

```graphql
mutation { assignTag(input: {tagId: "pii", objectType: "term", termId: 12}) { success code params } }
```

Auf einer Spalte setzt `alsoTerm: true` den Tag stattdessen auf den Begriff der Spalte. `params.covers` im Ergebnis gibt an, wie viele Spalten der Tag erreicht: bei einer Spalte, wie viele andere Spalten ihr Begriff hält; bei einem Begriff, wie viele er hält. Namentlich abgelehnt werden: ein anderer Tag als `pii` auf einem Begriff (`tag.not_on_terms`), ein Begriff, der keine Eigenschaft ist (`tag.term_not_property`), ein Begriff im Entwurf (`tag.term_not_live`, „Definieren Sie *Begriff*, um ihn zu markieren“) und, mit `alsoTerm`, eine Schlüsselspalte (`tag.key_column`) oder eine Spalte ohne Begriff (`tag.column_has_no_term`). Eine direkt markierte Spalte braucht eine Begründung.

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

Entfernt eine Tag-Zuweisung, auch von einem Begriff (`objectType: "term"`, `termId`). Das Entfernen von `pii` von einer Spalte, die ihn über ihren Begriff trägt, wird abgelehnt (`tag.inherited_from_term`): Heben Sie die Markierung des Begriffs auf, oder verschieben Sie die Spalte auf einen Begriff, der zu ihrem Inhalt passt.

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

Fügt einen zulässigen Parameterwert für einen parametrisierten Tag hinzu oder beschreibt ihn neu. Die Liste zulässiger Werte ist geschlossen: Jede Zuweisung muss einen Wert daraus benennen.

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

Entfernt einen zulässigen Wert. Wird abgelehnt, solange eine Zuweisung ihn noch trägt, da diese Zuweisungen dann einen Typ benennen würden, den die Liste nicht mehr zulässt.

---

### Metriken {: #metrics_1 }

#### `createMetric(input: MetricInput!) → MutationResult`

Erstellt eine governte Metrikdefinition. Ein bereits vorhandener Name wird abgelehnt (`schema.metric_name_taken`). Der Ausdruck muss unter sqlglot parsen und mindestens eine Aggregatfunktion enthalten. Regeneriert alle metrik-komponierten Sichten, die diese Metrik referenzieren. Erfordert `table_registration`.

#### `updateMetric(input: MetricInput!) → MutationResult`

Ersetzt die Definition einer bestehenden Metrik, mit denselben Prüfungen wie `createMetric`. Ein Name, den keine Metrik hält, wird abgelehnt (`schema.metric_not_found`). Das Speichern braucht die Domänen des neuen Ausdrucks und der Definition, die er ersetzt.

#### `deleteMetric(name: String!) → MutationResult`

Löscht eine governte Metrik. Baut Schemas neu auf. Erfordert `table_registration`.

---

### Kalender {: #calendars_1 }

#### `createCalendar(input: CalendarInput!) → MutationResult`

Erstellt oder ersetzt einen versionierten Snapshot-Grenz-Kalender. Validiert durch Konstruktion des In-Memory-`Calendar` vor der Persistierung. Es schlägt fehl bei unbekanntem Basissystem, falscher Zeitzone oder falschem Fiskalanker.

#### `deleteCalendar(name: String!) → MutationResult`

Löscht einen Kalender (alle Versionen). Wird abgelehnt, wenn eine materialisierte Sicht ihn referenziert.

---

### Materialisierte Sichten {: #materialized-views_1 }

#### `refreshMv(mvId: String!) → MutationResult`

Löst eine manuelle Aktualisierung einer materialisierten Sicht aus. Koordiniert über die Flotte hinweg, wenn der MV-Konsistenzmodus `shared` ist.

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

Aktiviert oder deaktiviert eine materialisierte Sicht.

---

### Cache

#### `purgeCache → MutationResult`

Löscht alle gecachten Abfrageergebnisse.

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

Löscht gecachte Ergebnisse für eine Tabelle.

---

### Geplante Tasks {: #scheduled-tasks_1 }

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

Erstellt einen geplanten Trigger — entweder einen Webhook-Aufruf oder eine SQL-Anweisung — und registriert ihn live in APScheduler. `kind` ist `"webhook"` oder `"sql"`.

#### `deleteScheduledTask(taskId: String!) → MutationResult`

Entfernt einen geplanten Trigger aus der Konfiguration und dem laufenden Scheduler.

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

Aktiviert oder deaktiviert einen geplanten Task in der Konfigurationsdatei.

---

### Data Quality

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

Führt einen Vertrag gegen die Live-Tabelle aus und gibt Ergebnisse zurück, ohne etwas zu landen. Eine Mutation statt einer Query, weil dies einen echten Scan kostet. Was sie beweist, ist, ob sich der Dataset-Bezeichner zur governten Tabelle auflöst, die der Betreiber meint.

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

Löst sofort den Poll-Job einer Checker-Tabelle aus. Landet Zeilen auf normalem Weg, sodass Ergebnisse persistieren und die DQ-Historie den neuen Scan anzeigt.

---

### Schema-Wartung {: #schema-maintenance }

#### `rebuildSchemas → MutationResult`

Baut das In-Memory-Schema aus dem Datenbankzustand neu auf. Nützlich nach externen Datenbankänderungen.

---

### Erstellungsanfragen {: #creation-requests_1 }

#### `approveCreationRequest(requestId: Int!) → MutationResult`

Genehmigt eine Erstellungsanfrage: dieselbe Regel, dieselben Ablehnungen und Einträge im Admin-Protokoll wie `POST /admin/creation-requests/{id}/approve`. Der Antragsteller kann seine eigene Anfrage nicht genehmigen, und die Genehmigung eines Benutzers zählt einmal. Die Genehmigung, die die Zahl der Anfrage vervollständigt, erstellt, was sie verlangt; `params.status` ist `executed`, wenn diese es getan hat, sonst `pending`, und `params.waitingOn` listet die Domänen, auf die eine Beziehungsanfrage noch wartet.

#### `executeCreationRequest(requestId: Int!) → MutationResult`

Führt eine Erstellungsanfrage, die ihre Genehmigungen hat — Beziehung, Sicht, Tabelle, Quelle oder Webhook —, über denselben Code aus, den die direkte Mutation nutzt. Abgelehnt mit `requests.approvals_incomplete`, bis die erforderlichen Genehmigungen der Anfrage (eine, oder zwei bei einer Beziehung) von anderen Benutzern als dem Antragsteller erreicht sind; eine Beziehungsanfrage wird außerdem mit `requests.waiting_on_domains` abgelehnt, solange eine Domäne, die sie berührt, keinen Genehmiger hat. Genehmigungen werden über `POST /admin/creation-requests/{id}/approve` erteilt, und die Genehmigung, die die Zahl vervollständigt, führt die Erstellung aus; diese Mutation dient also dazu, eine fehlgeschlagene Erstellung erneut zu versuchen. Eine fehlgeschlagene Erstellung lässt die Anfrage ausstehend und antwortet mit dem eigenen Code des Fehlers.

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

Lehnt eine eingereihte Anfrage mit einem umsetzbaren Grund ab. `reason` ist erforderlich. Eine Beziehungsanfrage kann jeder Benutzer ablehnen, der sie genehmigen könnte. Der Autor einer Anfrage jedes anderen Typs kann sie zurückziehen; das wird im Admin-Protokoll als Rücknahme vermerkt.

---

### Abfragekompilierung {: #query-compilation }

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

Kompiliert eine GraphQL-Abfrage der Datenebene gegen das Schema einer Rolle und gibt die vollständige Routing-Entscheidung zurück: semantisches SQL, Engine-SQL, direktes SQL, Route, Enforcement-Metadaten (angewendete RLS-Filter, ausgeschlossene Spalten, angewendete Maskierung) und kompiliertes Cypher. Gibt ein Ergebnis pro Root-Feld in der Abfrage zurück.

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

## Wichtige Eingabetypen {: #key-input-types }

### `SourceInput`

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
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` |
| `loadProtected` | `Boolean` | Nur geplante Aktualisierung |
| `offPeakWindow` | `String` | Wartungsfenster `HH:MM-HH:MM` |
| `offPeakTz` | `String` | IANA-Zeitzone |
| `cdc` | `SourceCdcConfigInput` | Kafka-CDC-Transportkonfiguration, einschließlich der Art, wie ihre Schema Registry erreicht wird (siehe [Authentifizierung der Schema Registry](subscriptions.md#schema-registry-authentication)). `schemaRegistryPassword` und `schemaRegistryToken` werden als Vault-Referenzen zurückgegeben, nie als Anmeldedaten. |

### `TableInput`

Die zentrale Eingabe zur Tabellenregistrierung. Wichtige Felder über die Grundlagen hinaus:

| Feld | Hinweise |
|-------|-------|
| `materialize` | Landet eine Kopie im Materialisierungs-Store |
| `mvRefreshInterval` | Sekunden zwischen Aktualisierungen |
| `mvPersist` | `replace` \| `append` \| `upsert` |
| `mvIncremental` | Inkrementelle Wartung |
| `mvHoldOnFailingChecks` | Bei fehlschlagenden Prüfungen anhalten: Eine Aktualisierung wird abgebrochen, und die Sicht behält ihre vorherige Kopie, solange eine Prüftabelle, die eine ihrer Eingaben prüft, bei ihrem letzten Lauf fehlgeschlagen ist |
| `businessKey` | Spalten, die über alle Versionen hinweg dasselbe Ding bezeichnen; deklariert mit `validTime` und `recordedTime`, alle drei oder keine |
| `validTime` | Spalte mit dem Datum, das das Unternehmen dem Datensatz gibt |
| `recordedTime` | Spalte mit dem Zeitpunkt, zu dem diese Version erfasst oder geändert wurde |
| `mvCalendar` | Snapshot-Kalendername |
| `mvGrain` | Snapshot-Granularität: `daily`, `weekly`, `monthly`, `annual` oder benutzerdefiniert `3WE` / `LFR` |
| `viewSql` | SQL für eine abgeleitete Sicht |
| `viewMetrics` | Deklarative metrik-komponierte Sicht-Spezifikation — schließt sich mit `viewSql` gegenseitig aus |
| `dqContract` | YAML-/JSON-Data-Quality-Vertragstext |
| `queryTemplate` | Cypher für eine Neo4j-Tabelle |
| `live` | Live-Delivery-Konfiguration für SSE-/Kafka-Push |
| `discover` | Leitet Spalten zur Registrierungszeit aus der Live-Quelle ab |
| `gqlNamingConvention` | GraphQL-Namenskonvention der Tabelle; `null` erbt. Weggelassen erbt eine neue Tabelle, und eine Aktualisierung behält die gespeicherte |
| `roleTtl` | Die ganze Liste Rolle zu TTL, `[{ role, ttl }]` |
| `replicate` | Wann die Tabelle aus ihrem Replikat bedient wird; `null` erbt den Wert der Quelle |
| `writeGrants` | Die ganze Menge der Schreibberechtigungen der Tabelle; erfordert `masking_config` |
| `draft` | `false` gibt die Tabelle frei; sonst beginnt eine Registrierung als Entwurf |
| `cacheTtl` | Die eigene Cache-TTL der Tabelle; `null` hat keine eigene |
| `pagination` | Das deklarierte Paging der Tabelle; `null` deklariert keines |
| `region` | Wo die Daten der Tabelle liegen; bei einer Aktualisierung verschiebt eine andere Region sie, geprüft wie bei `setTableRegion` |
| `rowRules` | Zeilenregeln der Tabelle (`RLSRuleInput`), mit ihr gespeichert; erfordert `masking_config` |

### `RelationshipInput`

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
| `viaTable` | Junction-Tabellenname für Viele-zu-viele-Kanten |
| `recordCandidate` | Schreibt zusätzlich eine `accepted`-Zeile in relationship_candidates |

---

## Beispiel: eine Tabelle registrieren {: #example-registering-a-table }

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

## Beispiel: eine RLS-Regel erstellen {: #example-creating-an-rls-rule }

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
