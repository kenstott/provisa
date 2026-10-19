# Import aus Hasura {: #importing-from-hasura }

Provisa kann bestehende Hasura-Metadaten in eine Provisa-`config.yaml` umwandeln und dabei getrackte Tabellen, Beziehungen, Berechtigungen und Remote Schemas erhalten.

## Interaktiver Import (Admin → Hasura-Konfiguration importieren) {: #interactive-import-admin-import-hasura-config }

Die Admin-Oberfläche nutzt dieselben Konverter, sodass ein Import weder Shell-Zugriff noch einen Config-Datei-Roundtrip benötigt. Erfordert die Capability `org_settings`; der Import landet in der Organisation, in der die Sitzung aktiv ist.

1. **Hochladen.** Wählen Sie ein gezipptes Hasura-v2-Metadatenverzeichnis, ein gezipptes DDN-Projekt, einen konsolidierten Metadaten-Export (`.yaml`/`.json`, einschließlich der `{resource_version, metadata}`-Hülle, die die Metadaten-API zurückgibt), oder eine einzelne `.hml`-Datei. Belassen Sie das Format bei *Automatisch erkennen*, außer der Upload ist mehrdeutig.
2. **Domänen zuordnen** (optional). Jedes Paar ordnet ein v2-Schema oder einen DDN-Subgraph einer Provisa-Domäne zu; alles Nicht-Zugeordnete behält seinen ursprünglichen Namen.
3. **Konvertieren und Vorschau.** Der Server konvertiert und liefert Anzahlen, den Konvertierungsbericht (siehe [Der Bericht](#the-report)) und die generierte Konfiguration zurück. In diesem Schritt wird nichts geschrieben.
4. **Prüfen und bearbeiten.** Die Konfiguration ist direkt bearbeitbar: Verbindungsdetails, Domänennamen, Rollennamen. Was Sie anwenden, ist das, was angezeigt wird.
5. **Anwenden.** *Auf diese Organisation anwenden* lädt die Konfiguration und baut die Schemas der Organisation neu auf. Ein Import wird immer mit dem zusammengeführt, was die Organisation bereits hat.

Endpunkte: `POST /admin/import/hasura/preview` und `POST /admin/import/hasura/apply`.

---

## Hasura v2

### Metadaten exportieren {: #export-metadata }

Beide Export-Layouts lassen sich konvertieren.

**Das Metadatenverzeichnis der CLI.** `hasura metadata export` schreibt ein Verzeichnis. `databases/databases.yaml` listet jede Quelle mit Art und Verbindung auf und verweist über `!include`-Indizes auf ihre Tabellen und Funktionen:

```bash
hasura metadata export
```

```yaml
# metadata/databases/databases.yaml
- name: default
  kind: postgres
  configuration:
    connection_info:
      database_url:
        from_env: HASURA_GRAPHQL_DATABASE_URL
  tables: "!include default/tables/tables.yaml"
  functions: "!include default/functions/functions.yaml"
```

Das flache Layout (`tables.yaml` und `actions.yaml` im Wurzelverzeichnis) lässt sich ebenfalls konvertieren.

**Das einzelne Dokument der Metadaten-API.** Die Metadaten-API liefert den gesamten Export als ein Objekt, mit der `configuration` je Quelle, für die das Verzeichnis-Layout keinen Platz hat:

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

Sowohl das reine Objekt als auch die Hülle `{resource_version, metadata}` werden akzeptiert. Der Admin-Import nimmt diese Form entgegen (siehe [Interaktiver Import](#interactive-import-admin-import-hasura-config)); der Kommandozeilen-Konverter liest nur Verzeichnisse.

### Konvertieren {: #convert }

Der Kommandozeilen-Konverter liest ein Hasura-Metadaten-**Verzeichnis** (das von `hasura metadata export` erzeugte Layout oder das flache `tables.yaml`/`actions.yaml`-Layout) und schreibt eine Provisa-Konfiguration:

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Lassen Sie `-o` weg, um die Konfiguration nach stdout zu schreiben. Bleibt etwas von Hand nachzuarbeiten, wird der Bericht nach stderr ausgegeben.

Flags:

| Flag | Zweck |
| ------ | --------- |
| `-o`, `--output` | Ausgabepfad für YAML (Standard: stdout) |
| `--source-overrides` | YAML-Datei mit Verbindungs-Overrides pro Quelle (Host, Port, Anmeldedaten), nach Quellenname indiziert |
| `--domain-map` | Schema-zu-Domäne-Zuordnungen als `SCHEMA=DOMAIN`-Paare |
| `--auth-env-file` | `.env`-Datei mit Auth-Konfiguration; konvertiert JWT/JWK, Admin-Secret und Claims-Map |
| `--dry-run` | Parsen und Validieren ohne Ausgabe zu schreiben |

### Der Bericht {: #the-report }

Jede Konvertierung sortiert, was sie findet, in drei Teile. Lesen Sie sie in dieser Reihenfolge.

1. **Unverändert übernommen.** Das Objekt hat eine direkte Provisa-Entsprechung und wird ohne Bedeutungsänderung konvertiert: Quellen, Tabellen, genehmigte Beziehungen, Select-Berechtigungen, Cron Triggers.
2. **Auf ein besseres Provisa-Feature abgebildet.** Provisa hat ein Feature, das die Aufgabe besser erledigt als das Hasura-Pendant, daher wird das Objekt zu diesem Feature. Jeder Eintrag nennt den Grund. Hier landen Event Triggers, Actions, Remote Schemas, zusätzliche Namen von Wurzelfeldern und von mehreren Rollen geerbte Rollen.
3. **Von Hand nacharbeiten.** Das Objekt hat keine Provisa-Entsprechung, oder dem Export fehlt, was die Konvertierung braucht. Jeder Eintrag nennt, was zu tun ist. Das sind die Warnungen.

Die Vorschau liefert die drei Teile als Listen. Der Kommandozeilen-Konverter gibt sie auf stderr als `Carried as`, `Mapped to` und `Redo by hand` mit jeweils einer Anzahl aus. Eine Konvertierung ohne Einträge im dritten Teil ist abgeschlossen.

### Was konvertiert wird {: #what-gets-converted }

| Hasura-Konzept | Teil | Provisa-Ergebnis |
| --------------- | ---- | -------------- |
| Quelle (`kind`, `connection_info`) | übernommen | `sources[]`. Eine im Export ausgeschriebene Verbindung wird zu Host, Port, Datenbank und Anmeldedaten. Eine aus einer Umgebungsvariable gelesene wird nicht erraten (siehe [Verbindungen und Domänen](#connections-and-domains-on-the-import-tab)). |
| Getrackte Tabelle | übernommen | Eine registrierte Tabelle. Ihr benutzerdefinierter Name (oder ihr Wurzelfeld `select`) wird zum Tabellen-Alias. |
| Benutzerdefinierte Spaltennamen (`column_config` oder das ältere `custom_column_names`) | übernommen | Spalten-`alias`. |
| Wurzelfeldnamen über `select` hinaus (`select_by_pk`, `select_aggregate`, `insert`, ...) | abgebildet | Namentlich gemeldet. Provisa stellt eine Tabelle als ein Feld bereit: Ein Lesezugriff über den Schlüssel ist ein Filter darauf, ein Aggregat ihr Feld `_aggregate`, ein Schreibvorgang ihre Mutation. Prüfen Sie, ob die Abfragen der App diese Form haben. |
| Object- und Array-Relationship | übernommen | Eine genehmigte Beziehung (`many-to-one` für Object, `one-to-many` für Array), deren GraphQL-Feld der Name der Hasura-Relationship ist. Die abweichenden Fälle stehen unten. |
| Select-Berechtigung | übernommen | Spaltenberechtigungen (`visible_to`) und eine Zeilenregel. Ein Term mit Sitzungsvariable (`X-Hasura-User-Id`) wird zu `current_setting('provisa.hasura_user_id')`, das die Anfrage zur Abfragezeit aus den Claims der Identität oder den Attributen des Mitglieds bindet. Eine Hasura-Sitzungsvariable behält ihren eigenen Namen; `provisa.user_id` bleibt die angemeldete Identität. |
| Zeilenregel über eine Relationship | übernommen | Eine `EXISTS`-Unterabfrage über die verbundene Tabelle. |
| Insert-, Update- und Delete-Berechtigung | übernommen | Schreibberechtigungen auf der Tabelle: eine je Rolle und Operation. `columns` werden zu den Spalten der Berechtigung, `filter` zu ihrem `rows`, `check` zu ihrem `check` und `set` zu ihren Presets. |
| Geerbte Rolle von einer Rolle | übernommen | Diese Rolle als `parent_role_id`. |
| Geerbte Rolle von mehreren Rollen | abgebildet | Eine Rolle, die die Vereinigung ihrer Berechtigungen hält, ausgeschrieben: Eine Provisa-Rolle hat einen Elternteil. |
| Computed Field | nacharbeiten | Eine Spalte einer Sicht über die Tabelle. |
| Getrackte Funktion | übernommen | Ein Befehl; siehe unten. Andernfalls nacharbeiten. |
| Action | abgebildet | Ein Befehl, der den Handler der Action aufruft. |
| Cron Trigger | übernommen | Ein geplanter Trigger. |
| Event Trigger | abgebildet | Ein Trigger auf Änderungen der Tabelle. |
| Remote Schema | abgebildet | Eine `graphql_remote`-Quelle, deren Wurzelfelder als governte Tabellen landen. |

Fälle bei Relationships:

- **Kollision.** Hat die Tabelle bereits ein Feld mit dem Namen der Relationship, erhält die Relationship den Namen, den Provisa vergibt, und der Bericht warnt. Benennen Sie sie auf der Seite Relationships um, wenn die App einen anderen Namen braucht.
- **Derselbe Join zweimal.** Eine zweite Relationship, die einen bereits deklarierten Join deklariert, wird mit der ersten zusammengeführt. Der Bericht vermerkt das unter „übernommen".
- **Mehrere Spaltenpaare.** Eine Provisa-Beziehung verbindet über ein Spaltenpaar. Eine Relationship über mehrere wird gewarnt und nicht angelegt, Zeilenregeln über sie werden aber übernommen.
- **Nur Fremdschlüsselspalte.** Eine Object-Relationship, die mit `foreign_key_constraint_on: artist_id` deklariert ist, nennt im Export kein Ziel. Der Konverter löst sie über die inverse Array-Relationship auf und warnt, wenn es keine gibt.

Details zu Schreibberechtigungen:

- Ein leeres `filter` oder `check` steht für jede Zeile, geschrieben als `TRUE`.
- Ein `set`-Wert, der eine Sitzungsvariable ist (`X-Hasura-User-Id`), wird zu einem `session`-Preset der passenden Sitzungsvariable (`X-Hasura-User-Id` wird zu `hasura_user_id`). Jeder andere Wert wird zu einem `literal`-Preset. Eine Preset-Spalte steht nicht zusätzlich in den Spalten der Berechtigung.
- Das implizite Hasura-admin besitzt jede Berechtigung. Es wird zur Rolle `org_admin`, die eine Insert-, Update- und Delete-Berechtigung über jede Spalte und jede Zeile hält und in jeder importierten Spalte genannt wird.
- Jede Rolle mit einer Insert-, Update- oder Delete-Berechtigung erhält außerdem das Recht `write`.

Details zu Befehlen:

- **Getrackte Funktion.** Der Export nennt die Funktion und die Rollen, die sie aufrufen, nicht aber ihre Argumente oder ihren Rückgabewert. Die Vorschau liest beides aus dem Katalog der Datenbank und registriert einen Befehl mit der gefundenen Signatur. Wird die Quelle nicht erreicht, ist sie nicht PostgreSQL, oder die Funktion fehlt, ist überladen oder gibt Zeilen einer Tabelle zurück, die der Import nicht registriert, geht die Funktion in die Nacharbeit. Der Kommandozeilen-Konverter erreicht nie eine Datenbank, daher geht jede getrackte Funktion, die er findet, in die Nacharbeit.
- **Action.** Der Befehl ruft den Handler der Action als `POST` auf. Ein `{{VAR}}` in der Handler-URL wird als `${env:VAR}` geschrieben und beim Aufruf des Befehls aufgelöst. Der Befehl ist für die Rollen sichtbar, die die Berechtigungen der Action nennen, oder nur für `org_admin`, wenn sie keine nennen. Eine asynchrone Action wird gewarnt: Ihr Befehl wartet auf die Antwort des Handlers. Ein Rückgabetyp, dessen Felder der Export nicht trägt, wird gewarnt.
- **Event Trigger.** Der Trigger feuert bei jeder Änderung der Tabelle, einmal je Änderungsstapel, und sendet die Tabelle und die geänderten Zeilen. Er feuert nicht je Zeile mit alten und neuen Werten. Ein Hasura-Trigger auf weniger als alle drei Operationen wird gewarnt, da der Provisa-Trigger bei allen feuert. Wiederholungen werden nicht übernommen: Ein Hasura-Trigger mit `num_retries` wird gewarnt, weil ein Provisa-Trigger einmal aufruft. Wiederholen Sie im Handler, wenn ein verpasster Aufruf zählt.
- **Cron Trigger.** Der Cron-Ausdruck, die URL, die Payload und das Enabled-Flag werden übernommen. Wiederholungen werden nicht übernommen und genauso gewarnt.

Ein konsolidiertes Dokument enthält auch Abschnitte, für die der Konverter kein Ziel hat: `query_collections`, `allowlist`, `rest_endpoints`, `network` und `opentelemetry`. Ist einer vorhanden, führt der Bericht ihn unter „nacharbeiten" auf. Der Verzeichnis-Reader öffnet ihre Dateien nie.

### Verbindungen und Domänen auf dem Import-Tab {: #connections-and-domains-on-the-import-tab }

Eine Verbindung, die der Export ausschreibt, wird wie geschrieben konvertiert. Liest der Export eine Datenbank-URL aus einer Umgebungsvariable (`database_url: {from_env: HASURA_GRAPHQL_DATABASE_URL}`), steht der Wert der Variable nicht im Export, daher errät die Konvertierung keinen. Die Quelle wird mit leeren Verbindungsfeldern registriert, und der Bericht verlangt Nacharbeit: Tragen Sie Host, Port, Datenbank, Benutzername und Passwort der Quelle in den Quelleinstellungen des Imports ein und konvertieren Sie erneut. Nur die geänderten Felder reisen mit, als Quell-Überschreibungen. Die Domänenzeilen decken jedes Schema, jeden Subgraphen und jedes Remote-Schema ab, das der Upload trägt; jede ist eine Auswahl über die bestehenden Domänen der Organisation, die auch einen eingegebenen Namen annimmt, markiert als „neue Domäne", wenn er keiner entspricht. Anwenden führt mit dem zusammen, was die Organisation bereits hat.

### Typen kommen bei der Vorschau aus der Quelle {: #types-come-from-the-source-at-preview }

Ein Hasura-Export benennt Spalten ohne Typen, und eine verfolgte Tabelle ohne Berechtigung nennt keine Spalten. Die Vorschau läuft mit den Quellverbindungen, die Sie angeben, und liest daher die `information_schema.columns` jeder erreichbaren SQL-Quelle: Jede typlose Spalte erhält den Typ der Quelle, abgebildet auf das IR-Vokabular, und eine Tabelle ohne Spalten nimmt jede Spalte, die die Quelle hat, sichtbar nur für `org_admin`, da Hasura sie keiner anderen Rolle offengelegt hat. Eine Quelle, die die Vorschau nicht erreicht, wird als `[sources]`-Warnung gemeldet, und ihre Spalten bleiben typlos, damit Sie sie vor dem Anwenden vervollständigen.

### Einschränkungen {: #limitations }

- **Computed Fields** werden von Hand nachgearbeitet: Der Export nennt die Datenbankfunktion, definiert sie aber nicht. Fügen Sie sie als Spalte einer Sicht über die Tabelle hinzu oder berechnen Sie sie in der App.
- **Remote Schemas** landen als Tabellen aus den Rollen-Berechtigungs-SDLs. Ein Remote Schema ohne Berechtigungen landet nichts, da der Export keine andere Angabe seiner Form trägt.
- **Getrackte Funktionen** brauchen die Datenbank zur Vorschauzeit, um ihre Signatur zu lesen; offline werden sie auf der Seite Commands nachgearbeitet.
- **Wiederholungen bei Event und Cron Triggers** werden nicht übernommen.

---

## Hasura DDN (v3)

### HML-Projekt lokalisieren {: #locate-the-hml-project }

Der DDN-Konverter liest das DDN-Projekt-**Verzeichnis** mit `.hml`-Dateien direkt, ohne Supergraph-Build-Schritt. Die erste Verzeichniskomponente unter der Projektwurzel wird als Subgraph-Name übernommen; Dateien unter `globals/` werden dem Subgraph `globals` zugeordnet.

### Konvertieren {: #convert_1 }

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

Lassen Sie `-o` weg, um die Konfiguration nach stdout zu schreiben.

Flags:

| Flag | Zweck |
| ------ | --------- |
| `-o`, `--output` | Ausgabepfad für YAML (Standard: stdout) |
| `--source-overrides` | YAML-Datei mit Verbindungs-Overrides pro Quelle |
| `--domain-map` | Subgraph-zu-Domäne-Zuordnungen als `SUBGRAPH=DOMAIN`-Paare |
| `--aggregates-output` | Ausgabepfad für die Aggregate-Expressions-Sidecar-Datei (Standard: `<output>-aggregates.yaml`) |
| `--dry-run` | Parsen und Validieren ohne Ausgabe zu schreiben |

Die `AggregateExpression`-Metadaten werden in einer Sidecar-Datei `*-aggregates.yaml` erhalten.

### Was konvertiert wird {: #what-gets-converted_1 }

| DDN-Konzept | Provisa-Entsprechung |
| ------------ | ------------------- |
| Subgraph-Modell | `tables[]` unter einer Quelle |
| Relationship | `relationships[]` |
| Berechtigungsregel | RLS-Filter |
| Command | Webhook-Mutation oder Sicht |
| Connector | Quelleintrag mit Verbindungsdetails |

### Einschränkungen {: #limitations_1 }

- **Lambda-Connectors** (TypeScript-/Python-Funktionen) erfordern manuelle Webhook-Einrichtung
- **Lifecycle-Plugins** haben keine direkte Entsprechung
- **DDN-Auth-Modi** werden auf Provisa-Auth-Provider abgebildet, aber JWT-Claim-Pfade müssen eventuell angepasst werden

---

## Nach dem Import {: #after-import }

1. Prüfen Sie die generierte `config.yaml` und den Bericht. Beginnen Sie mit den Einträgen unter `Redo by hand`
2. Verifizieren Sie die Verbindungsanmeldedaten. Eine Verbindung, die der Export aus einer Umgebungsvariable las, kommt leer an; der Bericht nennt sie
3. Starten Sie Provisa und bestätigen Sie, dass Tabellen im Explorer erscheinen
4. Führen Sie Ihre bestehenden GraphQL-Abfragen aus; das Schema ist für gängige Muster kompatibel
5. Reichen Sie Abfragen zur Genehmigung über die Admin-API oder -UI ein, bevor Sie die Produktions-Governance aktivieren
