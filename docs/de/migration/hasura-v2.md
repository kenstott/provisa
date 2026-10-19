# Migration von Hasura v2 zu Provisa {: #migrating-from-hasura-v2-to-provisa }

## Voraussetzungen {: #prerequisites }

1. Eine laufende Hasura-v2-Instanz (v2.x) mit exportierten Metadaten.
2. Exportieren Sie die Metadaten in einem der beiden Layouts:

   **Das Metadatenverzeichnis der CLI:**

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   Dadurch wird ein Verzeichnis `metadata/` erstellt. `databases/databases.yaml` listet jede Quelle
   mit Art und Verbindung auf und verweist über `!include`-Indizes auf ihre Tabellen und Funktionen;
   `actions.yaml`, `cron_triggers.yaml`, `inherited_roles.yaml`, `remote_schemas.yaml` und die
   übrigen Dateien liegen daneben. Das ältere flache Layout (`tables.yaml` im Wurzelverzeichnis)
   lässt sich ebenfalls konvertieren.

   **Das einzelne Dokument der Metadaten-API:**

   ```bash
   curl -X POST http://localhost:8080/v1/metadata \
     -H "X-Hasura-Admin-Secret: <secret>" \
     -d '{"type":"export_metadata","args":{}}' > metadata.json
   ```

   Sowohl das reine Objekt als auch die Hülle `{resource_version, metadata}` funktionieren. Der
   Admin-Import (Admin, Hasura-Konfiguration importieren) nimmt diese Form entgegen; der
   nachfolgende Kommandozeilen-Konverter liest Verzeichnisse.
3. Python 3.11+ mit installiertem Paket `provisa`.

## Verwendung der CLI {: #cli-usage }

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### Argumente {: #arguments }

| Argument | Erforderlich | Beschreibung |
| ---------- | ---------- | ------------- |
| `metadata_dir` | Ja | Pfad zum exportierten Hasura-v2-Metadatenverzeichnis |

### Optionen {: #options }

| Option | Standard | Beschreibung |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | Pfad der YAML-Ausgabedatei |
| `--source-overrides FILE` | Keine | YAML-Datei mit Verbindungsüberschreibungen pro Quelle |
| `--domain-map KEY=VAL ...` | Keine | Schema-zu-Domäne-Zuordnungen (z. B. `public=core hr=people`) |
| `--auth-env-file FILE` | Keine | Pfad zur `.env`-Datei mit der JWT/Admin-Secret-Authentifizierungskonfiguration |
| `--dry-run` | aus | Analysiert und validiert, ohne die Ausgabe zu schreiben |

### Datei mit Quellenüberschreibungen {: #source-overrides-file }

Eine YAML-Datei, die nach Quellenname indiziert ist und Verbindungseigenschaften enthält, die überschrieben werden sollen:

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### Datei für die Authentifizierungsumgebung {: #auth-env-file }

Eine Datei im `.env`-Stil mit der zu konvertierenden Hasura-Authentifizierungskonfiguration. Der Konverter
ordnet Folgendes zu:

- JWT mit `jwk_url` -> Provisa `provider: oauth`.
- JWT `claims_map` -> Provisa `role_mapping[]`.
- Admin Secret -> Provisa `superuser`.
- Webhook-Authentifizierung -> es wird eine Warnung ausgegeben (kein Provisa-Äquivalent).

## Der Bericht {: #the-report }

Die Konvertierung endet mit einem Bericht in drei Teilen:

1. **Unverändert übernommen.** Das Objekt hat eine Provisa-Entsprechung und wird ohne Bedeutungsänderung konvertiert.
2. **Auf ein besseres Provisa-Feature abgebildet.** Jeder Eintrag nennt, warum das Provisa-Feature die Aufgabe besser erledigt.
3. **Von Hand nacharbeiten.** Jeder Eintrag nennt, was zu tun ist. Das sind die Warnungen.

Der Kommandozeilen-Konverter gibt den Bericht auf stderr als `Carried as`, `Mapped to` und `Redo by hand` mit jeweils einer Anzahl aus, wenn etwas nachzuarbeiten bleibt. Der Admin-Import liefert die drei Listen mit der Vorschau.

## Feature-Paritätsmatrix {: #feature-parity-matrix }

| Hasura-v2-Feature | Teil | Provisa-Entsprechung | Hinweise |
| --- | --- | --- | --- |
| **Quellen** (postgres, mysql, mssql, bigquery, citus) | übernommen | `sources[]` | Art abgebildet: pg/postgres -> postgresql, mssql -> sqlserver. Eine ausgeschriebene Verbindungs-URL wird in Host/Port/Datenbank/Benutzername/Passwort zerlegt. Pool-Größe bleibt erhalten. Eine aus einer Umgebungsvariable gelesene URL wird nicht erraten (siehe unten). |
| **Tabellen** (getrackte Tabellen) | übernommen | `tables[]` | Schema und Tabellenname bleiben erhalten. `source_id` verknüpft mit der Quelle. |
| **Benutzerdefinierte Tabellennamen** (`custom_name`, `custom_root_fields.select`) | übernommen | `tables[].alias` | Der erste Nicht-Null-Wert aus `select`, `select_by_pk`, `custom_name`. |
| **Benutzerdefinierte Spaltennamen** (`column_config`, älteres `custom_column_names`) | übernommen | `columns[].alias` | Der benutzerdefinierte Name jeder Spalte wird zu ihrem Alias. |
| **Weitere Wurzelfeldnamen** (`select_by_pk`, `select_aggregate`, `insert`, ...) | abgebildet | Das eine Feld der Tabelle | Namentlich gemeldet. Ein Lesezugriff über den Schlüssel ist ein Filter auf der Tabelle, ein Aggregat ihr Feld `_aggregate`, ein Schreibvorgang ihre Mutation. |
| **Select-Berechtigungen** (Spalten, Filter) | übernommen | `columns[].visible_to[]`, `rls_rules[]` | Spaltenlisten werden zu `visible_to`; `*` wird unterstützt. Filter werden zu SQL; Sitzungsvariablen werden zu `current_setting('provisa.<name>')`. |
| **Filter über Relationships** | übernommen | `EXISTS`-Unterabfragen in `rls_rules[]` | Die Regel folgt der Relationship bis zu ihrer Tabelle. |
| **Insert-, Update- und Delete-Berechtigungen** | übernommen | `tables[].write_grants[]` | Eine Berechtigung je Rolle und Operation: Spalten aus `columns`, `rows` aus `filter`, `check` aus `check`, Presets aus `set`. Rollen erhalten das Recht `write`. |
| **Admin-Rolle** | übernommen | `org_admin` | Hält jede Berechtigung über jede Spalte und Zeile und wird in jeder importierten Spalte genannt. |
| **Object-Relationships** | übernommen | `relationships[]` mit `cardinality: many-to-one` | Genehmigt. Das GraphQL-Feld ist der Name der Hasura-Relationship. |
| **Array-Relationships** | übernommen | `relationships[]` mit `cardinality: one-to-many` | Genehmigt. Das GraphQL-Feld ist der Name der Hasura-Relationship. |
| **Geerbte Rollen** (eine Rolle) | übernommen | `roles[].parent_role_id` | Die Rolle wird zum Elternteil. |
| **Geerbte Rollen** (mehrere Rollen) | abgebildet | Eine Rolle, die die Vereinigung ihrer Berechtigungen hält | Provisa-Rollen haben einen Elternteil, daher wird die Vereinigung auf der Rolle ausgeschrieben. |
| **Computed Fields** | nacharbeiten | Eine Sichtspalte | Der Export nennt die Funktion, nicht ihren Rumpf. Der Bericht nennt das hinzuzufügende `SELECT`. |
| **Getrackte Funktionen** | übernommen, sonst nacharbeiten | `functions[]`-Befehle | Die Vorschau liest die Signatur aus der Datenbank. Ohne sie fügen Sie die Funktion auf der Seite Commands hinzu. |
| **Actions** | abgebildet | Befehle, die den Handler aufrufen | `{{VAR}}` in der Handler-URL wird als `${env:VAR}` geschrieben und beim Aufruf des Befehls aufgelöst. |
| **Cron Triggers** | übernommen | `scheduled_triggers[]` | Cron-Ausdruck, URL, Payload und Enabled-Flag. Wiederholungen werden nicht übernommen (gewarnt). |
| **Event Triggers** | abgebildet | `scheduled_triggers[]` auf die Änderung der Tabelle | Feuert je Änderungsstapel bei jeder Änderung. Wiederholungen werden nicht übernommen (gewarnt). |
| **Remote Schemas** | abgebildet | `sources[]` (`graphql_remote`), als Tabellen gelandet | Name, URL, Header und Authentifizierung bleiben erhalten; Wurzelfelder werden zu governten Tabellen. |
| **Enum-Tabellen** | übernommen | Tabelle angelegt | Das Flag `is_enum` wird nicht übernommen (keine Provisa-Entsprechung). |
| **Allow Lists, Query Collections, REST-Endpunkte** | nacharbeiten | Keine | Werden gemeldet, wenn sie in einem konsolidierten Export vorkommen; nicht konvertiert. |

### Relationships {: #relationships }

- Das GraphQL-Feld jeder Relationship ist der Hasura-Name. Hat die Tabelle bereits ein Feld mit diesem Namen, erhält die Relationship den Namen, den Provisa vergibt, und der Bericht warnt.
- Zwei Relationships, die denselben Join deklarieren, werden zu einer zusammengeführt. Der Bericht nennt, welche in welche eingegangen ist.
- Eine Relationship über mehrere Spaltenpaare wird gewarnt und nicht angelegt, da eine Provisa-Beziehung über ein Paar verbindet. Zeilenregeln über sie werden dennoch konvertiert.
- Eine allein über eine Fremdschlüsselspalte deklarierte Object-Relationship wird über die inverse Array-Relationship aufgelöst; ohne eine solche wird gewarnt.

### Schreibberechtigungen {: #write-grants }

Eine Berechtigung legt fest, was eine Rolle mit einer Operation in eine Tabelle schreiben darf. Ein leeres `filter` oder `check` steht für jede Zeile (`TRUE`). Ein `set`-Wert `X-Hasura-User-Id` wird zu einem `session`-Preset von `hasura_user_id`; jeder andere Wert wird zu einem `literal`-Preset. Eine Preset-Spalte steht nicht zusätzlich in den Spalten der Berechtigung.

### Actions, getrackte Funktionen und Trigger {: #actions-tracked-functions-and-triggers }

- Eine **Action** wird zu einem Befehl, der ihren Handler aufruft, sichtbar für die Rollen, die ihre Berechtigungen nennen (nur für `org_admin`, wenn sie keine nennen). Eine asynchrone Action wird gewarnt: Der Befehl wartet auf die Antwort des Handlers.
- Eine **getrackte Funktion** wird zu einem Befehl, wenn die Vorschau ihre Argumente und ihre Rückgabetabelle aus einer PostgreSQL-Quelle lesen kann. Offline, und für jede Funktion, die fehlt, überladen ist oder eine Tabelle zurückgibt, die der Import nicht registriert, geht sie in die Nacharbeit.
- Ein **Event Trigger** feuert bei jeder Änderung seiner Tabelle, einmal je Stapel, und sendet die Tabelle und die geänderten Zeilen statt eines Ereignisses je Zeile mit alten und neuen Werten. Ein Trigger, der auf weniger als alle drei Operationen hörte, wird gewarnt.

## Schritte nach der Konvertierung {: #post-conversion-steps }

1. **Prüfen Sie den Bericht.** Beginnen Sie mit `Redo by hand`.
2. **Quellverbindungen konfigurieren.** Eine Verbindung, die der Export ausschreibt, wird wie geschrieben konvertiert. Eine aus einer Umgebungsvariable gelesene kommt leer an. Verwenden Sie `--source-overrides` oder bearbeiten Sie die Ausgabe direkt.
3. **Domänenzuordnungen prüfen.** Ohne `--domain-map` landen alle Tabellen in `default`.
   Ordnen Sie Schemas mit `--domain-map public=core analytics=reporting` Domänen zu.
4. **Zeilenregeln prüfen.** Filter werden zu SQL konvertiert. Prüfen Sie komplexe boolesche
   Ausdrücke (verschachtelte `_and`/`_or`/`_exists`) und Filter über Relationships.
5. **Aufgelistetes nacharbeiten.** Computed Fields, getrackte Funktionen, die die Vorschau nicht lesen konnte,
   Wiederholungen bei Triggern und Webhook-Authentifizierung werden nicht automatisch konvertiert.
6. **Authentifizierung einrichten.** Wenn Ihre Hasura-Instanz JWT-/Webhook-Authentifizierung verwendet, erstellen Sie eine
   Auth-Env-Datei und führen Sie den Lauf mit `--auth-env-file` erneut aus.
7. **Sitzungsvariablen senden.** Konfigurieren Sie den Identitätsanbieter so, dass das Token jede Sitzungsvariable trägt, die der Bericht unter „zugeordnet zu“ aufführt (zum Beispiel `x-hasura-user-id`, auf oberster Ebene oder im Hasura-Claims-Objekt), oder erfassen Sie sie je Mitglied als Attribut.
8. **Testen.** Starten Sie den Provisa-Server und prüfen Sie Abfragen gegen Ihre Datenquellen.

## Häufige Probleme und Fehlerbehebung {: #common-issues-and-troubleshooting }

### Die Verbindung kommt aus einer Umgebungsvariable {: #connection-comes-from-an-environment-variable }

Ist die `database_url` der Quelle eine Referenz auf eine Umgebungsvariable (`{"from_env": "PG_URL"}`),
steht ihr Wert nicht im Export. Der Konverter errät keinen: Die Quelle hat leere Werte für
Host, Datenbank, Benutzername und Passwort, und der Bericht führt sie unter `Redo by hand` auf.
Tragen Sie die Verbindung in den Quelleinstellungen des Imports (oder mit `--source-overrides`) ein und konvertieren Sie
erneut.

### Platzhalterspalten {: #wildcard-columns }

Wenn eine Berechtigung `columns: "*"` gewährt, erstellt der Konverter einen einzelnen
Platzhalter-Spalteneintrag. Ersetzen Sie ihn nach der Konvertierung durch eine explizite Spaltenliste
aus dem Datenbankschema.

### Genauigkeit von Event-Triggern {: #event-trigger-fidelity }

Ein Event Trigger wird zu einem Trigger auf Änderungen seiner Tabelle, der denselben Webhook aufruft. Er feuert
je Änderungsstapel bei jeder Änderung, ob die Änderung über Provisa oder direkt in die Datenbank kam, und sendet die Tabelle und die
geänderten Zeilen. Die alten und neuen Werte je Zeile, die Filterung je Operation und Wiederholungen von Hasura werden nicht übernommen;
der Bericht warnt bei jedem, der zutrifft.
Lassen Sie den Handler auf die Zeilen reagieren, für die er gedacht ist, und wiederholen Sie in ihm, wenn ein verpasster Aufruf zählt.

### Fehlende Rollen {: #missing-roles }

Rollen werden aus Tabellenberechtigungen, Action-Berechtigungen und geerbten Rollen gesammelt. Existiert eine Rolle in Hasura,
kommt aber in keiner davon vor, erscheint sie nicht in der Ausgabe.

### Benutzerdefinierte Root-Felder {: #custom-root-fields }

Die Wurzelfeldnamen `select` und `select_by_pk` ergeben den Tabellen-Alias. Die übrigen benutzerdefinierten Wurzelfelder
(`select_aggregate`, `insert`, `update`, `delete`) werden namentlich gemeldet: Provisa stellt
eine Tabelle als ein Feld bereit, mit Aggregaten und Schreibvorgängen als ihrem Feld `_aggregate` und ihren Mutationen.

## Beispiel {: #example }

Konvertierung eines typischen Hasura-v2-Projekts mit zwei Schemas, die Domänen zugeordnet sind:

```bash
# Export metadata from Hasura
hasura metadata export --endpoint http://localhost:8080

# Convert with domain mapping and source overrides
python -m provisa.hasura_v2 metadata/ \
  -o provisa.yaml \
  --domain-map public=core hr=people \
  --source-overrides overrides.yaml \
  --auth-env-file auth.env

# Dry run first to check for warnings
python -m provisa.hasura_v2 metadata/ --dry-run
```

Struktur der Ausgabe (gekürzt):

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
domains:
  - id: core
  - id: people
tables:
  - source_id: default
    domain_id: core
    schema_name: public
    table_name: users
    columns:
      - name: id
        visible_to: [org_admin, user]
      - name: email
        visible_to: [org_admin, user]
    write_grants:
      - role: user
        operation: update
        columns: [email]
        rows: "id = current_setting('provisa.hasura_user_id')"
        check: "TRUE"
      - role: org_admin
        operation: insert
        columns: [id, email]
        check: "TRUE"
      - role: org_admin
        operation: update
        columns: [id, email]
        rows: "TRUE"
        check: "TRUE"
      - role: org_admin
        operation: delete
        rows: "TRUE"
roles:
  - id: user
    capabilities: [query_development, write]
    domain_access: ["*"]
rls_rules:
  - table_id: users
    role_id: user
    filter: "id = current_setting('provisa.hasura_user_id')"
relationships:
  - id: orders.user
    source_table_id: orders
    target_table_id: users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
    graphql_alias: user
```
