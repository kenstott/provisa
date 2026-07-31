# Migration von Hasura v2 zu Provisa

## Voraussetzungen

1. Eine laufende Hasura-v2-Instanz (v2.x) mit exportierten Metadaten.
2. Metadaten mit der Hasura-CLI exportieren:

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   Dadurch wird ein Verzeichnis `metadata/` erstellt, das `sources.yaml`, `actions.yaml`,
   `cron_triggers.yaml`, `inherited_roles.yaml`, `remote_schemas.yaml` usw. enthält.
3. Python 3.11+ mit installiertem Paket `provisa`.

## Verwendung der CLI

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### Argumente

| Argument | Erforderlich | Beschreibung |
| ---------- | ---------- | ------------- |
| `metadata_dir` | Ja | Pfad zum exportierten Hasura-v2-Metadatenverzeichnis |

### Optionen

| Option | Standard | Beschreibung |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | Pfad der YAML-Ausgabedatei |
| `--source-overrides FILE` | Keine | YAML-Datei mit Verbindungsüberschreibungen pro Quelle |
| `--domain-map KEY=VAL ...` | Keine | Schema-zu-Domäne-Zuordnungen (z. B. `public=core hr=people`) |
| `--auth-env-file FILE` | Keine | Pfad zur `.env`-Datei mit der JWT/Admin-Secret-Authentifizierungskonfiguration |
| `--dry-run` | aus | Analysiert und validiert, ohne die Ausgabe zu schreiben |

### Datei mit Quellenüberschreibungen

Eine YAML-Datei, die nach Quellenname indiziert ist und Verbindungseigenschaften enthält, die überschrieben werden sollen:

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### Datei für die Authentifizierungsumgebung

Eine Datei im `.env`-Stil mit der zu konvertierenden Hasura-Authentifizierungskonfiguration. Der Konverter
ordnet Folgendes zu:

- JWT mit `jwk_url` -> Provisa `provider: oauth`.
- JWT `claims_map` -> Provisa `role_mapping[]`.
- Admin Secret -> Provisa `superuser`.
- Webhook-Authentifizierung -> es wird eine Warnung ausgegeben (kein Provisa-Äquivalent).

## Feature-Paritätsmatrix

| Hasura-v2-Feature | Provisa-Äquivalent | Hinweise |
| --- | --- | --- |
| **Quellen** (postgres, mysql, mssql, bigquery, citus) | `sources[]` | Typ zugeordnet: pg/postgres -> postgresql, mssql -> sqlserver. Die Verbindungs-URL wird in host/port/database/username/password zerlegt. Pool-Einstellungen bleiben erhalten. |
| **Tabellen** (nachverfolgte Tabellen) | `tables[]` | Schema und Tabellenname bleiben erhalten. `source_id` verknüpft mit der Quelle. |
| **Benutzerdefinierte Tabellennamen** (`custom_name`, `custom_root_fields.select`) | `tables[].alias` | Erster nicht-null Wert von `select`, `select_by_pk`, `custom_name`. |
| **Benutzerdefinierte Spaltennamen** | `columns[].alias` | Ordnet das Dictionary `custom_column_names` Spaltenaliassen zu. |
| **Select-Berechtigungen** (Spalten, Filter) | `columns[].visible_to[]`, `rls_rules[]` | Spaltenlisten werden zu `visible_to`. Platzhalterspalten (`*`) werden unterstützt. Filter werden über `bool_expr_to_sql` in SQL umgewandelt. |
| **Insert-/Update-Berechtigungen** (Spalten) | `columns[].writable_by[]` | Spaltenlisten werden zu `writable_by`. Rollen erhalten die Capability `write`. |
| **Delete-Berechtigungen** | Upgrade der Rollen-Capability | Die Rolle erhält die Capability `write`. Keine tabellenspezifische Delete-Zuordnung. |
| **Objektbeziehungen** | `relationships[]` mit `cardinality: many-to-one` | Die Spaltenzuordnung bleibt erhalten. |
| **Array-Beziehungen** | `relationships[]` mit `cardinality: one-to-many` | Die Spaltenzuordnung bleibt erhalten. |
| **Berechnete Felder** | `functions[]` | Werden einer Function zugeordnet, deren `returns` auf die ID der übergeordneten Tabelle verweist. |
| **Nachverfolgte Funktionen** | `functions[]` | `exposed_as` ist standardmäßig mutation. Das Schema bleibt erhalten. |
| **Actions** (Handler für gespeicherte Prozedur) | `functions[]` | Wird in eine Function-Konfiguration konvertiert, wenn eine gespeicherte Prozedur zugrunde liegt. |
| **Actions** (Webhook-Handler) | Nicht konvertiert | Es wird eine Warnung ausgegeben, einschließlich der Handler-URL. |
| **Cron-Trigger** | Nicht konvertiert | Es wird eine Warnung ausgegeben. (Zur Laufzeit geplante Trigger existieren, werden vom Konverter aber nicht zugeordnet.) |
| **Event-Trigger** | Nicht konvertiert | Es wird eine Warnung ausgegeben. (Zur Laufzeit existierende Event-Trigger werden vom Konverter nicht zugeordnet.) |
| **Vererbte Rollen** | `roles[].parent_role_id` | Die erste Rolle in `role_set` wird zur übergeordneten Rolle. Alle untergeordneten Rollen werden erstellt. |
| **Remote-Schemas** | `sources[]` (`graphql_remote`) | Werden als `graphql_remote`-Quelle registriert. Name, URL, Header und Authentifizierungskonfiguration bleiben erhalten. |
| **Enum-Tabellen** | Tabelle wird erstellt | Das Flag `is_enum` wird nicht übernommen (kein Provisa-Äquivalent). |
| **Allow-Listen** | Übersprungen | Im Metadatenmodell nicht vorhanden. |

## Schritte nach der Konvertierung

1. **Die YAML-Ausgabe prüfen.** Kontrollieren Sie, ob Quellen, Tabellen und Rollen korrekt aussehen.
2. **Quellverbindungen konfigurieren.** Der Konverter analysiert Verbindungs-URLs, verwendet bei einem
   Analysefehler jedoch standardmäßig `localhost`. Verwenden Sie `--source-overrides` oder bearbeiten Sie die Ausgabe direkt.
3. **Domänenzuordnungen prüfen.** Ohne `--domain-map` landen alle Tabellen in `default`.
   Weisen Sie Schemas mit `--domain-map public=core analytics=reporting` Domänen zu.
4. **RLS-Regeln prüfen.** Filter werden in SQL-Näherungen umgewandelt. Komplexe boolesche
   Ausdrücke (verschachtelte `_and`/`_or`/`_exists`) sollten manuell überprüft werden.
5. **Warnungen prüfen.** Der Konverter gibt auf stderr eine Zusammenfassung der Warnungen für
   Features aus, die er nicht zuordnen kann (Event-Trigger, Cron-Trigger, webhook-basierte Actions).
6. **Authentifizierung einrichten.** Wenn Ihre Hasura-Instanz JWT-/Webhook-Authentifizierung verwendet, erstellen Sie
   eine Authentifizierungsumgebungsdatei und führen Sie den Vorgang erneut mit `--auth-env-file` aus.
7. **Testen.** Starten Sie den Provisa-Server und überprüfen Sie Abfragen gegen Ihre Datenquellen.

## Häufige Probleme und Fehlerbehebung

### Verbindungs-URL wird nicht analysiert

Wenn `database_url` der Quelle eine Referenz auf eine Umgebungsvariable ist (`{"from_env": "PG_URL"}`),
kann der Konverter diese zum Zeitpunkt der Konvertierung nicht auflösen. Die Quelle erhält
Platzhalterwerte (`host: localhost`, `database: default`). Beheben Sie dies mit `--source-overrides`.

### Platzhalterspalten

Wenn eine Berechtigung `columns: "*"` gewährt, erstellt der Konverter einen einzelnen
Platzhalter-Spalteneintrag. Nach der Konvertierung möchten Sie diesen möglicherweise durch explizite
Spaltenlisten ersetzen, indem Sie das tatsächliche Datenbankschema prüfen.

### Genauigkeit von Event-Triggern

Event-Trigger werden mit `operations` und `webhook_url` konvertiert, aber Hasura-spezifische
Zustellgarantien (genau einmal, erneute Zustellung) haben in Provisa keine direkten Entsprechungen.
Prüfen Sie den Abschnitt `event_triggers` und konfigurieren Sie Ihre Webhook-Infrastruktur entsprechend.

### Fehlende Rollen

Rollen werden ausschließlich aus Berechtigungseinträgen erfasst. Wenn eine Rolle in Hasura existiert,
aber für keine Tabelle oder Action Berechtigungen hat, erscheint sie nicht in der Ausgabe.

### Benutzerdefinierte Root-Felder

Nur die Root-Felder `select` und `select_by_pk` werden für den Tabellenalias verwendet. Andere
benutzerdefinierte Root-Felder (`select_aggregate`, `insert`, `update`, `delete`) werden nicht zugeordnet.

## Beispiel

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

Ausgabestruktur:

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
    ...
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
        visible_to: [user, admin]
      - name: email
        visible_to: [admin]
        writable_by: [admin]
    alias: Users
roles:
  - id: admin
    capabilities: [read, write]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
rls_rules:
  - table_id: default.public.users
    role_id: user
    filter: "id = x-hasura-user-id"
relationships:
  - id: default.public.orders.user
    source_table_id: default.public.orders
    target_table_id: default.public.users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
```
