# Migration von Hasura DDN (v3) zu Provisa

## Voraussetzungen

1. Ein Hasura-DDN-Projekt mit HML-Dateien (Erweiterung `.hml`).
   DDN-Projekte haben typischerweise eine Verzeichnisstruktur wie:
   ```
   my-ddn-project/
     app/
       subgraph1/
         models/
           MyModel.hml
         commands/
           MyCommand.hml
       subgraph2/
         ...
     globals/
       ...
   ```
2. Python 3.11+ mit installiertem `provisa`-Paket.

## Verwendung der CLI

```bash
python -m provisa.ddn <hml-dir> -o provisa.yaml
```

### Argumente

| Argument | Erforderlich | Beschreibung |
|----------|----------|-------------|
| `hml_dir` | Ja | Pfad zum DDN-HML-Projektverzeichnis (wird rekursiv nach `.hml`-Dateien durchsucht) |

### Optionen

| Option | Standard | Beschreibung |
|--------|---------|-------------|
| `-o, --output FILE` | stdout | Pfad zur Ausgabe-YAML-Datei |
| `--source-overrides FILE` | Keine | YAML-Datei mit Verbindungsüberschreibungen pro Quelle |
| `--domain-map KEY=VAL ...` | Keine | Zuordnungen von Subgraph zu Domain (z. B. `app=core analytics=reporting`) |
| `--dry-run` | aus | Analysiert und validiert, ohne die Ausgabe zu schreiben |

### Datei für Quellüberschreibungen

Eine YAML-Datei, indiziert nach Connector-Namen (nach der ID-Bereinigung: Leerzeichen, Punkte
und Schrägstriche werden zu Unterstrichen) mit Verbindungseigenschaften:

```yaml
my_pg_connector:
  host: prod-db.example.com
  port: 5432
  database: chinook
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

## Feature-Paritätsmatrix

| DDN-Typ | Provisa-Äquivalent | Hinweise |
|---|---|---|
| **DataConnectorLink** | `sources[]` | Der Quelltyp wird aus der Connector-URL abgeleitet (postgres, mysql, mssql, mongo, clickhouse, snowflake, bigquery). Verbindungsdetails verwenden standardmäßig Platzhalter; verwenden Sie `--source-overrides`, um die tatsächlichen Werte festzulegen. |
| **ObjectType** | Spaltendefinitionen in `tables[]` | Felder werden zu Spalten. `dataConnectorTypeMapping.fieldMapping` löst GraphQL-Feldnamen zu physischen Spaltennamen auf. |
| **Model** | `tables[]` | Jedes Model erzeugt eine Tabelle. `source_id` stammt vom Connector, `table_name` von der Collection. `graphql_type_name` wird zu `alias`. Der Subgraph (und damit `domain_id`) wird aus dem Verzeichnis der Datei abgeleitet: der erste Verzeichnisbestandteil unter dem Projekt-Root. |
| **Relationship** | `relationships[]` | Object-Typ -> `many-to-one`, Array-Typ -> `one-to-many`. Die Feldzuordnung wird durch Nachschlagen der physischen Spalte aufgelöst. |
| **TypePermissions** | `columns[].visible_to[]` | `allowedFields` bestimmt, welche Rollen welche Spalte sehen können. |
| **ModelPermissions** | `rls_rules[]` | Filterprädikate werden in SQL-WHERE-Klauseln umgewandelt. Unterstützt `_eq`, `_neq`, `_gt`, `_lt`, `_gte`, `_lte`, `_in`, `_nin`, `_like`, `_is_null`, `_and`, `_or`, `_not`. Verweise auf Sitzungsvariablen bleiben als `${x-hasura-...}` erhalten. |
| **Command** | `functions[]` | Sowohl Funktionen als auch Prozeduren werden abgebildet. Argumente, Rückgabetyp und GraphQL-Root-Feldname bleiben erhalten. `domain_id` wird aus dem Subgraph gesetzt. |
| **AggregateExpression** | Sidecar-Datei `provisa-aggregates.yaml` | Count, count_distinct und Aggregatfunktionen pro Feld bleiben in einer Sidecar-Datei erhalten und werden in die Provisa-Aggregatkonfiguration umgewandelt. |
| **BooleanExpressionType** | Übersprungen (stillschweigend) | Wird intern von DDN zum Filtern verwendet; kein direktes Provisa-Äquivalent erforderlich. |
| **AuthConfig** | Übersprungen (stillschweigend) | DDN-Auth-Konfiguration wird nicht abgebildet; konfigurieren Sie die Provisa-Authentifizierung separat. |
| **ScalarType** | Übersprungen | Es wird eine Warnung mit Anzahl ausgegeben. |
| **GraphqlConfig** | Übersprungen | Es wird eine Warnung mit Anzahl ausgegeben. |
| **CompatibilityConfig** | Übersprungen | Es wird eine Warnung mit Anzahl ausgegeben. |
| **Andere nicht erkannte Typen** | Übersprungen | Es wird eine Warnung mit Anzahl pro Typ ausgegeben. |

## Kernkonzept: Auflösung von GraphQL-Feld zu physischer Spalte

DDN trennt das GraphQL-Schema (Feldnamen) vom physischen Datenbankschema
(Spaltennamen) über `dataConnectorTypeMapping` auf ObjectTypes. Der Konverter:

1. Liest `fieldMapping`-Einträge aus den Typzuordnungen jedes ObjectType.
2. Erstellt eine Nachschlagetabelle: `{graphql_field_name -> physical_column_name}`.
3. Nimmt bei Feldern ohne explizite Zuordnung an, dass der Feldname dem Spaltennamen entspricht.
4. Verwendet diese Nachschlagetabelle beim Erstellen von Spalten, Beziehungen und RLS-Filterausdrücken.

Das bedeutet, dass die Ausgabe `provisa.yaml` **physische Spaltennamen** für `columns[].name` verwendet
und `columns[].alias` auf den GraphQL-Feldnamen setzt, wenn diese abweichen.

## Schritte nach der Konvertierung

1. **Überprüfen Sie die YAML-Ausgabe.** Verifizieren Sie Quellen, Tabellen und Spaltenzuordnungen.
2. **Konfigurieren Sie die Quellverbindungen.** Connectors liefern nur einen URL-Hinweis für die
   Typerkennung. Der tatsächliche Host/Port/Datenbank/Zugangsdaten müssen über
   `--source-overrides` oder durch Bearbeiten der Ausgabe bereitgestellt werden.
3. **Überprüfen Sie die Domain-Zuordnungen.** Subgraph-Namen werden aus der Verzeichnisstruktur
   abgeleitet (der erste Verzeichnisbestandteil unter dem Projekt-Root). Ohne `--domain-map` wird
   jeder Subgraph-Name direkt zu einer Domain-ID. Verwenden Sie `--domain-map`, um sie umzubenennen.
4. **Prüfen Sie die RLS-Regeln.** DDN-Filterprädikate werden in SQL-Näherungen umgewandelt.
   Verschachtelte boolesche Logik (`_and`/`_or`/`_not`) wird unterstützt, aber komplexe,
   beziehungsübergreifende Filter erfordern möglicherweise eine manuelle Überprüfung.
5. **Überprüfen Sie die Aggregatkonfiguration.** Aggregatausdrücke werden in eine Sidecar-Datei
   `provisa-aggregates.yaml` geschrieben und in die Provisa-Aggregatkonfiguration umgewandelt.
6. **Überprüfen Sie die Warnungen.** Der Konverter gibt auf stderr eine Zusammenfassung mit
   übersprungenen DDN-Typen und Modellen aus, die auf unbekannte ObjectTypes verweisen.
7. **Testen Sie.** Starten Sie den Provisa-Server und überprüfen Sie Abfragen gegen Ihre Datenquellen.

## Häufige Probleme und Fehlerbehebung

### Erkennung des Quelltyps schlägt fehl

Die Connector-URL wird heuristisch verwendet (Suche nach Schlüsselwörtern wie „postgres",
„mysql", „mongo"). Wenn die URL kein erkennbares Schlüsselwort enthält, verwendet die Quelle
standardmäßig `postgresql`. Überschreiben Sie dies mit `--source-overrides`.

### Fehlender ObjectType für ein Model

Wenn ein Model auf einen ObjectType-Namen verweist, der in keiner `.hml`-Datei gefunden wurde,
wird die Tabelle übersprungen und eine Warnung ausgegeben. Stellen Sie sicher, dass alle HML-Dateien
im durchsuchten Verzeichnis enthalten sind.

### Subgraph-Erkennung

Subgraphs werden aus der Verzeichnisstruktur abgeleitet: Der erste Verzeichnisbestandteil unter
dem Projekt-Root wird als Subgraph-Name verwendet. Das Feld `subgraph` innerhalb von HML-Dokumenten
wird nicht verwendet. Dateien unter einem `globals/`-Verzeichnis werden dem Subgraph `globals`
zugeordnet und von der Domain-Erkennung ausgeschlossen.

### Auflösung der Beziehungsquelle

Beziehungen verweisen auf einen `source_type` (ObjectType-Namen) und ein `target_model` (Model-Namen).
Wenn kein Model den angegebenen ObjectType verwendet, wird die Beziehung stillschweigend übersprungen.

### Spaltenaliase überall

Wenn Ihr DDN-Projekt `fieldMapping` umfangreich verwendet, ist zu erwarten, dass die meisten Spalten
in der Ausgabe einen `alias` haben. Dies ist korrektes Verhalten -- `name` ist die physische Spalte,
`alias` ist der GraphQL-Name, den Ihre Anwendung verwendet hat.

### Aggregatausdrücke

Aggregatausdrücke bleiben in einer Sidecar-Datei `provisa-aggregates.yaml` erhalten, die neben der
Ausgabe geschrieben und in die Provisa-Aggregatkonfiguration umgewandelt wird. Sie werden nicht in der
`description` der Tabelle gespeichert.

## Beispiel: Konvertierung eines Chinook-DDN-Projekts

```bash
# Convert the DDN project
python -m provisa.ddn ./chinook-ddn/ \
  -o provisa.yaml \
  --domain-map app=music \
  --source-overrides overrides.yaml

# Dry run to check warnings first
python -m provisa.ddn ./chinook-ddn/ --dry-run
```

Ausgabestruktur:

```yaml
sources:
  - id: chinook_pg
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: chinook
    ...
domains:
  - id: music
tables:
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Album
    columns:
      - name: AlbumId
        visible_to: [admin, user]
      - name: Title
        visible_to: [admin, user]
      - name: ArtistId
        visible_to: [admin, user]
    alias: Albums
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Artist
    columns:
      - name: artist_id
        visible_to: [admin, user]
        alias: ArtistId
      - name: artist_name
        visible_to: [admin, user]
        alias: Name
    alias: Artists
roles:
  - id: admin
    capabilities: [read]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
relationships:
  - id: chinook_pg.public.Album.Artist
    source_table_id: chinook_pg.public.Album
    target_table_id: chinook_pg.public.Artist
    source_column: ArtistId
    target_column: artist_id
    cardinality: many-to-one
functions:
  - name: GetTopTracks
    source_id: chinook_pg
    schema_name: public
    function_name: get_top_tracks
    returns: Track
    domain_id: music
    description: "DDN function"
```
