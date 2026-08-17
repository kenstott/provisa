# Import aus Hasura

Provisa kann bestehende Hasura-Metadaten in eine Provisa-`config.yaml` umwandeln und dabei getrackte Tabellen, Beziehungen, Berechtigungen und Remote Schemas erhalten.

## Interaktiver Import (Admin → Hasura-Konfiguration importieren)

Die Admin-Oberfläche nutzt dieselben Konverter, sodass ein Import weder Shell-Zugriff noch einen Config-Datei-Roundtrip benötigt. Erfordert die Capability `org_settings`; der Import landet in der Organisation, in der die Sitzung aktiv ist.

1. **Hochladen.** Wählen Sie ein gezipptes Hasura-v2-Metadatenverzeichnis, ein gezipptes DDN-Projekt, einen konsolidierten Metadaten-Export (`.yaml`/`.json`, einschließlich der `{resource_version, metadata}`-Hülle, die die Metadaten-API zurückgibt), oder eine einzelne `.hml`-Datei. Belassen Sie das Format bei *Automatisch erkennen*, außer der Upload ist mehrdeutig.
2. **Domänen zuordnen** (optional). Jedes Paar ordnet ein v2-Schema oder einen DDN-Subgraph einer Provisa-Domäne zu; alles Nicht-Zugeordnete behält seinen ursprünglichen Namen.
3. **Konvertieren und Vorschau.** Der Server konvertiert und liefert Anzahlen, Konverter-Warnungen und die generierte Konfiguration zurück. In diesem Schritt wird nichts geschrieben.
4. **Prüfen und bearbeiten.** Die Konfiguration ist direkt bearbeitbar — Verbindungsdetails, Domänennamen, Rollennamen. Was Sie anwenden, ist das, was angezeigt wird.
5. **Anwenden.** *Bestehende semantische Schicht ersetzen* löscht jede Quelle, Tabelle, Rolle und Regel, die in der Konfiguration fehlt; ist die Option deaktiviert, wird der Import mit dem zusammengeführt, was die Organisation bereits hat. Das Anwenden lädt die Konfiguration und baut die Schemas der Organisation neu auf.

Endpunkte: `POST /admin/import/hasura/preview` und `POST /admin/import/hasura/apply`.

---

## Hasura v2

### Metadaten exportieren

Über Ihre Hasura-Konsole oder -CLI:

```bash
hasura metadata export --output metadata.yaml
```

Oder über die Hasura-API:

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

### Konvertieren

Der v2-Konverter liest ein Hasura-Metadaten-**Verzeichnis** (das von `hasura metadata export` erzeugte Layout oder das flache `tables.yaml`/`actions.yaml`-Layout) und schreibt eine Provisa-Konfiguration:

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Lassen Sie `-o` weg, um die Konfiguration nach stdout zu schreiben.

Flags:

| Flag | Zweck |
| ------ | --------- |
| `-o`, `--output` | Ausgabepfad für YAML (Standard: stdout) |
| `--source-overrides` | YAML-Datei mit Verbindungs-Overrides pro Quelle (Host, Port, Anmeldedaten) |
| `--domain-map` | Schema-zu-Domäne-Zuordnungen als `SCHEMA=DOMAIN`-Paare |
| `--auth-env-file` | `.env`-Datei mit Auth-Konfiguration; konvertiert JWT/JWK, Admin-Secret und Claims-Map |
| `--dry-run` | Parsen und Validieren ohne Ausgabe zu schreiben |

### Was konvertiert wird

| Hasura-Konzept | Provisa-Entsprechung |
| --------------- | ------------------- |
| Getrackte Tabelle | `tables[]` mit `publish: true` |
| Object-Relationship | `relationships[]` mit `cardinality: many-to-one` |
| Array-Relationship | `relationships[]` mit `cardinality: one-to-many` |
| Select-Berechtigung | Rollensichtbarkeit + RLS-Filter |
| Spaltenberechtigung | `visible_to` / `writable_by` |
| Insert/Update/Delete-Berechtigung | Mutation `writable_by` + RLS |
| Remote Schema | Quellregistrierung `graphql_remote` |
| Computed Field | `functions[]`-Eintrag mit `kind: query` |

### Einschränkungen

- **Actions** konvertieren automatisch: HTTP-Handler-Actions werden zu `webhooks[]`-Mutationen; Actions mit einem Nicht-HTTP-Handler (Datenbank) werden zu einem `functions[]`-Platzhalter und erzeugen eine Warnung, den Handler zu prüfen
- **Event Triggers** konvertieren zu einer `event_triggers`-Konfiguration pro Tabelle (Operationen, Webhook-URL, Retry-Richtlinie) und erzeugen eine Warnung zur eingeschränkten Genauigkeit
- **Remote Schemas** konvertieren zu `graphql_remote`-Quelleinträgen
- **Benutzerdefinierte SQL-Funktionen** erfordern Prüfung — einfache Fälle konvertieren zu `functions[]`-Einträgen, komplexe erfordern manuelle Arbeit
- **Cron Triggers** konvertieren zu `scheduler`-Konfigurationseinträgen unter Beibehaltung des Cron-Ausdrucks und des Enabled-Flags

---

## Hasura DDN (v3)

### HML-Projekt lokalisieren

Der DDN-Konverter liest das DDN-Projekt-**Verzeichnis** mit `.hml`-Dateien direkt — kein Supergraph-Build-Schritt erforderlich. Die erste Verzeichniskomponente unter der Projektwurzel wird als Subgraph-Name übernommen; Dateien unter `globals/` werden dem Subgraph `globals` zugeordnet.

### Konvertieren

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

### Was konvertiert wird

| DDN-Konzept | Provisa-Entsprechung |
| ------------ | ------------------- |
| Subgraph-Modell | `tables[]` unter einer Quelle |
| Relationship | `relationships[]` |
| Berechtigungsregel | RLS-Filter |
| Command | Webhook-Mutation oder Sicht |
| Connector | Quelleintrag mit Verbindungsdetails |

### Einschränkungen

- **Lambda-Connectors** (TypeScript-/Python-Funktionen) erfordern manuelle Webhook-Einrichtung
- **Lifecycle-Plugins** haben keine direkte Entsprechung
- **DDN-Auth-Modi** werden auf Provisa-Auth-Provider abgebildet, aber JWT-Claim-Pfade müssen eventuell angepasst werden

---

## Nach dem Import

1. Prüfen Sie die generierte `config.yaml` — achten Sie auf `warnings` des Konverters
2. Verifizieren Sie die Verbindungsanmeldedaten (der Konverter verwendet Platzhalterwerte)
3. Starten Sie Provisa und bestätigen Sie, dass Tabellen im Explorer erscheinen
4. Führen Sie Ihre bestehenden GraphQL-Abfragen aus — das Schema ist für gängige Muster kompatibel
5. Reichen Sie Abfragen zur Genehmigung über die Admin-API oder -UI ein, bevor Sie die Produktions-Governance aktivieren
