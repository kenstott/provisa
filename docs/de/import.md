# Import aus Hasura

Provisa kann vorhandene Hasura-Metadaten in eine Provisa-`config.yaml` umwandeln und dabei verfolgte Tabellen, Beziehungen, Berechtigungen und Remote-Schemas erhalten.

## Hasura v2

### Metadaten exportieren

Aus Ihrer Hasura-Konsole oder -CLI:

```bash
hasura metadata export --output metadata.yaml
```

Oder verwenden Sie die Hasura-API:

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

### Konvertieren

Der v2-Konverter liest ein Hasura-Metadaten-**Verzeichnis** (das von `hasura metadata export` erzeugte Layout, oder das flache `tables.yaml`-/`actions.yaml`-Layout) und schreibt eine Provisa-Konfiguration:

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Lassen Sie `-o` weg, um die Konfiguration nach stdout zu schreiben.

Flags:

| Flag | Zweck |
| ------ | --------- |
| `-o`, `--output` | Ausgabe-YAML-Pfad (Standard: stdout) |
| `--source-overrides` | YAML-Datei mit Pro-Quelle-Verbindungsüberschreibungen (Host, Port, Anmeldedaten) |
| `--domain-map` | Schema-zu-Domäne-Zuordnungen als `SCHEMA=DOMAIN`-Paare |
| `--auth-env-file` | `.env`-Datei mit Auth-Konfiguration; konvertiert JWT/JWK, Admin-Secret und Claims-Zuordnung |
| `--dry-run` | Parsen und Validieren, ohne die Ausgabe zu schreiben |

### Was konvertiert wird

| Hasura-Konzept | Provisa-Äquivalent |
| --------------- | ------------------- |
| Verfolgte Tabelle | `tables[]` mit `publish: true` |
| Objektbeziehung | `relationships[]` mit `cardinality: many-to-one` |
| Array-Beziehung | `relationships[]` mit `cardinality: one-to-many` |
| Select-Berechtigung | Rollensichtbarkeit + RLS-Filter |
| Spaltenberechtigung | `visible_to` / `writable_by` |
| Insert-/Update-/Delete-Berechtigung | Mutations-`writable_by` + RLS |
| Remote-Schema | `graphql_remote`-Quellenregistrierung |
| Berechnetes Feld | `functions[]`-Eintrag mit `kind: query` |

### Einschränkungen

- **Actions** werden automatisch konvertiert: HTTP-Handler-Actions werden zu `webhooks[]`-Mutationen; Actions mit einem Nicht-HTTP-Handler (Datenbank) werden zu einem `functions[]`-Platzhalter und geben eine Warnung aus, den Handler zu überprüfen
- **Event Triggers** werden zu Pro-Tabelle-`event_triggers`-Konfiguration konvertiert (Operationen, Webhook-URL, Wiederholungsrichtlinie) und geben eine Warnung bezüglich eingeschränkter Genauigkeit aus
- **Remote Schemas** werden zu `graphql_remote`-Quelleneinträgen konvertiert
- **Benutzerdefinierte SQL-Funktionen** erfordern eine Überprüfung — einfache Fälle konvertieren zu `functions[]`-Einträgen, komplexe benötigen manuelle Arbeit
- **Cron Triggers** werden zu `scheduler`-Konfigurationseinträgen konvertiert und erhalten dabei den Cron-Ausdruck und das Enabled-Flag

---

## Hasura DDN (v3)

### Das HML-Projekt finden

Der DDN-Konverter liest das DDN-Projekt-**Verzeichnis** von `.hml`-Dateien direkt — kein Supergraph-Build-Schritt ist erforderlich. Die erste Verzeichniskomponente unter dem Projekt-Root wird als Subgraph-Name übernommen; Dateien unter `globals/` werden dem Subgraph `globals` zugewiesen.

### Konvertieren

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

Lassen Sie `-o` weg, um die Konfiguration nach stdout zu schreiben.

Flags:

| Flag | Zweck |
| ------ | --------- |
| `-o`, `--output` | Ausgabe-YAML-Pfad (Standard: stdout) |
| `--source-overrides` | YAML-Datei mit Pro-Quelle-Verbindungsüberschreibungen |
| `--domain-map` | Subgraph-zu-Domäne-Zuordnungen als `SUBGRAPH=DOMAIN`-Paare |
| `--aggregates-output` | Ausgabepfad für die Aggregate-Expressions-Sidecar-Datei (Standard: `<output>-aggregates.yaml`) |
| `--dry-run` | Parsen und Validieren, ohne die Ausgabe zu schreiben |

`AggregateExpression`-Metadaten werden in einer Sidecar-Datei `*-aggregates.yaml` erhalten.

### Was konvertiert wird

| DDN-Konzept | Provisa-Äquivalent |
| ------------ | ------------------- |
| Subgraph-Modell | `tables[]` unter einer Quelle |
| Beziehung | `relationships[]` |
| Berechtigungsregel | RLS-Filter |
| Command | Webhook-Mutation oder Sicht |
| Connector | Quelleneintrag mit Verbindungsdetails |

### Einschränkungen

- **Lambda-Connectoren** (TypeScript-/Python-Funktionen) erfordern manuelle Webhook-Einrichtung
- **Lifecycle-Plugins** haben kein direktes Äquivalent
- **DDN-Auth-Modi** werden auf Provisa-Auth-Provider abgebildet, aber JWT-Claim-Pfade müssen möglicherweise angepasst werden

---

## Nach dem Import

1. Überprüfen Sie die generierte `config.yaml` — achten Sie auf `warnings` vom Konverter
2. Verifizieren Sie die Verbindungsanmeldedaten (der Konverter verwendet Platzhalterwerte)
3. Starten Sie Provisa und bestätigen Sie, dass Tabellen im Explorer erscheinen
4. Führen Sie Ihre bestehenden GraphQL-Abfragen aus — das Schema ist für gängige Muster kompatibel
5. Reichen Sie Abfragen zur Genehmigung über die Admin-API oder UI ein, bevor Sie die Produktions-Governance aktivieren
