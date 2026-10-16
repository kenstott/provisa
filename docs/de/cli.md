# CLI-Referenz {: #cli-reference }

Der Befehl `provisa` ist der einzige Einstiegspunkt für die per pip installierte Embedded-Stufe (REQ-1128).
Er startet die Laufzeitumgebung, verwaltet Lizenzen, löst die Veröffentlichung von Metadaten aus, deployt Modelle und
steuert das Wartungsbanner — ohne Docker, Node oder externe Dienste.

Installieren Sie ihn mit dem Extra `embedded`, das auch die Offline-DuckDB-Erweiterungen
und die eingebettete PostgreSQL-Control-Plane mitbringt:

```bash
pip install 'provisa[embedded]'
```

**Plattformanforderungen.** `provisa run` benötigt Python 3.12 und eine Plattform mit einem pgserver-
Wheel: Linux x86_64, macOS oder Windows x86_64. Für Linux aarch64 gibt es kein pgserver-Wheel und kein Quell-
Distributionspaket, sodass die Embedded-Stufe dort nicht läuft. Verwenden Sie auf aarch64 die Container-Stufe.
[tool-verified: `_require_supported_interpreter()` at cli.py:513-546]

---

## Gemeinsame Optionen {: #shared-options }

Mehrere Subcommands rufen die Provisa-HTTP-API auf. Sie teilen sich drei Flags und zwei Umgebungs-
variablen. [tool-verified: `_api_call()` at cli.py:345-376; each subcommand's argparse block
at cli.py:653-773]

| Flag | Standard | Fallback über Umgebungsvariable |
| --- | --- | --- |
| `--api <url>` | `http://127.0.0.1:8000` | `PROVISA_API_URL` |
| `--token <token>` | _(keiner)_ | `PROVISA_API_TOKEN` |
| `--timeout <seconds>` | 300 (30 für `maintenance`) | _(keine)_ |

`--api` ist die Basis-URL einer laufenden Provisa-Instanz. Bei Mandantenfähigkeit benennt der Hostname die
Organisation — `https://acme.provisa.org` routet zum Mandanten von Acme. `--token` ist ein Bearer-Token;
ist es leer, wird kein `Authorization`-Header gesendet, was für Deployments ohne Authentifizierung korrekt
ist. [tool-verified: cli.py:314-316, 357-365]

Setzen Sie beide Variablen in Ihrer CI-Umgebung, um sie nicht bei jedem Aufruf wiederholen zu müssen:

```bash
export PROVISA_API_URL=https://acme.provisa.org
export PROVISA_API_TOKEN=<token>
```

Subcommands, die diese Flags akzeptieren: `metadata export`, jeder `env`-Subcommand (`list`, `show`,
`create`, `delete`, `diff`, `merge`, `deploy`, `revert`, `redo`, `fetch`, `push`, `pull`),
`maintenance on`, `maintenance off`, `maintenance status`.

---

## provisa run

Startet das eingebettete Provisa-System — API-Server und UI-Static-/Proxy-Server — in einem einzigen Prozess.
Kein Docker, kein Node, keine externen Dienste. [tool-verified: cli.py module docstring lines 11-23;
`_cmd_run()` at cli.py:549-602]

```
provisa run [--demo] [--host HOST] [--api-port PORT] [--ui-port PORT]
            [--no-browser] [--reset] [--data-dir DIR]
```

### Flags

| Flag | Standard | Hinweise |
| --- | --- | --- |
| `--demo` | aus | Lädt die mitgelieferte Demo — Pet-Store- und Shelter-Beispieldomänen über eingebettetes SQLite (REQ-414) |
| `--host` | `127.0.0.1` | Bind-Adresse für beide Server |
| `--api-port` | `8000` | Port des API-Servers |
| `--ui-port` | `3000` | Port des UI-Static-/Proxy-Servers |
| `--no-browser` | aus | Öffnet keinen Browser, wenn die UI bereit ist; gibt die URL dennoch aus |
| `--reset` | aus | Verwirft und baut den eingebetteten Control-Plane-Store vor dem Start neu auf; nach einem Provisa-Upgrade verwenden, wenn der Start eine Schema-Abweichung meldet |
| `--data-dir` | `~/.provisa/native` | Verzeichnis mit dem eingebetteten PostgreSQL-Cluster und dem DuckDB-Erweiterungs-Cache |

[tool-verified: run subparser at cli.py:609-634]

### Umgebungsvariablen {: #environment-variables }

`provisa run` liest vor dem Start der HTTP-Server mehrere zusätzliche Variablen.
Setzen Sie sie, um die Standardwerte zu überschreiben, die `load_profile("native", ...)` sonst anwenden würde.
[tool-verified: `_apply_embedded_env()` at cli.py:83-116]

| Variable | Wirkung |
| --- | --- |
| `TRINO_HOST` / `TRINO_PORT` | Ersetzt die eingebettete DuckDB-Engine durch einen vom Kunden bereitgestellten Trino-Koordinator (REQ-1129) |
| `PROVISA_ENGINE_URL` | Alternative Möglichkeit, auf eine externe Federation-Engine zu verweisen |
| `PROVISA_CONFIG` | Zu ladende Konfigurationsdatei; `--demo` setzt dies auf die mitgelieferte Demo-Konfiguration (REQ-1127) |
| `PROVISA_DEMO` | Wird von `--demo` auf `1` gesetzt; markiert die Sitzung als Demo-Lauf |
| `PROVISA_DEMO_DIR` | Pfad zum Beispieldaten-Verzeichnis der Demo; von `--demo` gesetzt |
| `PROVISA_DUCKDB_EXT_DIR` | Vorab bereitgestelltes DuckDB-Erweiterungsverzeichnis; wird automatisch aus dem Paket `provisa-duckdb-ext` gesetzt, sofern vorhanden; fehlt es, lädt DuckDB bei der ersten Verwendung aus dem Netzwerk |

[tool-verified: `_apply_demo_config()` at cli.py:67-80; `_apply_embedded_env()` at cli.py:83-116]

### Startsequenz {: #startup-sequence }

1. Plattformprüfung — bricht mit klarer Meldung bei nicht unterstütztem Python oder fehlendem pgserver ab.
2. `--reset` (falls angefordert) — verwirft den eingebetteten PostgreSQL-Cluster; er wird im nächsten Schritt neu aufgebaut.
3. Demo-Konfiguration (falls `--demo`) — setzt `PROVISA_CONFIG` und `PROVISA_DEMO_DIR`.
4. Eingebettete Umgebung — startet die PostgreSQL-Control-Plane, löst deren Socket-URL auf und
   staged Offline-DuckDB-Erweiterungen, sofern `provisa-duckdb-ext` installiert ist.
5. Schema-Drift-Prüfung — durchsucht die laufende Control Plane nach fehlenden Spalten. Werden welche gefunden,
   wird ein `--reset`-Hinweis ausgegeben und mit Code 1 beendet. V1 kennt keine Migrationen; eine in einem
   neueren Release hinzugefügte Spalte erfordert einen Reset. [tool-verified: `_control_plane_drift()` at cli.py:119-162]
6. Beide Server starten gleichzeitig. Der Ready-Announcer pollt `GET /ready` (nicht `/health` — der
   `/ready`-Endpunkt bestätigt, dass der Store angebunden und die Engine warmgelaufen ist) und öffnet den Browser,
   sobald 200 zurückkommt. [tool-verified: `_announce_ready()` at cli.py:182-224]

### Exit-Codes

| Code | Bedeutung |
| --- | --- |
| 0 | Sauberes Herunterfahren (Strg-C) |
| 1 | Startfehler (Plattformprüfung fehlgeschlagen, Demo-Konfiguration fehlt, Schema-Drift erkannt) |

### Beispiel {: #example }

```bash
# Start with the demo data
provisa run --demo

# Start on non-default ports, no browser
provisa run --api-port 8080 --ui-port 4000 --no-browser

# Upgrade: reset the control plane first, then start
provisa run --reset

# Point at an external Trino cluster instead of the embedded DuckDB engine
TRINO_HOST=trino.internal TRINO_PORT=8080 provisa run
```

---

## provisa license apply

Prüft und installiert eine Lizenzdatei offline (REQ-1139). Die Datei ist die von
provisa.dev ausgestellte `license.json`. [tool-verified: `_cmd_license_apply()` at cli.py:271-280]

```
provisa license apply <file>
```

| Argument | Hinweise |
| --- | --- |
| `file` | Pfad zur Lizenzdatei; `~`-Expansion wird angewendet |

Exit-Code 0 bedeutet, dass die Lizenz gültig ist und installiert wurde. Exit-Code 1 bedeutet, dass sie abgelehnt wurde; der
Grund wird nach stderr ausgegeben. [tool-verified: cli.py:276-280]

```bash
provisa license apply ~/Downloads/license.json
```

---

## provisa license status

Zeigt die Maschinen-ID, den Trial-Status, die vergangenen Tage und die Lizenzgültigkeit (REQ-1139).
[tool-verified: `_cmd_license_status()` at cli.py:283-299]

```
provisa license status
```

Keine Flags. Gibt vier Zeilen aus — Maschinen-ID, Datum der ersten Erfassung, vergangene Tage, Trial-Status und
Lizenzstatus — und beendet sich mit 0. [tool-verified: cli.py:293-299]

```
Machine ID:   a1b2c3d4e5f6...
First seen:   2026-07-01
Elapsed:      83.0 days
Trial:        active
Licensed:     no (no license installed)
```

---

## provisa metadata export

Löst die On-Demand-Metadatenveröffentlichung des laufenden Servers aus (REQ-1072/REQ-1074). Sendet an
`POST /admin/metadata-export/publish` — denselben Endpunkt, den die Schaltfläche **Publish now** im Admin-Tab
aufruft, sodass beide Wege denselben vollständigen Snapshot senden. [tool-verified: `_cmd_metadata_export()`
at cli.py:302-342]

```
provisa metadata export [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Flag | Standard | Hinweise |
| --- | --- | --- |
| `--api` | `$PROVISA_API_URL`, dann `http://127.0.0.1:8000` | Bei Mandantenfähigkeit benennt der Hostname die Organisation |
| `--token` | `$PROVISA_API_TOKEN` | Bearer-Token für eine Identität mit `org_settings`; bei Deployments ohne Authentifizierung weglassen |
| `--timeout` | `300` | Sekunden, bevor der HTTP-Aufruf abgebrochen wird |

[tool-verified: cli.py:654-669]

| Exit-Code | Bedeutung |
| --- | --- |
| 0 | Jedes Asset veröffentlicht |
| 1 | Teilweise Veröffentlichung oder Verbindungsfehler; Fehler pro Asset werden nach stderr ausgegeben |

[tool-verified: cli.py:335-342]

```bash
provisa metadata export \
  --api  https://acme.provisa.org \
  --token "$PROVISA_API_TOKEN"

# Cron example — daily at 06:00
# 0 6 * * *  provisa metadata export --api https://acme.provisa.org >> /var/log/provisa-export.log 2>&1
```

Die vollständige Konfigurationsreferenz — Provider, Zugangsdaten, `reconcile_cron` und was der
Snapshot enthält — finden Sie unter [Metadata Export](metadata-export.md#from-the-command-line).

---

## provisa env

Führt den Lebenszyklus von Umgebungen gegen ein laufendes Provisa aus (REQ-1498). Jeder Subcommand ist ein Aufruf der
Admin-API, die auch die UI aufruft; die CLI entscheidet nichts. Die Positivliste dessen, was eine Kopie mitnimmt, die
Merge- und Entfernungsregeln, Bindung, Schutz und die Rechte des Aufrufers liegen beim Server, sodass sie gelten,
wie auch immer die Operation aufgerufen wird. [tool-verified: cli_envs.py:11-28]

```
provisa env {list,show,create,delete,diff,merge,deploy,revert,redo,fetch,push,pull} --org ORG ...
```
### Flags, die jeder Subcommand nimmt {: #flags-every-subcommand-takes }

| Flag | Erforderlich | Hinweise |
| --- | --- | --- |
| `--org` | ja | Organisation, die die Umgebung hält |
| `--json` | nein | Gibt die Antwort der API als JSON aus, für ein Skript |
| `--api` | nein | Siehe [Gemeinsame Optionen](#shared-options) |
| `--token` | nein | Bearer-Token für eine Identität, die die Operation ausführen darf |
| `--timeout` | nein | Sekunden; Standard 300 |

Subcommands, die auf eine Umgebung wirken, nehmen außerdem `--env ENV` (erforderlich). `list`, `diff`,
`merge` und `fetch` nicht; `diff` und `merge` benennen ihre Umgebungen mit `--from` und `--into`.
[tool-verified: cli_envs.py:349-374, `_command()`]

### Exit-Codes {: #exit-codes_1 }

| Code | Bedeutung |
| --- | --- |
| 0 | Erledigt, oder ein Probelauf, der nichts angewendet hat |
| 1 | Abgelehnt. Der Code der ablehnenden Regel erscheint auf stderr als `refused (<code>): <detail>`; mit `--json` ist es `{"refused": {...}}` auf stdout |
| 2 | Vorgeschlagen, nicht angewendet: Die Zielumgebung ist geschützt, und die Änderung wartet auf eine Genehmigung |

Exit-Code 2 ist beabsichtigt. Eine Pipeline, die eine ausstehende Genehmigung als freigegebenes Deploy behandelte,
läge falsch. [tool-verified: cli_envs.py:21-28, 40-41, 64-78, 120-135]

### provisa env list

Listet die Umgebungen der Org und den Build, auf dem jede steht. Keine Flags über die gemeinsamen hinaus.
[tool-verified: cli_envs.py:141-149]

```
provisa env list --org ORG [--json]
```
### provisa env show

Zeigt eine Umgebung und ihren aktuellen Build. Endet mit 1 und `environments.unknown`, wenn die
Umgebung nicht existiert. [tool-verified: cli_envs.py:163-183]

```
provisa env show --org ORG --env ENV [--json]
```
### provisa env create

Legt eine Umgebung an, die das Modell einer anderen hält, mit bei der Anlage gewählten Daten (REQ-1942). Die API
entscheidet, welche Wahl der Aufrufer treffen darf. [tool-verified: cli_envs.py:197-216]

```
provisa env create --org ORG --env NAME --data-mode MODE
                   [--from SOURCE] [--mutation-handling HANDLING] [--expires-at ISO8601]
```
| Flag | Erforderlich | Hinweise |
| --- | --- | --- |
| `--env` | ja | Name der neuen Umgebung |
| `--from` | nein | Umgebung, von der aus begonnen wird; Standard `prod` |
| `--data-mode` | ja | `inherit`, `unbound`, `test_fake` oder `test_synthetic`: die Verbindungen der Quelle per Referenz, keine, bis sie ihre eigenen bindet, oder Testdaten |
| `--mutation-handling` | nein | `refused`, `reversible` oder `direct`; Standard `refused` |
| `--expires-at` | nein | ISO-8601-Zeitpunkt, nach dem die Umgebung gelöscht wird |

[tool-verified: cli_envs.py:391-416]

```bash
provisa env create --org acme --env feature-x --from prod --data-mode test_fake
```
### provisa env delete

Setzt eine Umgebung außer Betrieb. Ihre Branches verschwinden nur, wenn sie benannt werden. [tool-verified: cli_envs.py:219-232]

```
provisa env delete --org ORG --env ENV [--delete-branch] [--delete-remote-branch]
```
| Flag | Hinweise |
| --- | --- |
| `--delete-branch` | Löscht auch ihren Branch |
| `--delete-remote-branch` | Löscht auch ihren Branch auf dem Remote |

[tool-verified: cli_envs.py:418-424]

### provisa env diff

Berichtet, was ein Merge einer Umgebung in eine andere tun würde. Es ist ein GET von `merge-preview` und kann
daher nicht schreiben; geben Sie es einem Runner, der nichts ändern darf. [tool-verified:
cli_envs.py:186-191]

```
provisa env diff --org ORG --from SOURCE --into TARGET [--removals] [--select PATH ...]
```
| Flag | Erforderlich | Hinweise |
| --- | --- | --- |
| `--from` | ja | Umgebung, aus der gemergt wird |
| `--into` | ja | Umgebung, in die gemergt wird |
| `--removals` | nein | Entfernt aus dem Ziel auch, was die Quelle nicht mehr hat |
| `--select PATH` | nein | Beschränkt den Merge auf dieses Objekt, über seinen Pfad im Modell (`metrics/revenue.yaml`); für weitere wiederholen. Was es voraussetzt und dem Ziel fehlt, wird mitgenommen und aufgeführt (REQ-1506) |

[tool-verified: cli_envs.py:426-439]

### provisa env merge

Mergt eine Umgebung in eine andere, nach Identität (REQ-1490). [tool-verified: cli_envs.py:235-250]

```
provisa env merge --org ORG --from SOURCE --into TARGET
                  [--removals] [--select PATH ...] [--dry-run] [--message MSG]
                  [--retire-source] [--retire-remote]
```
| Flag | Erforderlich | Hinweise |
| --- | --- | --- |
| `--from` | ja | Umgebung, aus der gemergt wird |
| `--into` | ja | Umgebung, in die gemergt wird |
| `--removals` | nein | Entfernt aus dem Ziel auch, was die Quelle nicht mehr hat |
| `--select PATH` | nein | Beschränkt den Merge auf dieses Objekt, über seinen Pfad im Modell (`metrics/revenue.yaml`); für weitere wiederholen. Was es voraussetzt und dem Ziel fehlt, wird mitgenommen und aufgeführt (REQ-1506) |
| `--dry-run` | nein | Gibt den Merge-Bericht aus und wendet nichts davon an |
| `--message` | nein | Was der Merge tut; zum Anwenden erforderlich |
| `--retire-source` | nein | Löscht die Quelle, sobald ihre Arbeit gelandet ist |
| `--retire-remote` | nein | Löscht mit `--retire-source` auch den Branch der Quelle auf dem Remote |

[tool-verified: cli_envs.py:429-451]

### provisa env deploy

Deployt das Modell an einer Git-Ref in eine Umgebung und macht diesen Baum zum aktuellen Modell der Umgebung
(REQ-1496). Das ist der Befehl, den eine Deployment-Pipeline ausführt; ein Deploy ist immer ein
Aufruf mit einer Identität gegen eine benannte Control Plane. [tool-verified:
cli_envs.py:253-267]

```
provisa env deploy --org ORG --env ENV --ref REF [--dry-run] [--message MSG]
```
| Flag | Erforderlich | Hinweise |
| --- | --- | --- |
| `--env` | ja | Umgebung, die das deployte Modell halten wird |
| `--ref` | ja | Branch oder Commit im Repository der Org |
| `--dry-run` | nein | Berichtet, was das Deploy tun würde; wendet nichts an |
| `--message` | nein | Notiz, die auf eine Genehmigungsanfrage übertragen wird, wenn das Ziel geschützt ist |

[tool-verified: cli_envs.py:453-468]

Ein Laden in eine bestehende Umgebung wendet nie Rollen an. `--seed` gibt es nicht mehr: Die API lehnt ein Seed
mit `environments.seed_only_at_creation` ab.

```bash
# Fetch remote branches, then deploy
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"

provisa env deploy \
  --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```
### provisa env revert

Setzt eine Umgebung auf einen früheren Build zurück (REQ-1543). Ohne `--to` geht es einen Commit
in der eigenen Historie der Umgebung zurück. Mit `--to` ist es das gewöhnliche Deploy dieses Builds.
[tool-verified: cli_envs.py:270-284]

```
provisa env revert --org ORG --env ENV [--to BUILD] [--message MSG]
```
| Flag | Erforderlich | Hinweise |
| --- | --- | --- |
| `--env` | ja | Zurückzusetzende Umgebung |
| `--to` | nein | Zu deployender Build; Standard ist der vor dem aktuellen Build |
| `--message` | nein | Notiz, die auf eine Genehmigungsanfrage übertragen wird |

[tool-verified: cli_envs.py:470-480]

### provisa env redo

Geht wieder vorwärts in Richtung des Builds, von dem ein Revert ausgegangen ist. [tool-verified:
cli_envs.py:287-301]

```
provisa env redo --org ORG --env ENV
```
### provisa env fetch

Ruft die Remote-Branches der Org in ihr Provisa-Repository ab (REQ-1541). Führen Sie dies vor einem Deploy aus,
wenn Sie `origin/<branch>` benennen wollen. Provisa führt es nie zeitgesteuert aus. [tool-verified:
cli_envs.py:307-320]

```
provisa env fetch --org ORG
```
Gibt eine Zeile pro abgerufenem Branch aus, `origin/<name>  <sha12>`.

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# origin/main  a1b2c3d4e5f6
# origin/dev   9f8e7d6c5b4a
```
### provisa env push

Sendet den Branch einer Umgebung an das Remote der Org (REQ-1546). Gibt `nothing to send` aus, wenn
nichts zu senden ist. [tool-verified: cli_envs.py:323-331]

```
provisa env push --org ORG --env ENV
```
### provisa env pull

Wendet an, was das Remote für eine Umgebung hält (REQ-1547). Gibt
`<env> already holds what the remote does` aus, wenn nichts anzuwenden ist. Exit-Code 2 gilt wie
beim Deploy. [tool-verified: cli_envs.py:334-343]

```
provisa env pull --org ORG --env ENV
```
Die vollständige Erklärung von Umgebungsklassen, Schutzregeln, Merge-Berichten und dem
Genehmigungslebenszyklus finden Sie unter [Environments](environments.md#the-env-cli-commands).

---

## provisa maintenance on

Setzt das geplante Wartungsbanner auf dem Deployment (REQ-1466). Führen Sie dies vor geplanten
Arbeiten aus, die die Data Plane herunterfahren — zum Beispiel vor einer Änderung von
`var.engine_cluster_mode`, die den Engine-Cluster und jeden darauf liegenden Shard ersetzt (REQ-1465).
[tool-verified: `_cmd_maintenance_on()` at cli.py:483-495]

```
provisa maintenance on [--message MSG] [--ends-at ISO8601]
                       [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Flag | Hinweise |
| --- | --- |
| `--message` | Überschreibt den Standardtext des Deployments; Standard ist die Standardmeldung des Servers |
| `--ends-at` | ISO-8601-Zeitpunkt, zu dem die Arbeiten voraussichtlich enden, z. B. `2026-08-14T22:30:00Z`; Standard ist keine Schätzung |
| `--api` | Siehe [Gemeinsame Optionen](#shared-options) |
| `--token` | Bearer-Token für eine Identität mit `platform_settings` |
| `--timeout` | Standard 30 s |

[tool-verified: cli.py:743-773]

Gibt den resultierenden Bannerstatus aus und beendet sich mit 0. Löst bei HTTP- oder Verbindungsfehler ein `SystemExit` aus.

```bash
provisa maintenance on \
  --message "Engine cluster rolling upgrade" \
  --ends-at "2026-09-22T03:00:00Z" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## provisa maintenance off

Löscht das Wartungsbanner, sobald die Arbeiten abgeschlossen sind (REQ-1466).
[tool-verified: `_cmd_maintenance_off()` at cli.py:498-501]

```
provisa maintenance off [--api URL] [--token TOKEN] [--timeout SECONDS]
```

Gibt den resultierenden Bannerstatus aus (active: false) und beendet sich mit 0.

```bash
provisa maintenance off --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# Maintenance notice: OFF
```

---

## provisa maintenance status

Zeigt den aktuellen Wartungsbannerstatus, ohne ihn zu ändern (REQ-1466).
[tool-verified: `_cmd_maintenance_status()` at cli.py:504-507]

```
provisa maintenance status [--api URL] [--token TOKEN] [--timeout SECONDS]
```

Gibt den Bannerstatus aus und beendet sich mit 0.

```
Maintenance notice: ON
  Message:  Engine cluster rolling upgrade
  Since:    2026-09-22T01:15:00Z
  Ends at:  2026-09-22T03:00:00Z
```

---

## Kurzreferenz {: #quick-reference }

| Befehl | REQ | Was er tut |
| --- | --- | --- |
| `provisa run` | REQ-1128 | Startet die eingebettete API + UI |
| `provisa run --demo` | REQ-414 | Startet mit Pet-Store-/Shelter-Beispieldaten |
| `provisa run --reset` | REQ-1535 | Baut die Control Plane vor dem Start neu auf |
| `provisa license apply <file>` | REQ-1139 | Installiert eine Lizenzdatei offline |
| `provisa license status` | REQ-1139 | Zeigt Maschinen-ID und Trial-/Lizenzstatus |
| `provisa metadata export` | REQ-1072 | Veröffentlicht den Metadaten-Snapshot auf Anforderung |
| `provisa env list --org ORG` | REQ-1498 | Listet die Umgebungen der Org und ihre Builds |
| `provisa env show --org ORG --env ENV` | REQ-1498 | Zeigt eine Umgebung |
| `provisa env create --org ORG --env ENV --data-mode MODE` | REQ-1942 | Legt eine Umgebung aus einer anderen an |
| `provisa env delete --org ORG --env ENV` | REQ-1498 | Setzt eine Umgebung außer Betrieb |
| `provisa env diff --org ORG --from A --into B` | REQ-1490 | Vorschau eines Merges (nur lesend) |
| `provisa env merge --org ORG --from A --into B` | REQ-1490 | Mergt eine Umgebung in eine andere |
| `provisa env deploy --org ORG --env ENV --ref REF` | REQ-1496 | Deployt eine Ref in eine Umgebung |
| `provisa env revert --org ORG --env ENV [--to BUILD]` | REQ-1543 | Geht einen Commit zurück oder deployt einen Build |
| `provisa env redo --org ORG --env ENV` | REQ-1543 | Geht wieder vorwärts |
| `provisa env fetch --org ORG` | REQ-1541 | Ruft Remote-Branches in das Provisa-Repository ab |
| `provisa env push --org ORG --env ENV` | REQ-1546 | Sendet den Branch einer Umgebung an das Remote |
| `provisa env pull --org ORG --env ENV` | REQ-1547 | Wendet an, was das Remote hält |
| `provisa maintenance on` | REQ-1466 | Setzt das Wartungsbanner |
| `provisa maintenance off` | REQ-1466 | Löscht das Wartungsbanner |
| `provisa maintenance status` | REQ-1466 | Zeigt den aktuellen Bannerstatus |

## Siehe auch {: #see-also }

- [Environments](environments.md) — Umgebungsmodell, geschützte Umgebungen, Deploy-Genehmigungslebenszyklus
- [Metadata Export](metadata-export.md) — Katalog-Provider, Konfiguration und Inhalt des Snapshots
- [Deployment](deployment.md) — Container-Stufe und Cloud-Deployment
