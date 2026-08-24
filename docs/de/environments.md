# Umgebungen

Eine Umgebung ist eine benannte Kopie des regierten Modells einer Organisation. Die Kopie ist physisch ein
eigenes PostgreSQL-Schema — keine Diskriminatorspalte, kein Präfix, ein echtes Schema —, sodass jede
bestehende Repository-Abfrage innerhalb einer Umgebung korrekt ist, ohne dass etwas umgeschrieben wird, und
die Zeilen der einen Umgebung nicht durch ein vergessenes Prädikat in den Lesezugriff einer anderen geraten
können (REQ-1487, REQ-1488).
[tool-verified: `environments.py` module docstring; `org_schema()` at environments.py lines 86-96]

Jede Organisation startet mit einer Umgebung namens `prod`. Sie kann weder gelöscht noch umbenannt werden.
Eine Anfrage, die keine Umgebung benennt, wird von `prod` bedient; eine Anfrage, die eine nicht existierende
Umgebung benennt, wird abgelehnt. [tool-verified: `PROD = "prod"` at environments.py line 44;
`select_environment()` at env_routing.py lines 93-129]

Umgebungen stehen Organisationen mit einem kostenpflichtigen Tarif zur Verfügung. [inferred: REQ-1507]

## Umgebungsnamen

Ein Name muss `[a-z][a-z0-9_]{1,31}` entsprechen — zwei bis zweiunddreißig Zeichen aus Kleinbuchstaben,
Ziffern und Unterstrichen, beginnend mit einem Buchstaben. `prod` und Namen, die mit `pg_` beginnen, werden
abgelehnt. Die Maximallänge für eine bestimmte Org hängt von der ID dieser Org ab: PostgreSQL kürzt einen
Bezeichner über 63 Byte stillschweigend, und der längste Schemaname, den eine Umgebung ableitet, ist das,
wovor die Obergrenze schützt. [tool-verified: `ENV_NAME_PATTERN` at environments.py line 59;
`validate_env_name()` at environments.py lines 119-142; `max_env_name_length()` at
environments.py lines 108-116]

## Was eine Kopie mitnimmt

Jede Tabelle im Org-Schema fällt in genau eine Klasse (REQ-1489). Die Klassifizierung ist eine Allow-List,
keine Ausschlussliste: Eine später hinzugefügte Tabelle reist nicht mit, bis jemand hier ihre Klasse benennt
— die Fehlerwirkung für eine vergessene Tabelle ist also ein roter Test. [tool-verified: `CLASSIFIED`
constant and module docstring, env_classes.py lines 19-22]

| Klasse | Tabellen | Was beim Kopieren geschieht |
| --- | --- | --- |
| CARRIED | domains, naming_rules, registered_tables, table_columns, relationships, metrics, roles, rls_rules, tags, tag_param_values, tag_assignments, glossary terms, materialized_views, calendars, api_endpoints, tracked_functions, tracked_webhooks, table_meta_links | Vollständig kopiert |
| IDENTITY_ONLY | sources, api_sources, kafka_sources, kafka_sinks | Identitäts- und Governance-Felder reisen mit; Verbindungswerte bleiben zurück (siehe Bindungen) |
| SEEDED_AT_CREATION | roles, user_role_assignments | Nur beim erstmaligen Anlegen einer Umgebung kopiert; spätere Merges lassen sie unberührt |
| PARTIAL | org_settings | Pro Schlüssel kopiert: Governance-Einstellungen reisen mit, Schlüssel, die ein externes Ziel oder eine umgebungsspezifische Laufzeit benennen, bleiben zurück |
| NEVER_SENSITIVE | org_secrets, user_directory | Nie kopiert |
| NEVER_RUNTIME | mv_refresh_log, relationship_candidates, admin_audit_log und weitere | Nie kopiert |

[tool-verified: `CARRIED`, `IDENTITY_ONLY`, `SEEDED_AT_CREATION`, `PARTIAL`, `NEVER_SENSITIVE`,
`NEVER_RUNTIME` frozensets, env_classes.py lines 29-113]

`SEEDED_AT_CREATION` existiert, um ein einziges bestimmtes Problem zu lösen. Eine neue Umgebung braucht
Rollen und Zuweisungen, sonst öffnet sie, ohne dass irgendjemand handeln kann. Aber ein späterer Merge, der
die `developer`-Zeile aus `prod` mitbrächte, würde die eingeschränkte Fassung überschreiben, die ein
eingeschränkter Branch womöglich braucht — und damit den Review-Pfad zur Eskalationsroute machen. Rollen und
Zuweisungen reisen deshalb einmal mit, beim Anlegen, und sind danach die eigene Antwort jeder Umgebung.
[tool-verified: env_classes.py lines 65-71; env_copy.py lines 41-44]

## Bindungen

Bindungen sind die Spalten, die sagen, wohin eine Quelle tatsächlich zeigt — `host`, `port`, `database`,
`username` und der Rest. Sie reisen in keiner Kopie mit. Eine Umgebung, die noch nicht gebunden wurde, wird
als `unbound` markiert, statt leer gelassen zu werden: Ein leerer Host ist kein fehlender, und der
Verbindungsaufbau würde ihn als `localhost:5432` lesen. [tool-verified: `BOUND_COLUMN = "bound"` at
env_classes.py line 143; `BINDING_COLUMNS` dict at env_classes.py lines 155-172]

Die Quellen einer Umgebung lösen auf eine von zwei Arten auf.

**Basis** — die Umgebung trägt ihre eigenen Anmeldedaten. Ein org_admin legt eine Basis an und bindet dann
jede Quelle ausdrücklich. [tool-verified: `CreateEnvBody.inherit_connections = False` (default) at
environments_router.py line 227; "binding a base is an org_admin's act" comment at line 358]

**Branch** — die Umgebung erbt die Anmeldedaten der Basis per Referenz. Nichts wird kopiert. Braucht eine
Abfrage eine Verbindung, läuft die Auflösung die `branched_from`-Kette hinauf und hält bei der ersten
Umgebung an, deren Zeile gebunden ist. Das Rotieren von Anmeldedaten auf der Basis pflanzt sich ohne
weiteres Zutun auf jeden ihrer Branches fort. Ein Widerruf widerruft für alle auf einmal. Kein Secret wird
je irgendwo materialisiert, von wo ein Branch, ein Export oder ein Repository es forttragen könnte.
[tool-verified: `resolve()` at env_bindings.py lines 114-151; `lineage()` at env_bindings.py
lines 74-102; env_bindings.py module docstring lines 11-33]

Um einen Branch anzulegen, setzen Sie **Verbindungen erben** im Umgebungs-Panel. Die Voreinstellung ist aus.
[tool-verified: `environmentsTab.json` key `inheritConnections`; `inheritHelp2` string]

## Die Git-Projektion

Jeder Schreibvorgang am Modell committet das Ergebnis in den Git-Branch der Umgebung. Das Repository ist eine
Projektion des Modells, nie seine Autorität: Provisa liest und schreibt die Control Plane; das Repository ist
die Aufzeichnung, nicht die Quelle. Einen Baum zu deployen erfordert einen ausdrücklichen Aufruf — ein
gemergter Pull Request auf dem Git-Host deployt sich nicht von selbst (REQ-1524, REQ-1526). [tool-verified:
deploy endpoint docstring at environments_router.py lines 777-791]

Jede Entität bekommt eine Datei. Der Pfad ist die REQ-1385-URI ohne Schema und Org:
`provisa://acme/sales/tables/Order` wird zu `sales/tables/Order.yaml`. Quellen landen in `sources/`,
Commands in `commands/`, Metriken in `metrics/`. Kindzeilen, die von einem Elternteil kaskadieren — Spalten,
Beziehungen, RLS-Regeln —, werden in die Datei des Elternteils geschrieben, nicht als eigene Dateien.
[tool-verified: `table_path()` at env_files.py line 109-115; `kind_path()` at env_files.py
lines 118-120; `COMMANDS_DIR = "commands"` at env_project.py line 71; env_files.py module
docstring lines 17-24]

Commands und ihre Tag-Zuweisungen überstehen den Hin- und Rückweg. Ein Tag auf einem Command wird in die
eigene Datei des Commands geleitet (`commands/<name>.yaml`); ein Tag, das zu keiner Datei gehört,
verschwindet aus der Projektion und würde beim nächsten Deploy dieses Baums gelöscht. [tool-verified:
env_project.py lines 346-364; `owner_command_name` routing in `_assignments_for()` at
env_project.py lines 137-164]

Kein Surrogatschlüssel gelangt in eine Datei. `registered_tables.id` ist eine automatisch hochzählende
Ganzzahl — dasselbe Modell bekommt in zwei Umgebungen unterschiedliche Zahlen, sodass ein naiver Dump gegen
sich selbst diffed. Jedes Surrogat wird verworfen und jede Referenz darauf als Pfad des Ziels geschrieben.
[tool-verified: `STORAGE_COLUMNS` and `_model_columns()` at env_files.py lines 62-128;
env_project.py docstring lines 26-27]

Die Serialisierung ist deterministisch. Schlüssel werden alphabetisch ausgegeben, Kindsammlungen nach ihrer
Adresse sortiert, und der YAML-Stil liegt fest. Zwei Umgebungen, die dasselbe Modell halten, erzeugen
byte-identische Bäume. [tool-verified: `dump()` at env_files.py lines 131-143]

## Merge

Das Mergen des Modells einer Umgebung in eine andere aktualisiert über die Identität: Jedes Objekt, das die
Quelle hat, wird im Ziel angelegt oder aktualisiert. Objekte, die die Quelle nicht mehr hat, werden nur
entfernt, wenn die aufrufende Seite Entfernungen ausdrücklich anfordert. Ein Merge, der auf halbem Weg
scheitert, lässt das Ziel so, wie es war — eine Transaktion. [tool-verified: `copy_model()` at
env_copy.py lines 216-234; REQ-1490 description]

Rufen Sie vor dem Anwenden den Vorschau-Endpunkt auf (`GET /{name}/merge-preview`) oder übergeben Sie
`dry_run: true`. Die Vorschau läuft denselben Codepfad, den der Merge verwendet; sie ist ein `GET`-Endpunkt,
damit ein CI-Skript, das das Flag falsch setzt, nicht versehentlich den Merge anwendet, den es prüfen wollte.
[tool-verified: `preview_merge()` docstring at environments_router.py lines 1086-1095]

Ein Merge lässt Bindungen, Rollen und Secrets des Ziels genau so, wie sie waren. Eine Dev-Umgebung verliert
ihre eigenen Datenbankverbindungen nicht dadurch, dass sie ein neueres Modell aus prod übernimmt. Prod erwirbt
nicht die Grants von dev. [tool-verified: env_copy.py lines 269-287; REQ-1490 scenario]

### Was der Bericht benennt

Der Merge-Bericht führt nach Pfad auf, was hinzugefügt, geändert, entfernt und unverändert gelassen wurde.
Er benennt außerdem alle **Konflikte** — Objekte, die beide Seiten seit dem letzten gemeinsamen Commit
geändert haben. Ein Konflikt wird gemeldet und nicht aufgelöst: Die Quelle gewinnt, denn genau das bedeutet
ein Merge in ein Ziel. Provisa bietet keine Konfliktauflösung, keine Merge-Marker, keine Auswahl pro Objekt.
Der Wert der Konfliktliste ist das Signal — zwei Personen haben dasselbe Objekt bearbeitet, ohne voneinander
zu wissen (REQ-1555). [tool-verified: `CopyReport.conflicts` at env_copy.py lines 151-165;
`detect_conflicts()` called at env_copy.py lines 261-263; REQ-1555 description]

Ein Objekt, das beide Seiten auf denselben Wert geändert haben, ist Übereinstimmung, kein Konflikt. Teilen
die beiden Umgebungen überhaupt keinen Vorfahren, ist die Basis im Bericht `None`, und die leere Konfliktliste
bedeutet, dass nichts verglichen wurde, nicht dass nichts kollidiert ist. [tool-verified:
`CopyReport.compared` property at env_copy.py lines 164-166; env_copy.py lines 255-264]

Der Merge landet als ein einzelner gequetschter Commit auf dem Branch des Ziels. Die Commit-Nachricht ist
erforderlich und darf nicht leer sein — sie ist die einzige Rechenschaft über den Arbeitsbereich, für den der
Squash steht. Die Commits der Quelle bleiben, wo sie sind, und bleiben danach per SHA deploybar.
[tool-verified: `_squash()` docstring at environments_router.py lines 663-680;
`MergeBody.message` comment at environments_router.py lines 258-260]

## Pull

Ein Pull nimmt, was die Gegenstelle für eine Umgebung hält, und macht es zum Modell. Er spult den lokalen
Branch nicht direkt vor; er wendet den geholten Baum über den gewöhnlichen Deploy-Pfad an, sodass dieselbe
Validierung und dieselbe Auditierung, die ein manuelles Deploy regieren, auch einen Pull regieren.
[tool-verified: `pull_environment()` docstring at environments_router.py lines 1450-1462]

Wie ein Merge meldet ein Pull, was er überschrieben hat — Objekte, die der eingehende Baum geändert hat und
die die lokale Umgebung seit dem letzten gemeinsamen Commit der beiden Linien ebenfalls geändert hatte. Eine
nicht committete lokale Änderung ist eine abgedriftete Umgebung (siehe Historie weiter unten); ein Pull
benennt sie im Bericht als gewöhnliche Änderung. [tool-verified: REQ-1556 description;
`pull_environment()` at environments_router.py lines 1485-1519]

Ein Pull wird abgelehnt, wenn die beiden Linien **auseinandergelaufen** sind — beide halten Commits, die die
andere nicht hat. Die Ablehnung trägt die Liste der Objekte, die beide Seiten angefasst haben, damit die
Person, die nun entscheiden muss, wessen Arbeit überlebt, weiß, welche Objekte sie ansehen muss.
[tool-verified: `state["diverged"]` check at environments_router.py lines 1491-1503;
`_collisions()` at environments_router.py lines 1581-1602]

## Historie

Jedes Deploy bewegt den Cursor der Umgebung in ihrer eigenen Commit-Linie vorwärts. Ein Undo tritt einen
Commit zurück; ein Redo tritt wieder vorwärts in Richtung der Position, die das Undo verlassen hat. Keine der
beiden Operationen entfernt einen Commit — ein Rückschritt fügt eine Position hinzu, er schreibt die Historie
nicht um. [tool-verified: `_move()` docstring at environments_router.py lines 854-868]

Ein Branch wird an der Spitze der Umgebung angesät, aus der er angelegt wurde, sodass ein Undo an diesem
Ansaatpunkt stoppt und nicht auf die Commits der Elternumgebung wandert. [tool-verified:
`origin_sha` comment at environments_router.py lines 428-448; `_move()` at
environments_router.py lines 907-916]

Die Flags `can_undo` und `can_redo` reisen mit der Antwort der Umgebungsliste mit. Beide melden `false`, wenn
die Projektion den Commit nicht hält, den die Control Plane benennt — ein Zustand, den der Entwurf zulässt und
der **abgedriftet** heißt. Ein Knoten, dessen Repository-Speicher einen bestimmten Commit nie erhalten hat,
listet seine Umgebungen weiterhin auf; nur die Historie-Antworten ändern sich (REQ-1561). [tool-verified:
`_with_history()` at environments_router.py lines 316-344; REQ-1561 description]

## Autorisierung

Umgebungen werden von zwei Rechten regiert. Keines gehört standardmäßig einer Analystin (REQ-1573).
[tool-verified: REQ-1573 description; `MANAGE_CAPABILITY = "environment_management"` and
`SWITCH_CAPABILITY = "environment_switch"` at environments_router.py line 110 and
env_routing.py line 53]

| Recht | Wer es hält (angesät) | Was es regiert |
| --- | --- | --- |
| `environment_management` | org_admin, developer | Umgebungen anlegen und löschen |
| `environment_switch` | org_admin, developer | Von einer anderen Umgebung als prod bedient werden |

`prod` braucht kein Recht — es ist das, wovon eine Anfrage bedient wird, die nichts benennt, und es zu
verweigern hieße, jede Anfrage zu verweigern.

Die Durchsetzung erfolgt am Auswahlpunkt, bevor irgendeine Route erreicht wird. Ein Mitglied ohne
`environment_switch` wird für alle Oberflächen zugleich abgelehnt — HTTP, GraphQL, SQL und die
Wire-Protokolle —, weil die Umgebung in der Middleware gebunden wird, nicht in einzelnen Handlern.
[tool-verified: `select_environment()` at env_routing.py lines 93-129; env_routing.py
module docstring lines 28-34]

Eine Analystin ohne Umgebungsrecht kann `prod` abfragen und sieht den Umgebungswechsler nicht. Eine externe
Kraft mit der Analystenrolle sieht keine Umgebungsoberfläche und kann keine andere Umgebung als die Produktion
anlegen oder in sie wechseln. [tool-verified: REQ-1573 use_case and scenario]

### Die Autorität der Umgebungseigentümerin

Eine Umgebung anzulegen ist der einzige Weg, auf dem ein nur lesendes Mitglied Rechte zur Modellbearbeitung
erwirbt (REQ-1528). Innerhalb der Umgebung, die sie angelegt hat, hält die erstellende Person die Capabilities
der Rolle `developer` — abzüglich der Datenrechte (`write`, `full_results`, `usage`). Rechte zum Modellbau,
keine Datenrechte. [tool-verified: `ENVIRONMENT_OWNER_CAPABILITIES` at env_authority.py lines 75-77;
`_DATA_RIGHTS` at env_authority.py lines 74-77; env_authority.py module docstring lines 14-38]

Die Vergabe wird zur Autorisierungszeit aus `environments.created_by` abgeleitet und nie in eine
Vergabetabelle geschrieben. Das Löschen der Umgebung entzieht sie im selben Akt.
[tool-verified: env_authority.py module docstring lines 39-42; `environment_owner()` at
env_authority.py lines 84-98]

Die Domänenmitgliedschaft begrenzt weiterhin, was die eigentümende Person ändern darf. Ein Branch ändert, was
ein Mitglied tun darf; er ändert nie, an welchen Domänen es das tun darf (REQ-1530).
[tool-verified: `domains_within()` at env_authority.py lines 121-145]

## Geschützte Umgebungen (REQ-1504)

Eine Umgebung kann geschützt sein. Ein Merge oder Deploy in eine geschützte Umgebung wird nicht auf Anfrage
angewendet; er wird vorgeschlagen, und jemand anderes als die anfragende Person muss ihn genehmigen.

`prod` ist automatisch geschützt, sobald die Org mehr als ein Mitglied hat. Eine Org mit einem einzigen
Mitglied kann „jemand anderes als die anfragende Person“ nicht erfüllen, also gilt die Regel dort nicht — sie
würde `prod` unmergebar machen. Jede Umgebung kann von einem org_admin als geschützt markiert werden.
[tool-verified: `is_protected()` at env_approvals.py lines 79-96; `protectedHelp2` UI string
in environmentsTab.json line 28]

Eine Merge-Anfrage ist eine Zeile, kein Bestätigungsdialog. Die genehmigende Person ist definitionsgemäß eine
andere als die anfragende und ist im Moment der Anfrage nicht anwesend; eine flüchtige Bestätigung würde die
Genehmigung in die Sitzung der anfragenden Person zwingen — genau die eine Anordnung, die die Anforderung
verbietet. [tool-verified: env_approvals.py module docstring lines 11-17]

Die Anfragezeile trägt den Merge-Bericht zusammen mit der Nachricht der anfragenden Person. Veraltung wird zur
Lesezeit abgeleitet, nie gespeichert: Zur Lesezeit neu zu planen und mit dem gespeicherten Bericht zu
vergleichen ist die einzige Variante, die nicht falsch sein kann. Eine veraltete Anfrage muss neu gestellt
werden. Die anfragende Person kann ihre eigene Anfrage nicht genehmigen. [tool-verified: `STALE` constant
and `effective_state()` at env_approvals.py lines 53, 215-243; `decide()` lines 265-268]

Zustände im Lebenszyklus einer Anfrage: `requested` → `approved`/`rejected` → `applied`. `stale` wird
abgeleitet. [tool-verified: `REQUESTED`, `APPROVED`, `REJECTED`, `APPLIED`, `STALE` at env_approvals.py
lines 47-53]

Dieselbe Tür bearbeitet Deploys von einer Repository-Ref: Die Anfrage nagelt den SHA zum Zeitpunkt des
Vorschlags fest. Bewegt sich die Ref zwischen Vorschlag und Entscheidung, liest die genehmigende Person den
Bericht für den festgenagelten Commit, nicht für den neuen. [tool-verified: `request_deploy()` at
env_approvals.py lines 150-189; env_approvals.py docstring lines 26-27]

!!! note
    Die Oberfläche für Merge-Anfragen liegt im Tab **Merge-Anfragen** des Umgebungs-Panels.
    Die Spalte **Bericht** zeigt anhand von Anzahlen, was sich ändern würde; die Zeile klappt auf und zeigt
    die Details pro Objekt. [tool-verified: `environmentsTab.json` keys `requestsTitle`, `colReport`,
    `approve`, `reject`]

## Die `env`-CLI-Befehle

`provisa env deploy` schickt das Modell an einer Ref in eine Umgebung. Der Befehl endet mit 0, wenn das
Deploy angewendet wurde oder ein Trockenlauf war, und mit 2, wenn die Umgebung geschützt ist und das Deploy
nur vorgeschlagen wurde — eine Pipeline, die eine ausstehende Genehmigung als freigegebenes Deploy behandelte,
läge falsch, und der Exit-Code sagt das. [tool-verified: `_cmd_env_deploy()` at cli.py lines 389-411]

```
provisa env deploy --org acme --env prod --ref main --token <token> --api <url>
```

`provisa env fetch` holt die entfernten Branches der Org in das lokale Repository. Ein Deploy kann dann
`origin/<branch>` benennen. [tool-verified: `_cmd_env_fetch()` at cli.py lines 414-426]

```
provisa env fetch --org acme --api <url> --token <token>
```

Beide Befehle akzeptieren `--api` (die Provisa-API-URL) und `--token` (ein Bearer-Token). Setzen Sie
`PROVISA_API_URL` und `PROVISA_API_TOKEN` in der Umgebung, um sie nicht bei jedem Aufruf übergeben zu müssen.
[inferred: shared `_api_call()` helper]

Die typische CI-Pipeline für einen repository-gestützten Arbeitsablauf:

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
provisa env deploy --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## Siehe auch

- [Deployment](deployment.md) — wie Sie die Control Plane aufsetzen, mit der sich Umgebungen verbinden
- [Commands](commands.md) — verfolgte Funktionen und Webhooks, die im Baum jeder Umgebung erscheinen
