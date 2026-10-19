# Umgebungen {: #environments }

Eine Umgebung ist eine benannte Kopie des regierten Modells einer Organisation. Die Kopie ist physisch ein
eigenes PostgreSQL-Schema — keine Diskriminatorspalte, kein Präfix, ein echtes Schema —, sodass jede
bestehende Repository-Abfrage innerhalb einer Umgebung korrekt ist, ohne dass etwas umgeschrieben wird, und
die Zeilen der einen Umgebung nicht durch ein vergessenes Prädikat in den Lesezugriff einer anderen geraten
können.

Jede Organisation startet mit einer Umgebung namens `prod`. Sie kann weder gelöscht noch umbenannt werden.
Eine Anfrage, die keine Umgebung benennt, wird von `prod` bedient; eine Anfrage, die eine nicht existierende
Umgebung benennt, wird abgelehnt.

Umgebungen stehen Organisationen mit einem kostenpflichtigen Tarif zur Verfügung.

Wie Sie eine Nicht-Produktionsumgebung mit gefahrlos nutzbaren Daten füllen, beschreibt [Testumgebungen aufbauen](test-data.md).

## Umgebungsnamen {: #environment-names }

Ein Name muss `[a-z][a-z0-9_]{1,31}` entsprechen — zwei bis zweiunddreißig Zeichen aus Kleinbuchstaben,
Ziffern und Unterstrichen, beginnend mit einem Buchstaben. `prod` und Namen, die mit `pg_` beginnen, werden
abgelehnt. Die Maximallänge für eine bestimmte Org hängt von der ID dieser Org ab: PostgreSQL kürzt einen
Bezeichner über 63 Byte stillschweigend, und der längste Schemaname, den eine Umgebung ableitet, ist das,
wovor die Obergrenze schützt.

## Was eine Kopie mitnimmt {: #what-a-copy-carries }

Jede Tabelle im Org-Schema fällt in genau eine Klasse. Die Klassifizierung ist eine Allow-List,
keine Ausschlussliste: Eine später hinzugefügte Tabelle reist nicht mit, bis jemand hier ihre Klasse benennt
— die Fehlerwirkung für eine vergessene Tabelle ist also ein roter Test.

| Klasse | Tabellen | Was beim Kopieren geschieht |
| --- | --- | --- |
| CARRIED | domains, naming_rules, registered_tables, table_columns, relationships, metrics, roles, rls_rules, tags, tag_param_values, tag_assignments, glossary terms, materialized_views, calendars, api_endpoints, tracked_functions, tracked_webhooks, table_meta_links | Vollständig kopiert |
| IDENTITY_ONLY | sources, api_sources, kafka_sources | Identitäts- und Governance-Felder reisen mit; Verbindungswerte bleiben zurück (siehe Bindungen) |
| SEEDED_AT_CREATION | roles, user_role_assignments | Nur beim erstmaligen Anlegen einer Umgebung kopiert; spätere Merges lassen sie unberührt |
| PARTIAL | org_settings | Pro Schlüssel kopiert: Governance-Einstellungen reisen mit, Schlüssel, die ein externes Ziel oder eine umgebungsspezifische Laufzeit benennen, bleiben zurück |
| NEVER_SENSITIVE | org_secrets, user_directory | Nie kopiert |
| NEVER_RUNTIME | mv_refresh_log, relationship_candidates, admin_audit_log und weitere | Nie kopiert |

`SEEDED_AT_CREATION` existiert, um ein einziges bestimmtes Problem zu lösen. Eine neue Umgebung braucht
Rollen und Zuweisungen, sonst öffnet sie, ohne dass irgendjemand handeln kann. Aber ein späterer Merge, der
die `developer`-Zeile aus `prod` mitbrächte, würde die eingeschränkte Fassung überschreiben, die ein
eingeschränkter Branch womöglich braucht — und damit den Review-Pfad zur Eskalationsroute machen. Rollen und
Zuweisungen reisen deshalb einmal mit, beim Anlegen, und sind danach die eigene Antwort jeder Umgebung.

## Bindungen {: #bindings }

Bindungen sind die Spalten, die sagen, wohin eine Quelle tatsächlich zeigt — `host`, `port`, `database`,
`username` und der Rest. Sie reisen in keiner Kopie mit. Eine Umgebung, die noch nicht gebunden wurde, wird
als `unbound` markiert, statt leer gelassen zu werden: Ein leerer Host ist kein fehlender, und der
Verbindungsaufbau würde ihn als `localhost:5432` lesen.

Eine Umgebung, die nicht als Unbound angelegt wird, kopiert die Verbindung jeder Quelle von ihrer Elternumgebung,
genau so, wie die Elternumgebung sie geschrieben hat: Ein `${secret:...}`- oder `${env:...}`-Verweis wird als dieser
Verweis kopiert, nie aufgelöst. Nach dem Anlegen ist die Kopie die eigene Zeile der Umgebung, auf der Seite Quellen
bearbeitbar, und beim Aufbau der Laufzeit der Umgebung wird nichts über die Elternumgebung aufgelöst.
Das Feld `binding` jeder Quelle sagt, woher ihre Verbindung stammt: `copied`, `own` (in dieser Umgebung angegeben)
oder `unbound`. Wird eine kopierte Verbindung bearbeitet, wird sie zu `own`. **Von der Elternumgebung neu kopieren**
(`POST /admin/orgs/{org}/environments/{name}/sources/recopy`) kopiert die Verbindungen der Elternumgebung erneut.
Ein Merge oder Deploy trägt keine Verbindungen, daher kommt eine Quelle, die er einführt, als `unbound` an.

## Die Git-Projektion {: #the-git-projection }

Jeder Schreibvorgang am Modell committet das Ergebnis in den Git-Branch der Umgebung. Einen Baum zu deployen erfordert einen ausdrücklichen Aufruf — ein
gemergter Pull Request auf dem Git-Host deployt sich nicht von selbst.

Jede Entität bekommt eine Datei. Der Pfad ist die `provisa://`-Adresse der Entität ohne Schema und Org:
`provisa://acme/sales/tables/Order` wird zu `sales/tables/Order.yaml`. Quellen landen in `sources/`,
Commands in `commands/`, Metriken in `metrics/`. Kindzeilen, die von einem Elternteil kaskadieren — Spalten,
Beziehungen, RLS-Regeln —, werden in die Datei des Elternteils geschrieben, nicht als eigene Dateien.

Commands und ihre Tag-Zuweisungen überstehen den Hin- und Rückweg. Ein Tag auf einem Command wird in die
eigene Datei des Commands geleitet (`commands/<name>.yaml`); ein Tag, das zu keiner Datei gehört,
verschwindet aus der Projektion und würde beim nächsten Deploy dieses Baums gelöscht.

Kein Surrogatschlüssel gelangt in eine Datei. `registered_tables.id` ist eine automatisch hochzählende
Ganzzahl — dasselbe Modell bekommt in zwei Umgebungen unterschiedliche Zahlen, sodass ein naiver Dump gegen
sich selbst diffed. Jedes Surrogat wird verworfen und jede Referenz darauf als Pfad des Ziels geschrieben.

Die Serialisierung ist deterministisch. Schlüssel werden alphabetisch ausgegeben, Kindsammlungen nach ihrer
Adresse sortiert, und der YAML-Stil liegt fest. Zwei Umgebungen, die dasselbe Modell halten, erzeugen
byte-identische Bäume.

## Merge

Das Mergen des Modells einer Umgebung in eine andere aktualisiert über die Identität: Jedes Objekt, das die
Quelle hat, wird im Ziel angelegt oder aktualisiert. Objekte, die die Quelle nicht mehr hat, werden nur
entfernt, wenn die aufrufende Seite Entfernungen ausdrücklich anfordert. Ein Merge, der auf halbem Weg
scheitert, lässt das Ziel so, wie es war — eine Transaktion.

Rufen Sie vor dem Anwenden den Vorschau-Endpunkt auf (`GET /{name}/merge-preview`) oder übergeben Sie
`dry_run: true`. Die Vorschau läuft denselben Codepfad, den der Merge verwendet; sie ist ein `GET`-Endpunkt,
damit ein CI-Skript, das das Flag falsch setzt, nicht versehentlich den Merge anwendet, den es prüfen wollte.

Ein Merge lässt Bindungen, Rollen und Secrets des Ziels genau so, wie sie waren. Eine Dev-Umgebung verliert
ihre eigenen Datenbankverbindungen nicht dadurch, dass sie ein neueres Modell aus prod übernimmt. Prod erwirbt
nicht die Grants von dev.

### Was der Bericht benennt {: #what-the-report-names }

Der Merge-Bericht führt nach Pfad auf, was hinzugefügt, geändert, entfernt und unverändert gelassen wurde.
Er benennt außerdem alle **Konflikte** — Objekte, die beide Seiten seit dem letzten gemeinsamen Commit
geändert haben. Ein Konflikt wird gemeldet und nicht aufgelöst: Die Quelle gewinnt, denn genau das bedeutet
ein Merge in ein Ziel. Provisa bietet keine Konfliktauflösung, keine Merge-Marker, keine Auswahl pro Objekt.
Der Wert der Konfliktliste ist das Signal — zwei Personen haben dasselbe Objekt bearbeitet, ohne voneinander
zu wissen.

Ein Objekt, das beide Seiten auf denselben Wert geändert haben, ist Übereinstimmung, kein Konflikt. Teilen
die beiden Umgebungen überhaupt keinen Vorfahren, ist die Basis im Bericht `None`, und die leere Konfliktliste
bedeutet, dass nichts verglichen wurde, nicht dass nichts kollidiert ist.

Der Merge landet als ein einzelner gequetschter Commit auf dem Branch des Ziels. Die Commit-Nachricht ist
erforderlich und darf nicht leer sein — sie ist die einzige Rechenschaft über den Arbeitsbereich, für den der
Squash steht. Die Commits der Quelle bleiben, wo sie sind, und bleiben danach per SHA deploybar.

## Pull

Ein Pull nimmt, was die Gegenstelle für eine Umgebung hält, und macht es zum Modell. Er spult den lokalen
Branch nicht direkt vor; er wendet den geholten Baum über den gewöhnlichen Deploy-Pfad an, sodass dieselbe
Validierung und dieselbe Auditierung, die ein manuelles Deploy regieren, auch einen Pull regieren.

Wie ein Merge meldet ein Pull, was er überschrieben hat — Objekte, die der eingehende Baum geändert hat und
die die lokale Umgebung seit dem letzten gemeinsamen Commit der beiden Linien ebenfalls geändert hatte. Eine
nicht committete lokale Änderung ist eine abgedriftete Umgebung (siehe Historie weiter unten); ein Pull
benennt sie im Bericht als gewöhnliche Änderung.

Ein Pull wird abgelehnt, wenn die beiden Linien **auseinandergelaufen** sind — beide halten Commits, die die
andere nicht hat. Die Ablehnung trägt die Liste der Objekte, die beide Seiten angefasst haben, damit die
Person, die nun entscheiden muss, wessen Arbeit überlebt, weiß, welche Objekte sie ansehen muss.

## Historie {: #history }

Jedes Deploy bewegt den Cursor der Umgebung in ihrer eigenen Commit-Linie vorwärts. Ein Undo tritt einen
Commit zurück; ein Redo tritt wieder vorwärts in Richtung der Position, die das Undo verlassen hat. Keine der
beiden Operationen entfernt einen Commit — ein Rückschritt fügt eine Position hinzu, er schreibt die Historie
nicht um.

Ein Branch wird an der Spitze der Umgebung angesät, aus der er angelegt wurde, sodass ein Undo an diesem
Ansaatpunkt stoppt und nicht auf die Commits der Elternumgebung wandert.

Die Flags `can_undo` und `can_redo` reisen mit der Antwort der Umgebungsliste mit. Beide melden `false`, wenn
die Projektion den Commit nicht hält, den die Control Plane benennt — ein Zustand, den der Entwurf zulässt und
der **abgedriftet** heißt. Ein Knoten, dessen Repository-Speicher einen bestimmten Commit nie erhalten hat,
listet seine Umgebungen weiterhin auf; nur die Historie-Antworten ändern sich.

## Autorisierung {: #authorization }

Umgebungen werden von zwei Rechten regiert. Keines gehört standardmäßig einer Analystin.

| Recht | Wer es hält (angesät) | Was es regiert |
| --- | --- | --- |
| `environment_management` | org_admin, developer | Umgebungen anlegen und löschen |
| `environment_switch` | org_admin, developer | Von einer anderen Umgebung als prod bedient werden |

`prod` braucht kein Recht — es ist das, wovon eine Anfrage bedient wird, die nichts benennt, und es zu
verweigern hieße, jede Anfrage zu verweigern.

Die Durchsetzung erfolgt am Auswahlpunkt, bevor irgendeine Route erreicht wird. Ein Mitglied ohne
`environment_switch` wird für alle Oberflächen zugleich abgelehnt — HTTP, GraphQL, SQL und die
Wire-Protokolle —, weil die Umgebung in der Middleware gebunden wird, nicht in einzelnen Handlern.

Eine Analystin ohne Umgebungsrecht kann `prod` abfragen und sieht den Umgebungswechsler nicht. Eine externe
Kraft mit der Analystenrolle sieht keine Umgebungsoberfläche und kann keine andere Umgebung als die Produktion
anlegen oder in sie wechseln.

### Die Autorität der Umgebungseigentümerin {: #environment-owner-authority }

Eine Umgebung anzulegen ist der einzige Weg, auf dem ein nur lesendes Mitglied Rechte zur Modellbearbeitung
erwirbt. Innerhalb der Umgebung, die sie angelegt hat, hält die erstellende Person die Capabilities
der Rolle `developer` — abzüglich der Datenrechte (`write`, `full_results`, `usage`). Rechte zum Modellbau,
keine Datenrechte.

Die Vergabe wird zur Autorisierungszeit aus `environments.created_by` abgeleitet und nie in eine
Vergabetabelle geschrieben. Das Löschen der Umgebung entzieht sie im selben Akt.

Die Domänenmitgliedschaft begrenzt weiterhin, was die eigentümende Person ändern darf. Ein Branch ändert, was
ein Mitglied tun darf; er ändert nie, an welchen Domänen es das tun darf.

## Geschützte Umgebungen {: #protected-environments }

Eine Umgebung kann geschützt sein. Ein Merge oder Deploy in eine geschützte Umgebung wird nicht auf Anfrage
angewendet; er wird vorgeschlagen, und jemand anderes als die anfragende Person muss ihn genehmigen.

`prod` ist automatisch geschützt, sobald die Org mehr als ein Mitglied hat. Eine Org mit einem einzigen
Mitglied kann „jemand anderes als die anfragende Person“ nicht erfüllen, also gilt die Regel dort nicht — sie
würde `prod` unmergebar machen. Jede Umgebung kann von einem org_admin als geschützt markiert werden.

Eine Merge-Anfrage ist eine Zeile, kein Bestätigungsdialog. Die genehmigende Person ist definitionsgemäß eine
andere als die anfragende und ist im Moment der Anfrage nicht anwesend; eine flüchtige Bestätigung würde die
Genehmigung in die Sitzung der anfragenden Person zwingen — genau die eine Anordnung, die die Anforderung
verbietet.

Die Anfragezeile trägt den Merge-Bericht zusammen mit der Nachricht der anfragenden Person. Veraltung wird zur
Lesezeit abgeleitet, nie gespeichert: Zur Lesezeit neu zu planen und mit dem gespeicherten Bericht zu
vergleichen ist die einzige Variante, die nicht falsch sein kann. Eine veraltete Anfrage muss neu gestellt
werden. Die anfragende Person kann ihre eigene Anfrage nicht genehmigen.

Zustände im Lebenszyklus einer Anfrage: `requested` → `approved`/`rejected` → `applied`. `stale` wird
abgeleitet.

Dieselbe Tür bearbeitet Deploys von einer Repository-Ref: Die Anfrage nagelt den SHA zum Zeitpunkt des
Vorschlags fest. Bewegt sich die Ref zwischen Vorschlag und Entscheidung, liest die genehmigende Person den
Bericht für den festgenagelten Commit, nicht für den neuen.

!!! note
    Die Oberfläche für Merge-Anfragen liegt im Tab **Merge-Anfragen** des Umgebungs-Panels.
    Die Spalte **Bericht** zeigt anhand von Anzahlen, was sich ändern würde; die Zeile klappt auf und zeigt
    die Details pro Objekt.

## Die `env`-CLI-Befehle {: #the-env-cli-commands }

`provisa env deploy` schickt das Modell an einer Ref in eine Umgebung. Der Befehl endet mit 0, wenn das
Deploy angewendet wurde oder ein Trockenlauf war, und mit 2, wenn die Umgebung geschützt ist und das Deploy
nur vorgeschlagen wurde — eine Pipeline, die eine ausstehende Genehmigung als freigegebenes Deploy behandelte,
läge falsch, und der Exit-Code sagt das.

```
provisa env deploy --org acme --env prod --ref main --token <token> --api <url>
```

`provisa env fetch` holt die entfernten Branches der Org in das lokale Repository. Ein Deploy kann dann
`origin/<branch>` benennen.

```
provisa env fetch --org acme --api <url> --token <token>
```

Beide Befehle akzeptieren `--api` (die Provisa-API-URL) und `--token` (ein Bearer-Token). Setzen Sie
`PROVISA_API_URL` und `PROVISA_API_TOKEN` in der Umgebung, um sie nicht bei jedem Aufruf übergeben zu müssen.

Die typische CI-Pipeline für einen repository-gestützten Arbeitsablauf:

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
provisa env deploy --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## Siehe auch {: #see-also }

- [Deployment](deployment.md) — wie Sie die Control Plane aufsetzen, mit der sich Umgebungen verbinden
- [Commands](commands.md) — verfolgte Funktionen und Webhooks, die im Baum jeder Umgebung erscheinen
- [Testumgebungen aufbauen](test-data.md) — Produktion profilieren, Fakes deklarieren, synthetische Daten für eine Nicht-Produktionsumgebung erzeugen
- [Fake-Methoden](fake-methods.md) — jede Art von Fake und jede Methode, die eine Spalte deklarieren kann
