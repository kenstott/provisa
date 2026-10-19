# Umgebungen {: #environments }

Eine Umgebung ist eine benannte Kopie des gesteuerten Modells einer Organisation. Physisch ist die Kopie ein eigenes PostgreSQL-Schema: ein echtes Schema, ohne Diskriminatorspalte und ohne Präfix. Darum ist jede vorhandene Repository-Abfrage in einer Umgebung korrekt, ohne umgeschrieben zu werden, und ein vergessenes Prädikat kann die Zeilen einer Umgebung nicht aus einer anderen lesbar machen.

Jede Organisation startet mit einer Umgebung namens `prod`. Sie kann weder gelöscht noch umbenannt werden.
Eine Anfrage, die keine Umgebung benennt, wird von `prod` bedient; eine Anfrage, die eine nicht existierende
Umgebung benennt, wird abgelehnt.

Umgebungen stehen Organisationen mit einem kostenpflichtigen Tarif zur Verfügung.

Wie Sie eine Nicht-Produktionsumgebung mit gefahrlos nutzbaren Daten füllen, beschreibt [Testumgebungen aufbauen](test-data.md).

## Umgebungsnamen {: #environment-names }

Ein Name muss `[a-z][a-z0-9_]{1,31}` entsprechen: zwei bis zweiunddreißig Kleinbuchstaben, Ziffern und Unterstriche, beginnend mit einem Buchstaben. `prod` und Namen, die mit `pg_` beginnen, werden abgelehnt. Die Höchstlänge für eine Organisation hängt von deren eigener ID ab. PostgreSQL kürzt einen Bezeichner über 63 Byte stillschweigend, und die Grenze schützt vor dem längsten Schemanamen, den eine Umgebung ableitet.

## Was eine Kopie mitnimmt {: #what-a-copy-carries }

Jede Tabelle im Org-Schema gehört genau einer Klasse an. Die Einteilung ist eine Positivliste: Eine später hinzugefügte Tabelle reist nicht mit, bis jemand hier ihre Klasse nennt, sodass eine vergessene Tabelle als roter Test auffällt.

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

Bindungen sind die Spalten, die sagen, wohin eine Quelle tatsächlich zeigt: `host`, `port`, `database`, `username` und die übrigen. Sie reisen nie in einer Kopie mit. Eine Umgebung, die noch nicht gebunden ist, wird als `unbound` markiert statt leer gelassen, denn ein leerer Host ist nicht dasselbe wie kein Host, und der Verbindungsaufbau würde ihn als `localhost:5432` lesen.

Eine Umgebung, die nicht als Unbound angelegt wird, kopiert die Verbindung jeder Quelle von ihrer Elternumgebung,
genau so, wie die Elternumgebung sie geschrieben hat: Ein `${secret:...}`- oder `${env:...}`-Verweis wird als dieser
Verweis kopiert, nie aufgelöst. Nach dem Anlegen ist die Kopie die eigene Zeile der Umgebung, auf der Seite Quellen
bearbeitbar, und beim Aufbau der Laufzeit der Umgebung wird nichts über die Elternumgebung aufgelöst.
Das Feld `binding` jeder Quelle sagt, woher ihre Verbindung stammt: `copied`, `own` (in dieser Umgebung angegeben)
oder `unbound`. Wird eine kopierte Verbindung bearbeitet, wird sie zu `own`. **Von der Elternumgebung neu kopieren**
(`POST /admin/orgs/{org}/environments/{name}/sources/recopy`) kopiert die Verbindungen der Elternumgebung erneut.
Ein Merge oder Deploy trägt keine Verbindungen, daher kommt eine Quelle, die er einführt, als `unbound` an.

## Die Git-Projektion {: #the-git-projection }

Jeder Schreibvorgang im Modell committet das Ergebnis auf den Git-Branch der Umgebung. Einen Baum zu deployen, verlangt einen ausdrücklichen Aufruf: Ein gemergter Pull Request auf dem Git-Host deployt sich nicht selbst.

Jede Entität bekommt eine Datei. Der Pfad ist die `provisa://`-Adresse der Entität ohne Schema und Organisation: `provisa://acme/sales/tables/Order` wird zu `sales/tables/Order.yaml`. Quellen landen in `sources/`, Commands in `commands/` und Metriken in `metrics/`. Kindzeilen, die von einem Elternteil abhängen (Spalten, Beziehungen, RLS-Regeln), stehen in der Datei des Elternteils und haben keine eigenen Dateien.

Commands und ihre Tag-Zuweisungen überstehen den Hin- und Rückweg. Ein Tag auf einem Command wird in die
eigene Datei des Commands geleitet (`commands/<name>.yaml`); ein Tag, das zu keiner Datei gehört,
verschwindet aus der Projektion und würde beim nächsten Deploy dieses Baums gelöscht.

Kein Surrogatschlüssel gelangt in eine Datei. `registered_tables.id` ist eine automatisch hochgezählte Ganzzahl, dasselbe Modell bekommt also in zwei Umgebungen verschiedene Zahlen, und ein naiver Dump würde sich von sich selbst unterscheiden. Jeder Surrogatschlüssel wird weggelassen, und jeder Verweis darauf wird als Pfad des Ziels geschrieben.

Die Serialisierung ist deterministisch. Schlüssel werden alphabetisch ausgegeben, Kindsammlungen nach ihrer
Adresse sortiert, und der YAML-Stil liegt fest. Zwei Umgebungen, die dasselbe Modell halten, erzeugen
byte-identische Bäume.

## Merge

Das Mergen des Modells einer Umgebung in eine andere aktualisiert nach Identität: Jedes Objekt, das die Quelle hat, wird im Ziel angelegt oder aktualisiert. Objekte, die die Quelle nicht mehr hat, werden nur entfernt, wenn der Aufrufer ausdrücklich Entfernungen verlangt. Ein Merge ist eine Transaktion; scheitert er mittendrin, bleibt das Ziel, wie es war.

Rufen Sie vor dem Anwenden den Vorschau-Endpunkt auf (`GET /{name}/merge-preview`) oder übergeben Sie
`dry_run: true`. Die Vorschau läuft denselben Codepfad, den der Merge verwendet; sie ist ein `GET`-Endpunkt,
damit ein CI-Skript, das das Flag falsch setzt, nicht versehentlich den Merge anwendet, den es prüfen wollte.

Ein Merge lässt Bindungen, Rollen und Secrets des Ziels genau so, wie sie waren. Eine Dev-Umgebung verliert
ihre eigenen Datenbankverbindungen nicht dadurch, dass sie ein neueres Modell aus prod übernimmt. Prod erwirbt
nicht die Grants von dev.

### Was der Bericht benennt {: #what-the-report-names }

Der Merge-Bericht listet nach Pfad, was hinzugefügt, geändert, entfernt und belassen wurde. Er nennt auch alle **Konflikte**: Objekte, die beide Seiten geändert haben, seit sie zuletzt einen Commit teilten. Ein Konflikt wird gemeldet, und die Quelle gewinnt, denn das bedeutet ein Merge in ein Ziel. Provisa bietet keine Konfliktlösung, keine Merge-Markierungen und keine Auswahl pro Objekt. Die Konfliktliste ist als Signal wertvoll: Zwei Menschen haben dasselbe Objekt bearbeitet, ohne es zu wissen.

Haben beide Seiten ein Objekt auf denselben Wert geändert, stimmen sie überein, und es ist kein Konflikt. Teilen die beiden Umgebungen gar keinen Vorfahren, ist die Basis im Bericht `None`, und eine leere Konfliktliste heißt, dass nichts verglichen wurde, nicht, dass nichts kollidiert ist.

Der Merge landet als ein zusammengefasster Commit auf dem Branch des Ziels. Die Commit-Nachricht ist Pflicht und darf nicht leer sein, weil sie die einzige Beschreibung der Arbeit ist, für die der zusammengefasste Commit steht. Die Commits der Quelle bleiben, wo sie sind, und lassen sich danach weiterhin per SHA deployen.

## Pull

Ein Pull nimmt, was die Gegenstelle für eine Umgebung hält, und macht es zum Modell. Er spult den lokalen
Branch nicht direkt vor; er wendet den geholten Baum über den gewöhnlichen Deploy-Pfad an, sodass dieselbe
Validierung und dieselbe Auditierung, die ein manuelles Deploy regieren, auch einen Pull regieren.

Wie ein Merge meldet ein Pull, was er überschrieben hat: Objekte, die der eingehende Baum geändert hat und die die lokale Umgebung seit dem letzten gemeinsamen Commit der beiden Linien ebenfalls geändert hatte. Eine nie committete lokale Änderung macht die Umgebung abgedriftet (siehe Verlauf unten), und ein Pull nennt sie im Bericht als gewöhnliche Änderung.

Ein Pull wird abgelehnt, wenn die beiden Linien **auseinandergelaufen** sind, also jede Commits hält, die die andere nicht hat. Die Ablehnung listet die Objekte, die beide Seiten berührt haben, damit derjenige, der nun entscheiden muss, wessen Arbeit bleibt, weiß, welche Objekte er ansehen muss.

## Historie {: #history }

Jedes Deployment schiebt den Zeiger der Umgebung auf ihrer eigenen Commit-Linie vorwärts. Ein Rückgängig geht einen Commit zurück, ein Wiederholen geht wieder vorwärts in Richtung der Stelle, an der das Rückgängig begann. Keines von beiden entfernt einen Commit: Ein Schritt zurück fügt eine Position hinzu und lässt den Verlauf, wie er ist.

Ein Branch wird an der Spitze der Umgebung angesät, aus der er angelegt wurde, sodass ein Undo an diesem
Ansaatpunkt stoppt und nicht auf die Commits der Elternumgebung wandert.

Die Flags `can_undo` und `can_redo` kommen mit der Antwort der Umgebungsliste. Beide melden `false`, wenn die Projektion den Commit nicht hält, den die Steuerungsebene nennt, ein Zustand, den das Design zulässt und **abgedriftet** nennt. Ein Knoten, dessen Repository-Speicher einen bestimmten Commit nie erhalten hat, listet seine Umgebungen trotzdem; nur die Antworten zum Verlauf ändern sich.

## Autorisierung {: #authorization }

Umgebungen werden von zwei Rechten regiert. Keines gehört standardmäßig einer Analystin.

| Recht | Wer es hält (angesät) | Was es regiert |
| --- | --- | --- |
| `environment_management` | org_admin, developer | Umgebungen anlegen und löschen |
| `environment_switch` | org_admin, developer | Von einer anderen Umgebung als prod bedient werden |

`prod` braucht kein Recht. Es bedient jede Anfrage, die keine Umgebung nennt, und es zu verweigern hieße, jede Anfrage zu verweigern.

Die Prüfung findet dort statt, wo die Umgebung gewählt wird, bevor irgendeine Route erreicht ist. Ein Mitglied ohne `environment_switch` wird auf allen Oberflächen zugleich abgewiesen (HTTP, GraphQL, SQL und die Wire-Protokolle), weil die Umgebung in der Middleware gebunden wird und nicht in einzelnen Handlern.

Eine Analystin ohne Umgebungsrecht kann `prod` abfragen und sieht den Umgebungswechsler nicht. Eine externe
Kraft mit der Analystenrolle sieht keine Umgebungsoberfläche und kann keine andere Umgebung als die Produktion
anlegen oder in sie wechseln.

### Die Autorität der Umgebungseigentümerin {: #environment-owner-authority }

Eine Umgebung anzulegen, ist der einzige Weg, auf dem ein Mitglied mit Leserechten Rechte zum Bearbeiten des Modells bekommt. In einer selbst angelegten Umgebung hat der Ersteller die Fähigkeiten der Rolle `developer`, abzüglich der Datenrechte (`write`, `full_results`, `usage`): Rechte zum Bauen des Modells, ohne Rechte an den Daten.

Die Vergabe wird zur Autorisierungszeit aus `environments.created_by` abgeleitet und nie in eine
Vergabetabelle geschrieben. Das Löschen der Umgebung entzieht sie im selben Akt.

Die Domänenmitgliedschaft begrenzt weiterhin, was die eigentümende Person ändern darf. Ein Branch ändert, was
ein Mitglied tun darf; er ändert nie, an welchen Domänen es das tun darf.

## Geschützte Umgebungen {: #protected-environments }

Eine Umgebung kann geschützt sein. Ein Merge oder Deploy in eine geschützte Umgebung wird nicht auf Anfrage
angewendet; er wird vorgeschlagen, und jemand anderes als die anfragende Person muss ihn genehmigen.

`prod` wird automatisch geschützt, sobald die Organisation mehr als ein Mitglied hat. Eine Organisation mit einem einzigen Mitglied kann „jemand anderes als der Antragsteller“ nicht erfüllen, daher gilt die Regel dort nicht; sie würde `prod` unmergebar machen. Ein org_admin kann jede Umgebung als geschützt markieren.

Ein Merge-Antrag ist eine gespeicherte Zeile, und kein Bestätigungsdialog ersetzt ihn. Wer genehmigt, ist per Definition jemand anderes als der Antragsteller und ist im Moment des Antrags nicht anwesend. Eine flüchtige Bestätigung würde die Genehmigung in die Sitzung des Antragstellers zwingen, und genau diese Anordnung verbietet die Regel.

Die Anfragezeile trägt den Merge-Bericht zusammen mit der Nachricht der anfragenden Person. Veraltung wird zur
Lesezeit abgeleitet, nie gespeichert: Zur Lesezeit neu zu planen und mit dem gespeicherten Bericht zu
vergleichen ist die einzige Variante, die nicht falsch sein kann. Eine veraltete Anfrage muss neu gestellt
werden. Die anfragende Person kann ihre eigene Anfrage nicht genehmigen.

Zustände im Lebenszyklus einer Anfrage: `requested` → `approved`/`rejected` → `applied`. `stale` wird
abgeleitet.

Deployments aus einer Repository-Referenz gehen durch dieselbe Tür, und der Antrag hält den SHA beim Vorschlag fest. Bewegt sich die Referenz zwischen Vorschlag und Entscheidung, liest der Genehmiger den Bericht zum festgehaltenen Commit, nicht zum neuen.

!!! note
    Die Oberfläche für Merge-Anfragen liegt im Tab **Merge-Anfragen** des Umgebungs-Panels.
    Die Spalte **Bericht** zeigt anhand von Anzahlen, was sich ändern würde; die Zeile klappt auf und zeigt
    die Details pro Objekt.

## Die `env`-CLI-Befehle {: #the-env-cli-commands }

`provisa env deploy` schickt das Modell an einer Referenz in eine Umgebung. Es endet mit 0, wenn das Deployment angewendet wurde oder ein Probelauf war, und mit 2, wenn die Umgebung geschützt ist und das Deployment nur vorgeschlagen wurde. Eine Pipeline, die eine offene Genehmigung als ausgeliefertes Deployment behandelt, läge falsch, und der Exit-Code unterscheidet beides.

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
