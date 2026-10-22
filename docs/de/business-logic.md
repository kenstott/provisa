# Geschäftslogik in Provisa

## Das Problem {: #the-problem }

Jede Organisation hat Standards für Geschäftslogik, und die meisten Teams halten sich daran. Doch über die Jahre, während Systeme gebaut und ersetzt werden und Teams wechseln, verteilt sich die Logik auf ETL-Jobs, Dashboards, APIs und gespeicherte Prozeduren. Jedes Stück ergibt für sich Sinn. Oft hat niemand das Ganze geplant, und noch öfter hat niemand das Ganze über die Zeit geplant. Das Ergebnis ist bekannt: „Nettoumsatz“ wird an drei Stellen auf drei Arten berechnet, und niemand kann sagen, welche stimmt.

Für Teile davon gibt es Antworten aus der Branche. Ein Metrics Layer, oft als Semantic Layer verkauft, definiert „Nettoumsatz“ einmal für jedes Werkzeug. Der [Open Data Product Standard](https://github.com/bitol-io/open-data-product-standard) (ODPS) beschreibt Datenprodukte und der [Open Data Contract Standard](https://github.com/bitol-io/open-data-contract-standard) (ODCS) beschreibt Datenverträge. Provisa [exportiert beide](metadata-export.md#data-products-as-odps-and-odcs). Alle sind gute Ideen und alle gewinnen an Verbreitung, aber jede deckt nur einen Teil ab. Die größere Frage ist, wie sie sich als ein stimmiges, zusammensetzbares Modell in kontrollierte, gesteuerte Daten einfügen. Logik, die Daten ableitet oder ändert, braucht dieselbe Governance wie die Daten selbst: wer sie nutzen darf, was sie liest und schreibt und wohin ihre Ergebnisse gehen.

## Was Geschäftslogik ist {: #definition }

Geschäftslogik sind die Regeln, die Daten erzeugen, ändern, löschen oder umformen.

Die Fachterminologie ist etwas anderes. Die Namen und Beschreibungen von Domänen, Tabellen und Spalten und die Begriffe im [Glossar](glossary.md) sagen, was Daten bedeuten. Geschäftslogik sagt, was mit ihnen geschieht.

## Drei Rollen {: #roles }

Jede Regel spielt eine von drei Rollen, und jede Rolle lässt sich auf drei Arten schreiben:

| Rolle | SQL | Externer Aufruf | Code |
|---|---|---|---|
| **Datensatz** | View, Statement-Befehl | API oder Prozedur, die Zeilen zurückgibt | Code-Resolver `@table`, `@stream`; `python`- oder `script`-Befehl |
| **Wert** | View-Ausdruck, Metrik | — | `python`- oder `script`-Befehl |
| **Änderung** | `INSERT`, `UPDATE`, `DELETE`, Statement-Befehl | API-Schreibzugriff, Mutation, Prozedur | `@command`; `python`- oder `script`-Befehl |

- **Externer Aufruf.** Die GraphQL-, REST- oder gRPC-API eines anderen Systems oder die gespeicherte Prozedur einer Datenbank, als Befehl registriert.
- **Tabellen als Argumente.** Ein Befehl kann Tabellen als Argumente nehmen, sodass ein Datensatz aus anderen Datensätzen gebaut werden kann.
- **Metrik.** Ein benanntes Aggregat, einmal definiert, etwa `net_revenue` als `SUM(orders.amount) - SUM(orders.refunds)`. Jede Abfrage schlüsselt es nach den Dimensionen auf, die sie anfragt, sodass Warehouse, Dashboard und API dieselbe Zahl lesen.

## Versteckte Nebenwirkungen {: #side-effects }

Eine versteckte Nebenwirkung ist eine, die man der Definition der Regel nicht ansieht.

Jede Regel ist benannt, typisiert, registriert, auditiert und abgesichert. Ein Befehl kann zusätzlich eine Freigabe verlangen, bevor er läuft.

| | Mögliche Nebenwirkungen | Begrenzt durch |
|---|---|---|
| **SQL** | Keine | — |
| **Externer Aufruf** | Die eigenen Workflows, Benachrichtigungen und Kaskaden des anderen Systems | Egress-Allowlist (HTTP, gRPC); als Secrets hinterlegte Zugangsdaten (Prozeduren, Quelloperationen) |
| **Code-Resolver** | Aufrufe an externe Dienste und Netzwerke; Ergebnisse, die schwanken; Kosten | Gepinnter Code, gepinnter Host-Fingerabdruck, Sandbox, Messung |
| **Host-Code** | Liest alles, was der Server lesen kann (Speicher, Zugangsdaten, Dateien, Netzwerk); kann den Server aushungern oder lahmlegen | `commands.allow_host_code`, standardmäßig aus |

Ein externer Aufruf ist durch das eigene Schema des Systems typisiert, oder durch die Argumente und Rückgabespalten, die Sie beim Registrieren des Befehls angeben.

Der Code eines Code-Resolvers ist über Paketversion, Git-Commit oder Archiv-Hash gepinnt und wird daher mit dem Modell befördert.

Host-Code ist ein `python`- oder `script`-Befehl. Er läuft im Server mit dessen vollem Zugriff, sodass bösartiger, fehlerhafter oder gieriger Code alles lesen kann, was der Server lesen kann, ihn aushungern oder lahmlegen kann. Das ist bequem und sicher, wenn Sie dem Code vertrauen. Das Modell enthält nur eine Referenz (`module:attr` oder eine Befehlszeile), sodass der Code bei der Beförderung nicht mitwandert. Er läuft nur, wo `commands.allow_host_code` eingeschaltet ist.

Diese Grenzen dämmen Nebenwirkungen ein, können sie aber nicht beseitigen. Was ein externes System oder Ihr eigener Code tut, liegt letztlich bei ihm.

## Was Ihnen das Modell gibt {: #what-you-get }

- Jede Regel ist in einer Domäne registriert, mit Berechtigungen.
- Jede Regel wird gleich gesteuert, egal wie sie geschrieben ist.
- Jede Regel ist in der [Lineage](lineage.md) sichtbar.

## Weiterlesen {: #read-next }

- [Views](configuration.md#views-governed-computed-datasets)
- [Metriken](modeling.md#metrics)
- [Befehle](commands.md)
- [Code-Resolver](code-sources.md)
- [Datenqualitätsprüfungen](sources.md#data-quality-checkers)
