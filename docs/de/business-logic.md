# Geschäftslogik in Provisa

## Das Problem {: #the-problem }

Geschäftslogik landet überall: in ETL-Jobs, Dashboards und APIs, im Besitz verschiedener Teams und von niemandem katalogisiert. „Nettoumsatz“ wird im Warehouse-Job auf eine Weise berechnet, im Finanz-Dashboard auf eine andere und in der Abrechnungs-API auf eine dritte, und niemand kann sagen, welche stimmt.

Das ist niemandes Schuld. Es gab kein gemeinsames, einfaches Modell, in das man die Regeln legen konnte.

## Was Geschäftslogik ist {: #definition }

Geschäftslogik sind die Regeln, die Daten erzeugen, ändern, löschen oder umformen.

Die Fachterminologie ist etwas anderes. Die Namen und Beschreibungen von Domänen, Tabellen und Spalten und die Begriffe im [Glossar](glossary.md) sagen, was Daten bedeuten. Geschäftslogik sagt, was mit ihnen geschieht.

## Drei Rollen {: #roles }

Jede Regel spielt eine von drei Rollen, und jede Rolle lässt sich auf drei Arten schreiben:

| Rolle | SQL | Externer Aufruf | Code-Resolver |
|---|---|---|---|
| **Datensatz** | View, Statement-Befehl | API oder Prozedur, die Zeilen zurückgibt | `@table`, `@stream` |
| **Wert** | View-Ausdruck, Metrik | — | — |
| **Änderung** | `INSERT`, `UPDATE`, `DELETE`, Statement-Befehl | API-Schreibzugriff, Mutation, Prozedur | `@command` |

- **Externer Aufruf.** Die GraphQL-, REST- oder gRPC-API eines anderen Systems oder die gespeicherte Prozedur einer Datenbank, als Befehl registriert.
- **Metrik.** Ein benanntes Aggregat, einmal definiert, etwa `net_revenue` als `SUM(orders.amount) - SUM(orders.refunds)`. Jede Abfrage schlüsselt es nach den Dimensionen auf, die sie anfragt, sodass Warehouse, Dashboard und API dieselbe Zahl lesen.

## Versteckte Nebenwirkungen {: #side-effects }

Eine versteckte Nebenwirkung ist eine, die man der Definition der Regel nicht ansieht.

Jede Regel ist benannt, typisiert, registriert, auditiert und abgesichert.

| | Mögliche Nebenwirkungen | Begrenzt durch |
|---|---|---|
| **SQL** | Keine | — |
| **Externer Aufruf** | Die eigenen Workflows, Benachrichtigungen und Kaskaden des anderen Systems | Freigabe |
| **Code-Resolver** | Aufrufe an externe Dienste und Netzwerke; Ergebnisse, die schwanken; Kosten | Sandbox, Messung |

Ein externer Aufruf ist durch das eigene Schema des Systems typisiert, oder durch die Argumente und Rückgabespalten, die Sie beim Registrieren des Befehls angeben.

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
