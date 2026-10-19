# Node- und Python-Quellen

Manchmal stecken die Daten, die Sie brauchen, hinter Code, den Ihr Team schon geschrieben hat: ein Scoring-Modell, ein Client für einen internen Dienst, ein Skript, das einen Export aufräumt. Mit einer Node- oder Python-Quelle stellen Sie diesen Code als Tabellen bereit. Sie markieren die Funktionen, die Sie teilen wollen, verweisen Provisa auf den Code, und jede markierte Funktion wird zu einer Tabelle, die jede Rolle nach denselben Regeln liest wie jede andere Tabelle.

## Was aus einer Funktion wird {: #what-a-function-becomes }

Provisa liest die Deklarationen Ihres Codes selbst: JSDoc-Tags und TypeScript-Typen bei Node, Docstrings und Type Hints bei Python. Drei Tags zählen.

| Tag | Wird zu |
|---|---|
| `@table` | Einer Tabelle. Die Funktion gibt eine Liste oder ein Iterable zurück, und jedes Element ist eine Zeile |
| `@command` | Einem Command, das auf Anfrage läuft |
| `@stream` | Einer Live-Tabelle. Die Funktion ist ein Generator, und jede Zeile wird gesendet, sobald sie geliefert wird |

Eine Funktion ohne Tag wird ignoriert. Ein Tag, das Provisa nicht kennt, wird abgelehnt, ebenso ein Typ, den es nicht abbilden kann. Die Ablehnung nennt die Funktion und listet alle Probleme auf einmal auf, sodass Sie sie in einem Durchgang beheben.

Der Zeilentyp liefert die Spalten. Text, Zahlen, Booleans, Datumswerte und Zeitstempel werden auf ihre Spaltentypen abgebildet, Listen bleiben Listen, und verschachtelte Objekte werden zu benannten Typen. Ist der Typ einer Zeile lose (`any`, `unknown` oder ein `Record` in TypeScript, `Any` oder ein `dict` in Python), ruft Provisa die Funktion einmal ohne Argumente auf und schlägt Spalten aus den zurückgegebenen Zeilen vor. Hat die Funktion einen Pflichtparameter, lässt sie sich so nicht aufrufen, und Sie typisieren die Spalten selbst, wenn Sie die Tabelle registrieren.

Die Parameter der Funktion werden zu den Parametern der Tabelle. Ein Parameter ohne Standardwert ist Pflicht, sodass ein Lesezugriff ohne ihn abgelehnt wird, bevor Ihr Code läuft. Ein Parameter mit Standardwert ist optional; lassen Sie ihn weg, gilt Ihr Standardwert. Ein Steward kann einen optionalen Parameter trotzdem zur Pflicht machen.

## Woher der Code kommt {: #where-the-code-comes-from }

Der Code ist festgelegt, sodass eine Quelle immer genau das ausführt, was registriert wurde:

- **Ein Paket**: ein npm-Paket für Node oder ein PyPI-Paket für Python, in einer exakten Version.
- **Ein Git-Repository**: ein vollständiger Commit mit 40 Zeichen, mit einem Unterverzeichnis, falls der Code nicht auf oberster Ebene liegt.
- **Ein Upload**: eine `.zip`, `.tgz` oder `.tar.gz` des Codes.

Node-Code läuft auf Node.js, Bun oder Deno, je nachdem, was Sie wählen. Python-Code läuft auf dem Python, das Provisa mitliefert. Hat der Code mehr als ein Python-Modul auf oberster Ebene, nennen Sie im Feld **Einstiegsmodul** dasjenige, über das er betreten wird.

## Abhängigkeiten installieren {: #installing-dependencies }

Code mit Abhängigkeiten braucht ein Lockfile, und jede Installation folgt ihm exakt. Ohne Lockfile wird ein Paket, das Abhängigkeiten deklariert, abgelehnt, und die Ablehnung sagt Ihnen, dass Sie eines hinzufügen sollen.

| Laufzeit | Lockfile |
|---|---|
| Node.js | `package-lock.json` oder `npm-shrinkwrap.json` |
| Bun | `bun.lock` oder `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock` (mit seiner `pyproject.toml`), `pylock.toml` oder eine `requirements.txt` mit `--hash` in jeder Zeile |

Ein veröffentlichtes Paket bringt sein Lockfile selten mit, deshalb nimmt das Formular eines neben der Paketangabe entgegen.

Kein Paket darf eigene Installationsskripte ausführen: npm installiert mit `--ignore-scripts`, und Python installiert nur Wheels und baut nie aus einer Quelldistribution.

Pakete kommen aus den eigenen Registries Ihrer Organisation, wenn Sie welche haben. Ein Administrator trägt Ihre npm-Registries, PyPI-Indizes und Git-Hosts einmal unter **Admin → Paket-Registries** ein, und jede Node- und Python-Quelle der Organisation installiert über sie. Die öffentliche npm-Registry und das öffentliche PyPI brauchen keinen Eintrag. Siehe [Paket-Registries](package-registries.md).

Eine Installation wird anhand des Inhalts-Hashes des Codes zwischengespeichert, sodass derselbe Code nur einmal installiert wird.

## Die Sandbox {: #the-sandbox }

Ihr Code läuft in einem eigenen Worker-Prozess, in einer Sandbox. Er kann seinen eigenen installierten Code lesen und sonst nichts auf der Festplatte. Er erreicht nur die Hosts auf der Egress-Allowlist des Deployments, über einen Proxy, den Provisa betreibt, und nie Provisas eigene Ports. Ein Host, der die Sandbox nicht bereitstellen kann, startet den Worker nicht und nennt, was fehlt, statt den Code ohne Einschränkung auszuführen. Unter Windows werden Code-Quellen abgelehnt.

## Eine Tabelle lesen {: #reading-a-table }

Die Tabelle einer Code-Quelle wird gelandet, nie an Ort und Stelle gelesen. Provisa ruft die Funktion mit den Argumenten des Lesezugriffs auf, prüft jede Zeile gegen die Spalten und hält die Zeilen unter diesen Argumenten für die Cache-TTL der Tabelle vor. Sichtbarkeit, Masken und Zeilenregeln gelten dann für die gelandeten Zeilen, wie für jede Tabelle.

Argumente übergeben Sie wie jeden Tabellenparameter: als `_nf_`-Bedingungen in SQL und als Feldargumente in GraphQL.

## Ein Beispiel {: #an-example }

Dieses Python-Modul bewertet, wie wahrscheinlich es ist, dass ein Konto abwandert:

```python
from dataclasses import dataclass
from datetime import datetime


@dataclass
class RiskRow:
    account_id: str
    score: float
    computed_at: datetime
    reasons: list[str]


def churn_risk(account_id: str, as_of: datetime | None = None) -> list[RiskRow]:
    """Churn risk of one account.

    @table
    """
    ...
```

Registrieren Sie es als `python`-Quelle aus seinem PyPI-Paket, Git-Commit oder Upload. Die Discovery bietet `churn_risk` dann als Tabelle mit vier Spalten an (`account_id`, `score`, `computed_at`, `reasons`), mit dem Pflichtparameter `account_id` und dem optionalen `as_of`. Sobald ein Steward die Tabelle registriert hat, lesen Sie sie in SQL:

```sql
SELECT score, reasons
FROM churn_risk
WHERE _nf_account_id = 'A-1042'
```

Dieselbe Funktion sieht in TypeScript so aus:

```typescript
export interface RiskRow {
  account_id: string;
  score: number;
  computed_at: Date;
  reasons: string[];
}

/** Churn risk of one account.
 * @table
 */
export async function churnRisk(accountId: string, asOf?: Date): Promise<RiskRow[]> {
  // ...
}
```
