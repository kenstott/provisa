# Code-Resolver

Geschäftslogik in Python oder Node.

Wenn Sie schon einen GraphQL-Server geschrieben haben, kennen Sie dieses Muster. Ein Code-Resolver ist eine Funktion, die für einen Objekttyp antwortet: Über GraphQL *ist* eine mit `@table` markierte Funktion der Resolver für den Typ ihrer Tabelle. Provisa ruft sie mit den Argumenten der Abfrage auf und regelt, was zurückkommt.

Was sich von einem Resolver unterscheidet, den Sie selbst schreiben:

- **Gesteuert.** Sichtbarkeit, Masken und Zeilenregeln gelten für die Zeilen, die Ihre Funktion zurückgibt, wie für jede Tabelle.
- **Gebündelt.** Eine Funktion mit einem Parameter `loader` erhält alle Schlüssel eines Joins in einem Aufruf, das DataLoader-Muster.
- **Heruntergereicht.** Ein Filter auf eine Spalte erreicht Ihre Funktion als Parameter, wenn Sie danach fragen.
- **Überall bedient.** Dieselbe Funktion beantwortet GraphQL, SQL (über HTTP, pgwire und Arrow Flight), Cypher (über HTTP und Bolt), gRPC, REST, JSON:API und MCP.

Code-Resolver sind eine von drei Arten, Geschäftslogik in Provisa zu schreiben. Siehe [Geschäftslogik in Provisa](business-logic.md).

## Ein erster Resolver {: #an-example }

```python
from dataclasses import dataclass


@dataclass
class Risk:
    score: float
    reasons: list[str]


def churn_risk(account_id: str) -> list[Risk]:
    """Churn risk of one account.

    @table
    """
    return [Risk(score=0.82, reasons=["no logins in 30 days"])]
```

So stellen Sie ihn bereit:

1. Legen Sie eine `python`-Quelle an und verweisen Sie auf den Code: ein PyPI-Paket, einen Git-Commit, einen Upload oder einen Ordner (siehe [Woher der Code kommt](#where-the-code-comes-from)).
2. Speichern Sie sie. Provisa installiert den Code, liest ihn und bietet `churn_risk` als Tabelle mit den Spalten `score` und `reasons` und einem Pflichtparameter `account_id` an.
3. Ein Steward registriert die Tabelle in einer Domäne, hier `accounts`, und gewährt sie wie jede andere.

Dann lesen Sie sie. In GraphQL ist der Parameter ein Feldargument:

```graphql
{
  churnRisk(account_id: "A-1042") {
    score
    reasons
  }
}
```

In SQL ist er eine `_nf_`-Bedingung:

```sql
SELECT score, reasons
FROM accounts.churn_risk
WHERE _nf_account_id = 'A-1042'
```

Node funktioniert genauso, mit JSDoc-Tags und TypeScript-Typen: siehe [Node](#node).

## Erstellen {: #build }

### Was aus einer Funktion wird {: #what-a-function-becomes }

Provisa liest die eigenen Deklarationen Ihres Codes: Docstrings und Type Hints in Python, JSDoc-Tags und TypeScript-Typen in Node.

| Tag | Wird zu |
|---|---|
| `@table` | Einer Tabelle. Die Funktion gibt eine Liste oder ein Iterable zurück, und jedes Element ist eine Zeile. |
| `@command` | Einem Befehl, der auf Anfrage ausgeführt wird. |
| `@stream` | Einer Live-Tabelle. Die Funktion ist ein Generator, und jede Zeile wird gesendet, sobald sie geliefert wird. |

- Eine Funktion ohne Tag wird ignoriert.
- Ein Tag, das Provisa nicht kennt, wird abgelehnt, ebenso ein Typ, den es nicht abbilden kann. Die Ablehnung listet alle Probleme auf einmal, sodass Sie sie in einem Durchgang beheben.

Der Zeilentyp liefert die Spalten:

- Text, Zahlen, Booleans, Datumswerte und Zeitstempel werden ihren Spaltentypen zugeordnet. Listen bleiben Listen, und verschachtelte Objekte werden zu benannten Typen.
- In Python ist ein Zeilentyp eine Dataclass, ein `TypedDict`, ein `NamedTuple`, ein Pydantic-Modell oder jede Klasse mit annotierten Feldern. Die Zeilen eines Pydantic-Modells werden so gesendet, wie `model_dump(by_alias=True)` sie liefert, sodass ein Feld mit Alias zu einer Spalte mit dem Namen des Alias wird.
- In TypeScript ist ein Zeilentyp ein Interface, ein Typalias oder eine Klasse.
- Die eigene Dokumentation eines Felds wird zur Beschreibung seiner Spalte, wenn Sie die Tabelle registrieren: ein Pydantic-`Field(description=...)` oder ein Doc-Kommentar an einer TypeScript-Eigenschaft.
- Ein loser Zeilentyp (`Any` oder ein `dict` in Python, `any`, `unknown` oder ein `Record` in TypeScript) hat keine Spalten zum Lesen. Provisa ruft die Funktion einmal ohne Argumente auf und schlägt Spalten aus den zurückgegebenen Zeilen vor. Hat die Funktion einen Pflichtparameter, lässt sie sich so nicht aufrufen, und Sie geben die Spaltentypen beim Registrieren der Tabelle selbst an.

Die Parameter der Funktion werden zu den Parametern der Tabelle:

- Ein Parameter ohne Standardwert ist Pflicht. Ein Lesen, das ihn weglässt, wird abgelehnt, bevor Ihr Code läuft.
- Ein Parameter mit Standardwert ist optional. Lassen Sie ihn weg, gilt Ihr Standardwert.
- Ein Steward kann einen optionalen trotzdem zur Pflicht machen.

### Pushdown {: #pushdown }

Ein Filter auf eine der zurückgegebenen Spalten kann Ihre Funktion als Argument erreichen, sodass sie nur die angefragten Zeilen holt. Welcher Parameter ihn erhält, deklarieren Sie auf eine von drei Arten:

- **Per Name.** Ein Parameter, der genau wie eine Rückgabespalte heißt, erhält eine Gleichheit auf dieser Spalte. Einer namens `<op>_<column>` erhält diesen Operator, etwa `gte_score` oder `in_region`.
- **Per Tag.** Eine Zeile `@<op>_<column> <parameter>` (oder `@<column> <parameter>`) im Docstring oder JSDoc, oder ein Inline-`/* @<op>_<column> */` vor einem TypeScript-Parameter.
- **Per `Annotated`.** In Python der String `"<op>_<column>"` (oder `"<column>"`) am Type Hint des Parameters.

Die Operatoren:

| `op` | Erhält |
|---|---|
| `ne`, `gt`, `gte`, `lt`, `lte` | Einen Wert vom Typ der Spalte |
| `in` | Eine Liste vom Typ der Spalte |
| `like` | Text, auf einer Textspalte |

- Ein Bereich auf einer Spalte füllt beide ihrer Parameter: `score BETWEEN 0.5 AND 0.9` ergibt `gte_score` und `lte_score`.
- Spaltennamen werden genau so abgeglichen, wie Sie sie im Zeilentyp deklariert haben. Ein Name, der erst passt, wenn Provisa die Spalte für einen Leser umbenennt, ist kein Treffer.
- Eine Deklaration, die die Typen Ihrer Funktion nicht annehmen können, wird abgelehnt.
- Der Filter wird trotzdem auf die Zeilen angewendet, die Ihre Funktion zurückgibt, sodass eine Funktion, die einen übergebenen Wert ignoriert oder lockert, keine falschen Zeilen liefern kann.

### Gebündelte Schlüssel: der Parameter `loader` {: #batched-keys }

Ein Join kann eine Code-Tabelle erreichen, wobei die Zeilen einer anderen Tabelle ihre Parameter liefern: etwa Kunden, verknüpft mit ihrem Churn-Score. Standardmäßig ruft Provisa die Funktion einmal für jeden eindeutigen Satz von Argumenten auf, den die verknüpfenden Zeilen ergeben, und schickt alle diese Aufrufe in einer Anfrage an den Worker.

Eine Funktion mit einem Parameter namens `loader` wird stattdessen einmal pro Batch aufgerufen, das Muster, das GraphQLs DataLoader bekannt gemacht hat.

- **Was sie erhält.** `loader` ist ein Hash: ein dict (in TypeScript ein Objekt) von einem stabilen Hash jedes Schlüssels auf den Schlüssel selbst. Ein Schlüssel ist ein JSON-Objekt mit den Werten der verknüpfenden Zeilen für die übrigen Parameter der Funktion.
- **Dedupliziert.** Jede eindeutige Kombination von Werten erscheint einmal.
- **Dünn besetzt.** Ein Feld ohne Wert fehlt im Schlüssel, sodass der eigene Standardwert der Funktion gilt, wie bei einem einzelnen Aufruf.
- **Was sie zurückgibt.** Einen Hash von jedem erhaltenen Schlüssel auf die Zeilen dieses Schlüssels: gewöhnliche Zeilen der Tabelle, keine oder viele. Provisa gibt die Zeilen jedes Schlüssels an jede verknüpfende Zeile, die den Schlüssel teilt.
- **Per Hash zugeordnet.** Die Funktion füllt die fehlenden Felder eines dünn besetzten Schlüssels mit Standardwerten, sodass die Werte, die sie sieht, von denen der verknüpfenden Zeile abweichen. Provisa berechnet den Hash, und die Funktion gibt nur die erhaltenen Schlüssel zurück.
- **Wie gewohnt typisiert.** Die Funktion deklariert weiterhin nur ihren Zeilentyp, eine Liste von Zeilen.
- **Standardwerte nötig.** Ihre Parameter neben `loader` brauchen Standardwerte, weil ein Loader-Aufruf nur `loader` übergibt.
- **Batchgröße.** Begrenzen Sie sie, wie DataLoaders `maxBatchSize`, mit `loader N` an einer von drei Stellen: eine Zeile `@loader 100` neben `@table`, ein Kommentar `/* @loader 100 */` vor dem Parameter `loader` in TypeScript, oder ein `Annotated`-String in Python, `loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`. Provisa teilt die Schlüssel dann in Batches dieser Größe und führt die Antworten per Hash zusammen. Ohne Größe enthält ein Batch alle Schlüssel.
- **Abgelehnt.** Eine Größe an einer Funktion ohne Parameter `loader`, eine Größe, die keine positive ganze Zahl ist, und zwei Größen an einer Funktion.

Verwenden Sie `loader`, wenn ein Aufruf über viele Schlüssel besser ist als viele Aufrufe über einen:

- Ein Modell über einen ganzen Batch in einem vektorisierten Durchgang bewerten.
- Anfragen an eine API oder ein Sprachmodell bündeln, das viele Eingaben auf einmal annimmt.
- Einmal pro Batch einrichten, etwa ein Modell laden oder eine Verbindung öffnen.

```python
from dataclasses import dataclass
from typing import TypedDict


class CustomerKey(TypedDict, total=False):
    customer_id: str
    plan: str
    region: str


@dataclass
class Score:
    customer_id: str
    score: float


def churn_score(
    customer_id: str | None = None,
    plan: str = "basic",
    region: str = "US",
    loader: dict[str, CustomerKey] | None = None,
) -> list[Score]:
    """Churn score of each customer in the batch.

    @table
    """
    keys = loader or {"": {"customer_id": customer_id, "plan": plan, "region": region}}
    rows = {h: {"plan": "basic", "region": "US", **key} for h, key in keys.items()}
    scores = MODEL.predict(features(list(rows.values())))  # one vectorized call for the batch
    out = {h: [Score(r["customer_id"], s)] for (h, r), s in zip(rows.items(), scores)}
    return out if loader else out[""]  # a loader call returns the hash of key to rows
```

Mit einer Kundentabelle verknüpft, wird sie mit einem `loader` wie `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}` aufgerufen. Der zweite Schlüssel trägt nur `customer_id`, sodass Plan und Region die Standardwerte erhalten. Die Funktion gibt `{"9f2c...": [...], "41ab...": [...], ...}` zurück.

### Woher der Code kommt {: #where-the-code-comes-from }

- **Ein Paket.** Ein npm-Paket für Node oder ein PyPI-Paket für Python, in einer exakten Version.
- **Ein Git-Repository.** Ein vollständiger 40-stelliger Commit, mit einem Unterverzeichnis, falls der Code nicht oben liegt.
- **Ein Upload.** Ein `.zip`, `.tgz` oder `.tar.gz` des Codes.
- **Ein Ordner auf Ihrem Rechner.** **Ordner wählen** zippt den Ordner in Ihrem Browser, ohne `node_modules`, `.venv`, `__pycache__` und `.git`, und lädt ihn als Archiv der Quelle hoch. In einem Browser, der den Ordner behalten kann (Chrome, Edge), zippt und sendet **Speichern** ihn erneut; ein anderer Browser fragt Sie erneut nach dem Ordner.
- **Ein Verzeichnis auf dem Server.** Bei einer Installation mit einer Organisation ein Verzeichnis unter denen, die der Betreiber erlaubt. Es wird an Ort und Stelle gelesen, sodass eine Änderung von der nächsten Abfrage gelesen wird. Ein Server mit vielen Organisationen lehnt es ab.

Jede Herkunft außer dem Serververzeichnis ist fixiert, sodass die Quelle genau das ausführt, was gespeichert wurde.

- **Einstiegsmodul.** Hat Python-Code mehr als ein Modul auf oberster Ebene, nennen Sie das, über das eingestiegen wird.
- **In VS Code öffnen.** Ein Serververzeichnis öffnet sich in Ihrem installierten VS Code; eine Git-Herkunft öffnet sich auf ihrem Commit in vscode.dev. Ein Paket oder ein Upload hat keinen solchen Link.

### Abhängigkeiten installieren {: #installing-dependencies }

Code mit Abhängigkeiten braucht eine Lockfile, und jede Installation folgt ihr exakt. Ohne eine wird ein Paket, das Abhängigkeiten deklariert, abgelehnt, und die Ablehnung fordert Sie auf, eine hinzuzufügen.

| Laufzeit | Lockfile |
|---|---|
| Node.js | `package-lock.json` oder `npm-shrinkwrap.json` |
| Bun | `bun.lock` oder `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock` (mit seiner `pyproject.toml`), `pylock.toml` oder eine `requirements.txt` mit `--hash` in jeder Zeile |

- **Mitgebracht oder angegeben.** Code, der seine eigene Lockfile mitbringt, installiert daraus: ein Git-Repository, Upload oder Ordner mit einer, oder ein npm-Paket, dessen Autor `npm-shrinkwrap.json` veröffentlicht hat (npm veröffentlicht nie `package-lock.json`). Die meisten npm-Pakete und Python-Pakete von PyPI bringen keine mit, also geben Sie eine daneben an.
- **Warum es wichtig ist.** Ohne eine können zwei Installationen desselben Codes abweichen, und ein Test, der heute besteht, kann morgen an einer neuen Abhängigkeitsversion scheitern.
- **Ein gebautes Image ist bereits fest.** Sobald Sie [das Image der Quelle bauen](#running-on-a-remote-host), sind ihre Abhängigkeiten darin. Eine Lockfile zählt für den Dev-Modus und für jeden neuen Build.
- **Keine Installationsskripte.** npm installiert mit `--ignore-scripts`, und Python installiert nur Wheels und baut nie aus einer Quelldistribution.
- **Ihre Registries.** Pakete kommen aus den eigenen npm-Registries, PyPI-Indizes und Git-Hosts Ihrer Organisation, einmal unter **Admin → Package registries** eingetragen. Die öffentliche npm-Registry und PyPI brauchen keinen Eintrag. Siehe [Package Registries](package-registries.md).
- **Zwischengespeichert.** Eine Installation wird nach dem Inhalts-Hash des Codes zwischengespeichert, sodass derselbe Code einmal installiert wird.

### Node {: #node }

- Node-Code läuft auf Node.js, Bun oder Deno, wie Sie wählen.
- Tags stehen in JSDoc, und Typen kommen aus TypeScript.

Der erste Resolver in TypeScript:

```typescript
export interface Risk {
  score: number;
  reasons: string[];
}

/** Churn risk of one account.
 * @table
 */
export async function churnRisk(account_id: string): Promise<Risk[]> {
  return [{ score: 0.82, reasons: ["no logins in 30 days"] }];
}
```

Das gebündelte `churnScore` in TypeScript:

```typescript
export interface CustomerKey {
  customer_id: string;
  plan?: string;
  region?: string;
}

export interface Score {
  customer_id: string;
  score: number;
}

/** Churn score of each customer in the batch.
 * @table
 */
export async function churnScore(
  customer_id?: string,
  plan = "basic",
  region = "US",
  loader?: Record<string, CustomerKey>,
): Promise<Score[]> {
  const keys = loader ?? { "": { customer_id, plan, region } };
  const rows = Object.entries(keys).map(([h, key]) => [h, { plan: "basic", region: "US", ...key }] as const);
  const scores = await model.predict(rows.map(([, row]) => row)); // one call for the whole batch
  const out = Object.fromEntries(
    rows.map(([h, row], i) => [h, [{ customer_id: row.customer_id, score: scores[i] }]]),
  );
  return loader ? out : out[""]; // a loader call returns the hash of key to rows
}
```

## Bereitstellung {: #deployment }

### Dev-Modus und Worker {: #workers }

Eine Quelle läuft standardmäßig auf dem Provisa-Server; das ist der Dev-Modus.

- Ihr Code läuft in einem eigenen Worker-Prozess, einem pro Quelle und Version ihres Codes in jedem Provisa-Prozess, gestartet bei seinem ersten Aufruf.
- Ein Worker, der abstürzt, lässt die laufenden Aufrufe fehlschlagen und nennt Quelle und Funktion. Der nächste Aufruf startet einen neuen.
- Ein Aufruf wird abgebrochen, wenn die Frist seiner Anfrage abläuft: Dem Worker wird gesagt, ihn zu stoppen, und die wartende Anfrage wird sofort freigegeben.

### Eine Tabelle lesen {: #reading-a-table }

Wie ein Lesen Ihre Funktion erreicht, hängt davon ab, was es sonst noch liest:

- **Ein Lesen einer einzelnen Code-Tabelle** geht direkt an Ihre Funktion. Nichts wird abgelegt. Provisa ruft sie mit den Argumenten des Lesens und seinen heruntergereichten Filtern auf und wendet den Rest der Anweisung auf die zurückgegebenen Zeilen an.
- **Ein Lesen, das andere Tabellen verknüpft,** gehört der Föderations-Engine. Die Tabelle einer Quelle im Dev-Modus wird dafür abgelegt: Die Zeilen werden unter diesen Argumenten für die Cache-TTL der Tabelle aufbewahrt.
- **Die Tabelle einer gebauten Quelle** in einem solchen Join wird live von ihrem Remote-Host gelesen, den die Engine als Postgres-Katalog anbindet. Sie wird stattdessen abgelegt, wenn ihre Quelle repliziert oder gefloort ist, wenn die Tabelle dauerhaft behalten wird, wenn sie eine Ablage-TTL über 0 deklariert, oder auf einer Engine, die keinen Host anbinden kann.

In jedem Fall gelten Sichtbarkeit, Masken und Zeilenregeln für die Zeilen, wie für jede Tabelle. Argumente übergeben Sie wie jeden Tabellenparameter: `_nf_`-Bedingungen in SQL, Feldargumente in GraphQL.

### Auf einem Remote-Host ausführen {: #running-on-a-remote-host }

Braucht Ihr Code eine Maschine, die der Server nicht ist, etwa einen GPU-Rechner oder einen Host in einem anderen Netzwerk, bauen Sie sein Image und führen es dort aus. Die Quelle läuft dann im Build-Modus, und ihre Tabellen, Berechtigungen und Leser bleiben gleich.

**Das Image bauen.**

- Speichern Sie die Quelle zuerst im Dev-Modus, damit ihr Code gelesen und fixiert wird.
- Wählen Sie unter **Remote host** in ihrem Formular die Architektur (Linux x86-64 oder ARM64) und wohin das Image geht: eine der Container-Registries Ihrer Organisation (**Admin → Container registries**) oder ein Download.
- Provisa setzt das Image selbst zusammen, auf einer minimalen Basis, ohne Docker-Daemon: den Code an seinem fixierten Hash, seine Abhängigkeiten für diese Plattform aus seiner Lockfile über Ihre Paket-Registries installiert, die fixierte Laufzeit und das Remote-Host-Programm. Nichts im Image läuft, während es gebaut wird.
- Der Tag des Images trägt den Code-Hash und die Version von Provisa, und seine Labels nennen die Laufzeit, den Code-Hash und die Exporte.

**Ausführen.** Das Formular zeigt einen Compose-Service für das Image. Laden Sie ein heruntergeladenes Image zuerst mit `docker load -i <file>`. Der Container lauscht auf 7443 und bewahrt seine Identität auf seinem Volume. Den Host-Port wählen Sie mit `PROVISA_HOST_PORT`; ohne ihn startet der Service nicht.

```yaml
services:
  provisa-code-rates:
    image: ghcr.io/example/provisa-code-rates:3f9a1c2e7b40-0.1.0
    restart: unless-stopped
    ports:
      - "${PROVISA_HOST_PORT:?set PROVISA_HOST_PORT to a free port on this host}:7443"
    volumes:
      - provisa-code-rates-state:/var/lib/provisa
volumes:
  provisa-code-rates-state: {}
```

Bei seinem ersten Start gibt der Container in seinem Log eine Zeile aus, die mit `pairing code:` beginnt.

**Verbinden.**

- Geben Sie im Formular den Host ein, unter dem Provisa den Container erreicht, den veröffentlichten Port und den Pairing-Code. In einem gemeinsamen Docker-Netzwerk oder in Kubernetes geben Sie den Container- oder Service-Namen und 7443 ein, ohne etwas zu veröffentlichen.
- Provisa koppelt sich über Mutual TLS mit dem Container. Der Code wird einmal verwendet; danach akzeptiert jede Seite nur die andere.
- Provisa prüft, dass der Container den Code der Quelle ausführt und ihre Exporte anbietet, und schaltet die Quelle dann in den Build-Modus.
- Jeder Provisa-Prozess hält seine eigene Verbindung zum Host und wählt erneut, wenn der Host nicht mehr antwortet. Solange er nicht antwortet, schlagen Lesezugriffe auf die Tabellen der Quelle fehl und nennen den Host.

**Wenn sich der Code ändert.**

- Ein Container, der mit anderem Code zurückkommt, wird abgelehnt, bis ein Steward den Code prüft und ihn im Formular neu fixiert.
- Um den Code selbst zu ändern, schalten Sie die Quelle zurück in den Dev-Modus, speichern den neuen Code und bauen erneut.

### Die Dienste von Provisa {: #provisas-services }

Ihr Code kann ein Modul importieren, das Provisa auf dem Server immer bereitstellt: `provisa_services` in Python, `@provisa/services` in Node.js, Bun und Deno. Es muss nichts installiert werden, und es liegt in keiner Paket-Registry. Außerhalb von Provisa existiert es nicht, also mocken Ihre Tests es.

```python
import provisa_services as ps

region = ps.settings.get("region")
rates = ps.cache.get("rates") or fetch_rates()
ps.cache.set("rates", rates, ttl=3600)
```

```typescript
import { settings, cache } from "@provisa/services";

const region = await settings.get("region");
const rates = (await cache.get("rates")) ?? (await fetchRates());
await cache.set("rates", rates, 3600);
```

| Aufruf | Was er tut |
|---|---|
| `secrets.get(name)` | Eines der deklarierten Secrets der Quelle, jetzt aus dem Vault gelesen. Siehe [Secrets](#secrets). |
| `settings.get(name, default)`, `settings.all()` | Die Einstellungen der Quelle, aus **Settings** in ihrem Formular, die ein Steward ändern kann, ohne den Code zu ändern. Eine fehlende ohne Standardwert löst einen Fehler aus. |
| `cache.get(key, default)`, `cache.set(key, value, ttl)`, `cache.delete(key)` | Der eigene Cache der Quelle, geteilt von jedem ihrer Worker. Ein Wert ist alles, was JSON fassen kann, bis 1 MB, aufbewahrt für `ttl` Sekunden oder bis er ersetzt wird. |
| `vectors.upsert(items, model)`, `vectors.search(query, k, model)`, `vectors.embed(texts, model)`, `vectors.delete(ids)` | Die eigene Vektorsammlung der Quelle, für ihre eigenen Referenzdaten. Ein Element hat eine `id` und einen `text` oder einen `vector`; ein Text wird von einem der Vektormodelle Ihrer Organisation eingebettet. Sie kann Ihre gesteuerten Tabellen nicht durchsuchen, deren Daten Code nur als Parameter erreichen. |
| `http.get(url)`, `http.post(url, ...)` | Ein Abruf, den Provisa für den Code macht, über den ausgehenden Proxy und die Zertifizierungsstellen der Bereitstellung. Der Host muss auf der Egress-Allowlist stehen, und jeder Abruf wird im Audit-Trail festgehalten. |
| `llm.chat(model, messages)`, `llm.embeddings(model, input)`, `llm.base_url()` (`llm.baseUrl()` in JavaScript) | Der LLM-Dienst von Provisa. Siehe [Ein Modell aufrufen](#calling-a-model). |
| `log(message, level, fields)`, `metric(name, value, kind, attributes)` | Eine Logzeile und ein Metrikpunkt namens `code_source.<name>`, angehängt an die Anfrage, die Ihr Code bedient. |

- Ein Aufruf, den der Dienst ablehnt, löst einen `ServiceError` aus, der den Dienst nennt und den Grund angibt.
- Die Dienste antworten Code, der auf dem Provisa-Server läuft. Auf einem Remote-Host löst jeder Aufruf einen Fehler aus.

#### Secrets {: #secrets }

Ein Schlüssel für eine kostenpflichtige API gehört in den Vault Ihrer Organisation, nicht in den Code.

- Wählen Sie unter **Secrets** im Formular der Quelle die Organisations-Secrets, die der Code lesen darf. In einer Konfigurationsdatei listen Sie sie im Mapping der Quelle:

    ```yaml
    mapping:
      secrets: [openrouter]
    ```

- Ein Name für sich ist das gleichnamige Organisations-Secret. Um stattdessen eine Umgebungsvariable der Bereitstellung zu lesen, geben Sie die Referenz an: `secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`.
- Die Liste verweist auf den Vault und kopiert nie einen Wert, sodass eine Konfiguration, die ein noch von niemandem gespeichertes Secret nennt, trotzdem lädt.

Ihr Code liest ein Secret mit `secrets.get(name)` oder direkt aus `$PROVISA_SERVICES/secrets/<name>`:

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

- `PROVISA_SERVICES` ist die einzige Umgebungsvariable, die Ihr Code erhält. Die Anfrage läuft über denselben Egress-Proxy wie der übrige Verkehr, und der Proxy beantwortet sie selbst; nichts sonst von Provisa ist so erreichbar.
- Die Adresse trägt ein Token, das nur der Worker dieser Quelle hält. Sie liest nur die Namen, die die Quelle listet, und funktioniert nicht mehr, wenn der Worker stoppt.
- Jedes Lesen löst das Secret in diesem Moment im Vault auf, sodass ein rotiertes Secret beim nächsten Lesen wirkt.
- Ein Name, den die Quelle nicht listet, wird abgelehnt, ebenso ein Secret, das sich nicht auflösen lässt. Die Ablehnung nennt das Secret, nie seinen Wert.
- Jedes Lesen wird im Audit-Trail der Organisation mit Quelle, Name und Zeit festgehalten. Kein Wert wird je in ein Log geschrieben.
- Der Host der API muss trotzdem auf der Egress-Allowlist stehen.

#### Ein Modell aufrufen {: #calling-a-model }

Ihr Code kann ein Sprachmodell über Provisa aufrufen, statt einen Herstellerschlüssel zu halten. `$PROVISA_SERVICES/llm/v1` ist ein OpenAI-kompatibler Endpunkt, sodass jedes OpenAI-SDK funktioniert, wenn seine Basis-URL dorthin zeigt. Die Adresse ist die Berechtigung, sodass der Schlüssel, nach dem das SDK fragt, beliebiger Text sein kann.

```python
import os
from openai import OpenAI

client = OpenAI(base_url=f"{os.environ['PROVISA_SERVICES']}/llm/v1", api_key="unused")
reply = client.chat.completions.create(
    model="provisa:sql_generation",
    messages=[{"role": "user", "content": "Summarise this account's last quarter."}],
)
```

```typescript
import OpenAI from "openai";

const client = new OpenAI({ baseURL: `${process.env.PROVISA_SERVICES}/llm/v1`, apiKey: "unused" });
const reply = await client.chat.completions.create({
  model: "provisa:sql_generation",
  messages: [{ role: "user", content: "Summarise this account's last quarter." }],
});
```

- **Per Rolle.** Benennen Sie ein Modell als `provisa:<role>`, und Sie erhalten das Modell, das Ihre Organisation dieser Rolle unter **Admin → AI Models** zuweist; es folgt dieser Wahl, wenn ein Administrator sie ändert. Sie können ein Modell auch direkt benennen, wenn ihm eine Rolle zugewiesen ist.
- **Embeddings.** `$PROVISA_SERVICES/llm/v1/embeddings` nimmt jedes der Vektormodelle Ihrer Organisation.
- Jedes andere Modell wird abgelehnt.
- Der Aufruf geht über die eigenen Hersteller, Endpunkte und Schlüssel Ihrer Organisation hinaus, und was er sendet, durchläuft zuerst die Schwärzungsregeln von Provisa.
- Streaming wird nicht angeboten. Senden Sie `stream: false`, den Standardwert.

## Schutzmechanismen {: #safeguards }

### Die Sandbox {: #the-sandbox }

- Ihr Code läuft in seinem eigenen Worker-Prozess, in einer Sandbox. Er kann seinen eigenen installierten Code lesen und sonst nichts auf der Festplatte.
- Er erreicht nur die Hosts auf der Egress-Allowlist der Bereitstellung, über einen Proxy, den Provisa betreibt, und nie die eigenen Ports von Provisa.
- Ein Host, der die Sandbox nicht bereitstellen kann, startet den Worker nicht und nennt, was fehlt; Code läuft nie unbeschränkt. Unter Windows werden Code-Quellen abgelehnt.
- Auf einem Remote-Host ist die Netzwerkrichtlinie des Containers die Egress-Grenze des Codes. Die Egress-Allowlist des Servers gilt für den Dev-Modus.

### Ablehnungen {: #refusals }

Jeder dieser Fälle wird abgelehnt, wo möglich bevor Ihr Code läuft:

- Ein unbekanntes Tag oder ein Typ, den Provisa nicht abbilden kann, alle Probleme einer Funktion auf einmal.
- Ein Lesen, das einen Pflichtparameter weglässt.
- Eine Pushdown- oder `loader`-Deklaration, die die Typen der Funktion nicht annehmen können.
- Ein Paket mit Abhängigkeiten und ohne Lockfile.
- Ein Serververzeichnis außerhalb der erlaubten oder auf einem Server mit vielen Organisationen.
- Ein Remote-Host, der anderen Code ausführt, als die Quelle fixiert hat.
- Ein Secret, das die Quelle nicht listet, ein Modell, das nicht erlaubt ist, ein Dienstaufruf, den der Dienst nicht beantwortet.

### Messung und Obergrenzen {: #metering }

- Jeder Modellaufruf wird für die Quelle festgehalten, mit dem Modell, seinen Tokens und seinen Kosten zum Eintrag des Modells in **Polly model prices**, und in den Audit-Trail der Organisation geschrieben.
- Um zu begrenzen, was eine Quelle ausgibt, setzen Sie **LLM monthly cap** in ihrem Formular, in USD. In einer Konfigurationsdatei gehört es ins Mapping:

    ```yaml
    mapping:
      llm: {cap_usd: 20}
    ```

- Eine Quelle mit Obergrenze kann nur Modelle aufrufen, die dort einen Preis haben.
- Sobald ihre Ausgaben für den Monat (UTC) die Obergrenze erreichen, werden ihre Aufrufe abgelehnt, bis der Monat wechselt. Aufrufe, die beim Erreichen der Obergrenze schon laufen, enden noch, sodass der Monat leicht über der Obergrenze enden kann.

### Governance auf Ergebnissen {: #governance }

- Die Zeilen Ihrer Funktion werden gegen die Spalten der Tabelle geprüft.
- Sichtbarkeit, Masken und Zeilenregeln gelten dann für jeden Leser, wie für jede Tabelle.
- Ihr Code sieht die Daten einer gesteuerten Tabelle nur als die Parameter, die ein Lesen ihm gibt.

### Tracing {: #tracing }

- Jeder Aufruf Ihres Codes wird als Teil der auslösenden Anfrage nachverfolgt: die Funktion, ihre Art, wie viele Argumente sie erhielt (nie deren Werte), die zurückgegebenen Zeilen und Batches, ihre Dauer und die CPU des Workers.
- Jeder Dienstaufruf wird genauso nachverfolgt.
- Bei der normalen Trace-Detailstufe stehen diese Angaben am Span der Anfrage; bei Debug-Detail ist jeder Aufruf ein eigener Span.
- Verwendet Ihr Code OpenTelemetry, schließen sich seine Spans demselben Trace an.
