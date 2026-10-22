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

Der Zeilentyp liefert die Spalten. Text, Zahlen, Booleans, Datumswerte und Zeitstempel werden auf ihre Spaltentypen abgebildet, Listen bleiben Listen, und verschachtelte Objekte werden zu benannten Typen. In TypeScript ist ein Zeilentyp ein Interface, ein Typalias oder eine Klasse. In Python ist er eine Dataclass, ein `TypedDict`, ein `NamedTuple`, ein Pydantic-Modell oder eine beliebige Klasse mit annotierten Feldern. Die Zeilen eines Pydantic-Modells werden so gesendet, wie `model_dump(by_alias=True)` sie liefert, sodass ein Feld mit Alias zu einer Spalte wird, die nach ihrem Alias benannt ist. Die eigene Dokumentation eines Feldes wird zur Beschreibung seiner Spalte, wenn Sie die Tabelle registrieren: ein Doc-Kommentar an einer TypeScript-Property oder ein Pydantic-`Field(description=...)`. Ist der Typ einer Zeile lose (`any`, `unknown` oder ein `Record` in TypeScript, `Any` oder ein `dict` in Python), ruft Provisa die Funktion einmal ohne Argumente auf und schlägt Spalten aus den zurückgegebenen Zeilen vor. Hat die Funktion einen Pflichtparameter, lässt sie sich so nicht aufrufen, und Sie typisieren die Spalten selbst, wenn Sie die Tabelle registrieren.

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

Ein Lockfile ist optional, aber ohne eines können sich zwei Installationen desselben Codes unterscheiden, sodass ein Test, der heute besteht, morgen an einer neuen Version einer Abhängigkeit scheitern kann. Für eine jedes Mal identische Installation muss der Code ein Lockfile mitbringen, oder Sie erstellen eines und geben es hier an. Ein gebautes Image ist bereits festgelegt. Sobald Sie das Image der Quelle bauen ([Image bauen](#running-on-a-remote-host)), stecken seine Abhängigkeiten darin, und wenn dieses Image Ihre Tests und Ihr CI/CD durchläuft, ist das getestete Image dasjenige, das läuft. Ein Lockfile zählt für den Dev-Modus und für jeden neuen Build des Images. Code, der sein eigenes Lockfile mitbringt, wird daraus installiert: ein Git-Repository oder Upload mit einem Lockfile, oder ein npm-Paket, dessen Autor `npm-shrinkwrap.json` veröffentlicht hat (npm veröffentlicht `package-lock.json` nie). Code ohne Lockfile wird mit frei schwankenden Abhängigkeiten installiert: ein Git-Repository oder Upload ohne Lockfile, die meisten npm-Pakete und Python-Pakete aus PyPI, deren Wheels keinen Platz dafür haben. Geben Sie neben solchem Code ein Lockfile an, damit jede Installation identisch ist. Mancher Code braucht keines, weil die Art seiner Verteilung bereits jede Abhängigkeit festlegt.

Kein Paket darf eigene Installationsskripte ausführen: npm installiert mit `--ignore-scripts`, und Python installiert nur Wheels und baut nie aus einer Quelldistribution.

Pakete kommen aus den eigenen Registries Ihrer Organisation, wenn Sie welche haben. Ein Administrator trägt Ihre npm-Registries, PyPI-Indizes und Git-Hosts einmal unter **Admin → Paket-Registries** ein, und jede Node- und Python-Quelle der Organisation installiert über sie. Die öffentliche npm-Registry und das öffentliche PyPI brauchen keinen Eintrag. Siehe [Paket-Registries](package-registries.md).

Eine Installation wird anhand des Inhalts-Hashes des Codes zwischengespeichert, sodass derselbe Code nur einmal installiert wird.

## Die Sandbox {: #the-sandbox }

Ihr Code läuft in einem eigenen Worker-Prozess, in einer Sandbox. Er kann seinen eigenen installierten Code lesen und sonst nichts auf der Festplatte. Er erreicht nur die Hosts auf der Egress-Allowlist des Deployments, über einen Proxy, den Provisa betreibt, und nie Provisas eigene Ports. Ein Host, der die Sandbox nicht bereitstellen kann, startet den Worker nicht und nennt, was fehlt, statt den Code ohne Einschränkung auszuführen. Unter Windows werden Code-Quellen abgelehnt.

## Secrets {: #secrets }

Code, der eine kostenpflichtige API aufruft, braucht deren Schlüssel, und der Schlüssel gehört in den Vault Ihrer Organisation, nicht in den Code. Wählen Sie unter **Secrets** im Formular der Quelle die Secrets der Organisation, die der Code lesen darf. In einer Konfigurationsdatei listen Sie sie im Mapping der Quelle auf:

```yaml
mapping:
  secrets: [openrouter]
```

Ein Name allein ist das Secret der Organisation mit diesem Namen. Um stattdessen eine Umgebungsvariable des Deployments zu lesen, geben Sie die Referenz an: `secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`. Die Liste ist eine Referenz auf den Vault, nie eine Kopie eines Werts, sodass eine Konfiguration, die ein noch von niemandem hinterlegtes Secret nennt, trotzdem lädt.

Ihr Code erhält eine Umgebungsvariable, `PROVISA_SERVICES`, und liest ein Secret bei Bedarf aus `$PROVISA_SERVICES/secrets/<name>`. In Python:

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

In TypeScript:

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

Die Anfrage geht über denselben Egress-Proxy wie der übrige Datenverkehr Ihres Codes, und der Proxy beantwortet sie selbst. Nichts sonst von Provisa ist auf diesem Weg erreichbar. Die Adresse trägt ein Token, das nur der Worker dieser Quelle hält; es liest nur die Namen, die die Quelle auflistet, und es funktioniert nicht mehr, sobald der Worker stoppt. Jeder Lesezugriff löst das Secret in diesem Moment im Vault auf, sodass ein rotiertes Secret beim nächsten Lesen gilt.

Ein Name, den die Quelle nicht auflistet, wird abgelehnt, ebenso ein Secret, das sich nicht auflösen lässt. Die Ablehnung nennt das Secret, nie seinen Wert. Jeder Lesezugriff wird mit Quelle, Name und Zeit im Audit-Trail der Organisation festgehalten, und kein Wert wird je in ein Log geschrieben. Der Host der API muss trotzdem auf der Egress-Allowlist stehen.

## Ein Modell aufrufen {: #calling-a-model }

Ihr Code kann ein Sprachmodell über Provisa aufrufen, statt einen Anbieterschlüssel zu halten. `$PROVISA_SERVICES/llm/v1` ist ein OpenAI-kompatibler Endpunkt, sodass jedes OpenAI-SDK funktioniert, wenn Sie seine Basis-URL dorthin setzen. Die Adresse ist der Berechtigungsnachweis, sodass der Schlüssel, den das SDK verlangt, beliebiger Text sein kann. In Python:

```python
import os
from openai import OpenAI

client = OpenAI(base_url=f"{os.environ['PROVISA_SERVICES']}/llm/v1", api_key="unused")
reply = client.chat.completions.create(
    model="provisa:sql_generation",
    messages=[{"role": "user", "content": "Summarise this account's last quarter."}],
)
```

In TypeScript:

```typescript
import OpenAI from "openai";

const client = new OpenAI({ baseURL: `${process.env.PROVISA_SERVICES}/llm/v1`, apiKey: "unused" });
const reply = await client.chat.completions.create({
  model: "provisa:sql_generation",
  messages: [{ role: "user", content: "Summarise this account's last quarter." }],
});
```

Benennen Sie ein Modell über seine Rolle, als `provisa:<role>`: Sie erhalten das Modell, das Ihre Organisation dieser Rolle unter **Admin → AI Models** zuweist, und es folgt dieser Wahl, wenn ein Administrator sie ändert. Sie können ein Modell auch direkt nennen, wenn es einer Rolle zugewiesen ist. Für Embeddings nimmt `$PROVISA_SERVICES/llm/v1/embeddings` jedes der Vektormodelle Ihrer Organisation. Jedes andere Modell wird mit Namen abgelehnt. Der Aufruf geht über die eigenen Anbieter, Endpunkte und Schlüssel Ihrer Organisation hinaus, und was er sendet, durchläuft zuerst die Schwärzungsregeln von Provisa. Streaming wird nicht angeboten: Senden Sie `stream: false`, den Standardwert.

Jeder Aufruf wird für die Quelle festgehalten, mit dem Modell, seinen Tokens und seinen Kosten zum Eintrag des Modells in **Polly model prices**, und in den Audit-Trail der Organisation geschrieben. Um zu begrenzen, was eine Quelle ausgibt, setzen Sie **LLM monthly cap** in ihrem Formular, in USD. In einer Konfigurationsdatei gehört es ins Mapping:

```yaml
mapping:
  llm: {cap_usd: 20}
```

Eine Quelle mit Obergrenze kann nur Modelle aufrufen, die dort einen Preis haben. Sobald ihre Ausgaben im Monat (UTC) die Obergrenze erreichen, werden ihre Aufrufe abgelehnt, bis der Monat wechselt. Aufrufe, die beim Erreichen der Obergrenze schon laufen, werden noch abgeschlossen, sodass der Monat leicht über der Obergrenze enden kann.

## Die Dienste von Provisa {: #provisas-services }

Ihr Code kann ein Modul importieren, das Provisa überall bereitstellt, wo er läuft: `provisa_services` in Python und `@provisa/services` in Node.js, Bun und Deno. Nichts muss installiert werden, und es liegt in keiner Paket-Registry. Außerhalb von Provisa existiert das Modul nicht, daher mocken Tests Ihrer Funktionen es.

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
| `settings.get(name, default)` | Eine der Einstellungen der Quelle, aus **Settings** in ihrem Formular, die ein Steward ändern kann, ohne den Code zu ändern. Fehlt sie und gibt es keinen Standardwert, wird ein Fehler ausgelöst. |
| `cache.get(key, default)`, `cache.set(key, value, ttl)`, `cache.delete(key)` | Der eigene Cache der Quelle, den alle ihre Worker teilen. Ein Wert ist alles, was JSON halten kann, bis 1 MB, gehalten für `ttl` Sekunden oder bis er ersetzt wird. |
| `vectors.upsert(items, model)`, `vectors.search(query, k, model)`, `vectors.embed(texts, model)`, `vectors.delete(ids)` | Die eigene Vektorsammlung der Quelle, für ihre eigenen Referenzdaten. Ein Element hat eine `id` und einen `text` oder einen `vector`; ein Text wird von einem der Vektormodelle Ihrer Organisation eingebettet. Sie kann Ihre verwalteten Tabellen nicht durchsuchen: Deren Daten erreichen Code nur als Parameter. |
| `http.get(url)`, `http.post(url, ...)` | Ein Abruf, den Provisa für den Code ausführt, über den ausgehenden Proxy und die Zertifizierungsstellen des Deployments. Der Host muss auf der Egress-Allowlist stehen, und jeder Abruf wird im Audit-Trail festgehalten. |
| `llm.chat(model, messages)`, `llm.embeddings(model, input)`, `llm.base_url()` (`llm.baseUrl()` in JavaScript) | Der LLM-Dienst von Provisa. Siehe [Ein Modell aufrufen](#calling-a-model). |
| `dq.dataset(name)`, `dq.product(id)` | Die Datenqualität eines Datensatzes (`domain.table` oder seine Tabellen-ID) oder eines Datenprodukts, wie die Meta-Domäne sie der Rolle der Anfrage zeigt, die Ihr Code bedient. Für einen Datensatz: die Prüftabellen, die ihn prüfen, jede mit Beschreibung, Vertrag, zugeordnetem Datenprodukt und letztem Lauf (Ergebnis, Zeitpunkt und fehlgeschlagene Prüfungen), sowie seine Profile mit dem letzten Lauf jedes Profils. Für ein Datenprodukt: seine Zusagen, Erwartungen, Partnerprüfungen und verwaisten Prüfungen. |
| `log(message, level, fields)`, `metric(name, value, kind, attributes)` | Eine Logzeile und ein Metrikpunkt namens `code_source.<name>`, angehängt an die Anfrage, die Ihr Code bedient. |

Ein Aufruf, den der Dienst ablehnt, löst einen `ServiceError` aus, der den Dienst nennt und den Grund angibt.

Die Dienste antworten Code, der auf dem Provisa-Server läuft. Auf einem entfernten Host sind sie noch nicht erreichbar, und jeder Aufruf löst einen Fehler aus.

Jeder Aufruf Ihres Codes wird als Teil der Anfrage verfolgt, die ihn ausgelöst hat, mit der Funktion, ihrer Art, der Zahl ihrer Argumente (nie deren Werte), den zurückgegebenen Zeilen und Batches, ihrer Dauer und der CPU des Workers. Jeder Dienstaufruf wird ebenso verfolgt. Bei normaler Trace-Detailstufe stehen diese Angaben auf dem Span der Anfrage; bei Debug-Detailstufe ist jeder Aufruf ein eigener Span. Wenn Ihr Code OpenTelemetry nutzt, schließen sich seine Spans demselben Trace an.

## Auf einem entfernten Host ausführen {: #running-on-a-remote-host }

Eine Quelle läuft standardmäßig auf dem Provisa-Server; das ist der Dev-Modus. Braucht ihr Code eine Maschine, die der Server nicht ist, etwa einen GPU-Rechner oder einen Host in einem anderen Netzwerk, bauen Sie ihr Image und führen es dort aus. Die Quelle läuft dann im Built-Modus, und ihre Tabellen, Berechtigungen und Leser bleiben gleich.

**Das Image bauen.** Speichern Sie die Quelle zuerst im Dev-Modus, damit ihr Code gelesen und gepinnt wird. Wählen Sie unter **Remote host** in ihrem Formular die Architektur (Linux x86-64 oder ARM64) und wohin das Image geht: in eine der Container-Registries Ihrer Organisation, eingetragen unter **Admin → Container registries**, oder als Download. Provisa setzt das Image selbst zusammen. Es enthält den Code mit seinem gepinnten Hash, seine Abhängigkeiten für diese Plattform aus dem Lockfile des Codes über Ihre Paket-Registries installiert, die gepinnte Laufzeit und das Remote-Host-Programm, auf einem minimalen Basis-Image. Beim Bauen läuft nichts im Image, und kein Docker-Daemon ist beteiligt. Der Tag des Images trägt den Code-Hash und die Version von Provisa, und seine Labels nennen die Laufzeit, den Code-Hash und die Exporte.

**Ausführen.** Das Formular zeigt einen Compose-Dienst für das Image. Ein heruntergeladenes Image wird zuerst mit `docker load -i <file>` geladen. Der Container lauscht intern auf Port 7443 und hält seine Identität auf dem Volume. Den Host-Port wählen Sie mit `PROVISA_HOST_PORT`; ohne ihn startet der Dienst nicht:

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

Beim ersten Start schreibt der Container eine Zeile, die mit `pairing code:` beginnt, in sein Log.

**Verbinden.** Geben Sie zurück im Formular den Host ein, unter dem Provisa den Container erreicht, den veröffentlichten Port und den Pairing-Code. In einem gemeinsamen Docker-Netzwerk oder in Kubernetes geben Sie den Container- oder Service-Namen und 7443 ein, ohne etwas zu veröffentlichen. Provisa koppelt sich über Mutual TLS mit dem Container. Der Code wird einmal verwendet, und danach akzeptiert jede Seite nur noch die andere. Provisa prüft dann, dass der Container den Code der Quelle ausführt und ihre Exporte anbietet, und schaltet die Quelle in den Built-Modus. Jeder Provisa-Prozess hält seine eigene Verbindung zum Host und wählt sich neu ein, wenn der Host nicht mehr antwortet. Solange der Host nicht antwortet, schlagen Lesezugriffe auf die Tabellen der Quelle fehl und nennen den Host.

**Wenn sich der Code ändert.** Ein Container, der mit anderem Code zurückkommt, wird abgelehnt, mit dem Hash, auf den die Quelle gepinnt ist, und dem, den der Host meldet, bis ein Steward den Code prüft und ihn im Formular neu pinnt. Um den Code selbst zu ändern, schalten Sie die Quelle zurück auf Dev, speichern den neuen Code und bauen erneut.

Im Container ist seine Netzwerkrichtlinie die Egress-Grenze des Codes; die Egress-Allowlist des Servers gilt für den Dev-Modus.

## Eine Tabelle lesen {: #reading-a-table }

Die Tabelle einer Code-Quelle wird gelandet, nie an Ort und Stelle gelesen. Provisa ruft die Funktion mit den Argumenten des Lesezugriffs auf, prüft jede Zeile gegen die Spalten und hält die Zeilen unter diesen Argumenten für die Cache-TTL der Tabelle vor. Sichtbarkeit, Masken und Zeilenregeln gelten dann für die gelandeten Zeilen, wie für jede Tabelle.

Argumente übergeben Sie wie jeden Tabellenparameter: als `_nf_`-Bedingungen in SQL und als Feldargumente in GraphQL.

## Gebündelte Schlüssel: der Parameter `loader` {: #batched-keys }

Ein Join kann eine Code-Tabelle erreichen: Die Zeilen einer anderen Tabelle liefern ihre Parameter, etwa Kunden, die mit ihrem Churn-Score verknüpft werden. Standardmäßig ruft Provisa die Funktion einmal für jede eindeutige Kombination von Argumenten auf, die die verknüpften Zeilen liefern, und sendet alle diese Aufrufe in einer Anfrage an den Worker.

Eine Funktion mit einem Parameter namens `loader` wird stattdessen einmal pro Batch aufgerufen, nach dem Muster, das DataLoader aus GraphQL bekannt gemacht hat. `loader` ist ein Hash: ein Dict (in TypeScript ein Objekt) von einem stabilen Hash jedes Schlüssels auf den Schlüssel selbst, ein JSON-Objekt mit den Werten der verknüpften Zeilen für die übrigen Parameter der Funktion. Provisa entfernt doppelte Schlüssel, sodass jede eindeutige Kombination von Werten einmal vorkommt. Ein Schlüssel ist dünn besetzt: Ein Feld ohne Wert fehlt, statt als null gesendet zu werden, sodass der eigene Standardwert der Funktion gilt, wie bei einem einzelnen Aufruf.

Die Funktion gibt dieselbe Form zurück: einen Hash von jedem erhaltenen Schlüssel auf die Zeilen dieses Schlüssels, gewöhnliche Zeilen der Tabelle. Provisa gibt die Zeilen jedes Schlüssels an jede verknüpfte Zeile, die diesen Schlüssel teilte, und ein Schlüssel kann auf keine oder viele Zeilen zeigen. Die Zuordnung über den Hash statt über die Werte der Zeilen hält einen dünn besetzten Schlüssel zugeordnet: Die Funktion füllt seine fehlenden Felder mit Standardwerten, also unterscheiden sich die Werte, die sie sieht, von denen der verknüpften Zeile. Provisa berechnet den Hash; die Funktion gibt nur die erhaltenen Schlüssel zurück. Die Funktion deklariert weiterhin nur ihren Zeilentyp, eine Liste von Zeilen, und die Zeilen jedes Schlüssels haben diesen Typ. Ihre Parameter neben `loader` brauchen Standardwerte, weil ein Loader-Aufruf nur `loader` übergibt.

Eine Funktion kann ihre Batchgröße begrenzen, wie `maxBatchSize` bei DataLoader, indem sie `loader N` an einer von drei Stellen deklariert: eine Zeile `@loader 100` neben `@table` im Docstring oder JSDoc, ein Kommentar `/* @loader 100 */` direkt vor dem Parameter `loader` in TypeScript oder ein `Annotated`-String am Parameter in Python, `loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`. Provisa teilt die Schlüssel dann in Batches dieser Größe, ruft die Funktion einmal pro Batch auf und führt die Antworten über den Hash zusammen. Ohne Größe enthält ein Batch alle Schlüssel. Die Erkennung lehnt mit Namen ab: eine Größe an einer Funktion ohne Parameter `loader`, eine Größe, die keine positive ganze Zahl ist, und zwei Größen an einer Funktion.

Verwenden Sie `loader`, wenn ein Aufruf über viele Schlüssel besser ist als viele Aufrufe über je einen:

- Ein Modell über einen ganzen Batch in einem vektorisierten Durchgang bewerten.
- Anfragen an eine API oder ein Sprachmodell bündeln, das viele Eingaben auf einmal annimmt.
- Einmal pro Batch vorbereiten, etwa ein Modell laden oder eine Verbindung öffnen, statt einmal pro Schlüssel.

Diese Python-Funktion bewertet jeden Kunden eines Batches in einem Durchgang:

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

Mit einer Kundentabelle verknüpft, wird sie mit einem `loader` wie `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}` aufgerufen. Der zweite Schlüssel trägt nur `customer_id`, also gelten für Plan und Region die Standardwerte. Die Funktion gibt `{"9f2c...": [...], "41ab...": [...], ...}` zurück, und Provisa gibt jede Liste von Zeilen an jede Kundenzeile, die diesen Schlüssel teilte.

Dieselbe Funktion in TypeScript:

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
