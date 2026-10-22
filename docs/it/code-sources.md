# Resolver di codice

Logica di business in Python o Node.

Se hai già scritto un server GraphQL, conosci questo schema. Un resolver di codice è una funzione che risponde per un tipo di oggetto: in GraphQL, una funzione marcata `@table` *è* il resolver del tipo della sua tabella. Provisa la chiama con gli argomenti della query e governa ciò che restituisce.

Cosa cambia rispetto a un resolver scritto a mano:

- **Governato.** Visibilità, maschere e regole di riga si applicano alle righe che la tua funzione restituisce, come per qualsiasi tabella.
- **A lotti.** Una funzione che accetta un parametro `loader` riceve tutte le chiavi di un join in una sola chiamata, lo schema DataLoader.
- **Delegato.** Un filtro su una colonna arriva alla tua funzione come parametro quando lo chiedi.
- **Servito ovunque.** La stessa funzione risponde a GraphQL, SQL (su HTTP, pgwire e Arrow Flight), Cypher (su HTTP e Bolt), gRPC, REST, JSON:API e MCP.

I resolver di codice sono uno dei tre modi di scrivere la logica di business in Provisa. Vedi [La logica di business in Provisa](business-logic.md).

## Un primo resolver {: #an-example }

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

Per servirlo:

1. Aggiungi un'origine `python` e puntala al codice: un pacchetto PyPI, un commit Git, un caricamento o una cartella (vedi [Da dove viene il codice](#where-the-code-comes-from)).
2. Salvala. Provisa installa il codice, lo legge e offre `churn_risk` come tabella con le colonne `score` e `reasons` e un parametro obbligatorio, `account_id`.
3. Uno steward registra la tabella in un dominio, qui `accounts`, e la concede come qualsiasi altra.

Poi leggila. In GraphQL il parametro è un argomento di campo:

```graphql
{
  churnRisk(account_id: "A-1042") {
    score
    reasons
  }
}
```

In SQL è una condizione `_nf_`:

```sql
SELECT score, reasons
FROM accounts.churn_risk
WHERE _nf_account_id = 'A-1042'
```

Node funziona allo stesso modo, con tag JSDoc e tipi TypeScript: vedi [Node](#node).

## Costruire {: #build }

### Cosa diventa una funzione {: #what-a-function-becomes }

Provisa legge le dichiarazioni del tuo codice stesso: docstring e annotazioni di tipo in Python, tag JSDoc e tipi TypeScript in Node.

| Tag | Diventa |
|---|---|
| `@table` | Una tabella. La funzione restituisce una lista o un iterabile, e ogni elemento è una riga. |
| `@command` | Un comando, eseguito su richiesta. |
| `@stream` | Una tabella live. La funzione è un generatore, e ogni riga viene inviata appena prodotta. |

- Una funzione senza tag viene ignorata.
- Un tag che Provisa non conosce viene rifiutato, come un tipo che non sa mappare. Il rifiuto elenca tutti i problemi in una volta, così li correggi in un solo passaggio.

Il tipo di riga dà le colonne:

- Testo, numeri, booleani, date e timestamp corrispondono ai loro tipi di colonna. Le liste restano liste, e gli oggetti annidati diventano tipi con nome.
- In Python un tipo di riga è una dataclass, un `TypedDict`, un `NamedTuple`, un modello Pydantic o qualsiasi classe con campi annotati. Le righe di un modello Pydantic vengono inviate come le dà `model_dump(by_alias=True)`, quindi un campo con un alias diventa una colonna con il nome dell'alias.
- In TypeScript un tipo di riga è un'interfaccia, un alias di tipo o una classe.
- La documentazione propria di un campo diventa la descrizione della sua colonna quando registri la tabella: un `Field(description=...)` di Pydantic, o un commento di documentazione su una proprietà TypeScript.
- Un tipo di riga generico (`Any` o un `dict` in Python, `any`, `unknown` o un `Record` in TypeScript) non ha colonne da leggere. Provisa chiama la funzione una volta senza argomenti e propone colonne dalle righe restituite. Se la funzione ha un parametro obbligatorio non può essere chiamata così, e indichi tu i tipi delle colonne quando registri la tabella.

I parametri della funzione diventano i parametri della tabella:

- Un parametro senza valore predefinito è obbligatorio. Una lettura che lo omette viene rifiutata prima che il tuo codice venga eseguito.
- Un parametro con valore predefinito è facoltativo. Omettilo e si applica il tuo valore predefinito.
- Uno steward può comunque rendere obbligatorio uno facoltativo.

### Delega dei filtri {: #pushdown }

Un filtro su una delle colonne restituite può arrivare alla tua funzione come argomento, così che recuperi solo le righe richieste. Dichiara quale parametro lo riceve in uno di tre modi:

- **Per nome.** Un parametro chiamato esattamente come una colonna restituita riceve un'uguaglianza su quella colonna. Uno chiamato `<op>_<column>` riceve quell'operatore, come `gte_score` o `in_region`.
- **Per tag.** Una riga `@<op>_<column> <parameter>` (o `@<column> <parameter>`) nella docstring o nel JSDoc, o un `/* @<op>_<column> */` in linea prima di un parametro TypeScript.
- **Per `Annotated`.** In Python, la stringa `"<op>_<column>"` (o `"<column>"`) sull'annotazione di tipo del parametro.

Gli operatori:

| `op` | Riceve |
|---|---|
| `ne`, `gt`, `gte`, `lt`, `lte` | Un valore del tipo della colonna |
| `in` | Una lista del tipo della colonna |
| `like` | Testo, su una colonna di testo |

- Un intervallo su una colonna riempie entrambi i suoi parametri: `score BETWEEN 0.5 AND 0.9` dà `gte_score` e `lte_score`.
- I nomi delle colonne vengono confrontati esattamente come li hai dichiarati nel tipo di riga. Un nome che corrisponde solo quando Provisa rinomina la colonna per un lettore non è una corrispondenza.
- Una dichiarazione che i tipi della tua funzione non possono accettare viene rifiutata.
- Il filtro viene comunque applicato alle righe che la tua funzione restituisce, quindi una funzione che ignora o allenta un valore ricevuto non può restituire righe sbagliate.

### Chiavi a lotti: il parametro `loader` {: #batched-keys }

Un join può raggiungere una tabella di codice, con le righe di un'altra tabella che ne forniscono i parametri: ad esempio clienti uniti al loro punteggio di abbandono. Per impostazione predefinita Provisa chiama la funzione una volta per ogni insieme distinto di argomenti dato dalle righe del join, e invia tutte queste chiamate al worker in una sola richiesta.

Una funzione che accetta un parametro chiamato `loader` viene invece chiamata una volta per lotto, lo schema reso noto dal DataLoader di GraphQL.

- **Cosa riceve.** `loader` è un hash: un dict (un oggetto in TypeScript) da un hash stabile di ogni chiave alla chiave stessa. Una chiave è un oggetto JSON con i valori delle righe del join per gli altri parametri della funzione.
- **Senza duplicati.** Ogni combinazione distinta di valori compare una volta.
- **Sparsa.** Un campo senza valore è omesso dalla chiave, così si applica il valore predefinito della funzione, come in una chiamata singola.
- **Cosa restituisce.** Un hash da ogni chiave ricevuta alle righe di quella chiave: righe normali della tabella, nessuna o molte. Provisa dà le righe di ogni chiave a ogni riga del join che condivideva la chiave.
- **Associato per hash.** La funzione riempie con valori predefiniti i campi mancanti di una chiave sparsa, quindi i valori che vede differiscono da quelli della riga del join. Provisa calcola l'hash, e la funzione restituisce soltanto le chiavi ricevute.
- **Tipizzata come sempre.** La funzione dichiara comunque solo il suo tipo di riga, una lista di righe.
- **Servono valori predefiniti.** I suoi parametri accanto a `loader` hanno bisogno di valori predefiniti, perché una chiamata con loader passa solo `loader`.
- **Dimensione del lotto.** Limitala, come `maxBatchSize` di DataLoader, con `loader N` in uno di tre punti: una riga `@loader 100` accanto a `@table`, un commento `/* @loader 100 */` prima del parametro `loader` in TypeScript, o una stringa `Annotated` in Python, `loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`. Provisa divide allora le chiavi in lotti di quella dimensione e unisce le risposte per hash. Senza dimensione, un lotto contiene tutte le chiavi.
- **Rifiutato.** Una dimensione su una funzione senza parametro `loader`, una dimensione che non è un intero positivo, e due dimensioni su una funzione.

Usa `loader` quando una chiamata su molte chiavi rende più di molte chiamate su una:

- Valutare un modello su un intero lotto in un passaggio vettorizzato.
- Raggruppare richieste a un'API o a un modello linguistico che accetta molti input insieme.
- Preparare qualcosa una volta per lotto, come caricare un modello o aprire una connessione.

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

Unita a una tabella di clienti, viene chiamata con un `loader` come `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}`. La seconda chiave porta solo `customer_id`, quindi piano e regione prendono i valori predefiniti. La funzione restituisce `{"9f2c...": [...], "41ab...": [...], ...}`.

### Da dove viene il codice {: #where-the-code-comes-from }

- **Un pacchetto.** Un pacchetto npm per Node o un pacchetto PyPI per Python, in una versione esatta.
- **Un repository Git.** Un commit completo di 40 caratteri, con una sottodirectory se il codice non è in cima.
- **Un caricamento.** Un `.zip`, `.tgz` o `.tar.gz` del codice.
- **Una cartella sulla tua macchina.** **Scegli cartella** comprime la cartella nel tuo browser, lasciando fuori `node_modules`, `.venv`, `__pycache__` e `.git`, e la carica come archivio dell'origine. In un browser che può conservare la cartella (Chrome, Edge), **Salva** la comprime e la invia di nuovo; un altro browser ti chiede di nuovo la cartella.
- **Una directory sul server.** Su un'installazione con una sola organizzazione, una directory sotto quelle che l'operatore consente. Viene letta dov'è, quindi una modifica viene letta dalla query successiva. Un server con molte organizzazioni la rifiuta.

Ogni origine tranne la directory del server è fissata, quindi l'origine esegue esattamente ciò che è stato salvato.

- **Modulo di ingresso.** Quando il codice Python ha più di un modulo di primo livello, indica quello da cui entrare.
- **Apri in VS Code.** Una directory del server si apre nel tuo VS Code installato; un'origine Git si apre al suo commit su vscode.dev. Un pacchetto o un caricamento non ha questo link.

### Installare le dipendenze {: #installing-dependencies }

Il codice con dipendenze ha bisogno di un lockfile, e ogni installazione lo segue esattamente. Senza, un pacchetto che dichiara dipendenze viene rifiutato, e il rifiuto ti chiede di aggiungerne uno.

| Runtime | Lockfile |
|---|---|
| Node.js | `package-lock.json` o `npm-shrinkwrap.json` |
| Bun | `bun.lock` o `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock` (con il suo `pyproject.toml`), `pylock.toml`, o un `requirements.txt` con `--hash` su ogni riga |

- **Incluso o fornito.** Il codice che porta il proprio lockfile installa da quello: un repository Git, un caricamento o una cartella che ne contiene uno, o un pacchetto npm il cui autore ha pubblicato `npm-shrinkwrap.json` (npm non pubblica mai `package-lock.json`). La maggior parte dei pacchetti npm, e i pacchetti Python da PyPI, non ne portano, quindi fornisci un lockfile accanto.
- **Perché conta.** Senza, due installazioni dello stesso codice possono differire, e un test che passa oggi può fallire domani per una nuova versione di una dipendenza.
- **Un'immagine costruita è già fissa.** Appena [costruisci l'immagine dell'origine](#running-on-a-remote-host), le sue dipendenze sono dentro. Un lockfile conta per la modalità dev e per ogni nuova build.
- **Nessuno script di installazione.** npm installa con `--ignore-scripts`, e Python installa solo wheel, senza mai costruire da una distribuzione sorgente.
- **I tuoi registri.** I pacchetti arrivano dai registri npm, dagli indici PyPI e dagli host Git della tua organizzazione, inseriti una volta in **Admin → Package registries**. Il registro npm pubblico e PyPI non richiedono una voce. Vedi [Package Registries](package-registries.md).
- **In cache.** Un'installazione viene messa in cache per hash del contenuto del codice, quindi lo stesso codice si installa una volta.

### Node {: #node }

- Il codice Node gira su Node.js, Bun o Deno, a tua scelta.
- I tag vanno nel JSDoc, e i tipi vengono da TypeScript.

Il primo resolver in TypeScript:

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

Il `churnScore` a lotti in TypeScript:

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

## Distribuzione {: #deployment }

### Modalità dev e worker {: #workers }

Per impostazione predefinita un'origine gira sul server Provisa; questa è la modalità dev.

- Il tuo codice gira in un processo worker tutto suo, uno per origine e versione del suo codice in ogni processo Provisa, avviato alla prima chiamata.
- Un worker che va in crash fa fallire le chiamate in corso, nominando l'origine e la funzione. La chiamata successiva ne avvia uno nuovo.
- Una chiamata viene annullata quando scade il termine della sua richiesta: al worker viene detto di fermarla, e la richiesta in attesa viene liberata subito.

### Leggere una tabella {: #reading-a-table }

Come una lettura raggiunge la tua funzione dipende da cos'altro legge:

- **Una lettura di una sola tabella di codice** va dritta alla tua funzione. Non viene depositato nulla. Provisa la chiama con gli argomenti della lettura e i suoi filtri delegati, e applica il resto dell'istruzione alle righe che restituisce.
- **Una lettura che unisce altre tabelle** spetta al motore di federazione. La tabella di un'origine in modalità dev viene depositata per essa: le righe sono conservate con quegli argomenti per il TTL di cache della tabella.
- **La tabella di un'origine costruita** in un tale join viene letta dal vivo dal suo host remoto, che il motore collega come catalogo Postgres. Viene invece depositata quando la sua origine è replicata o con soglia minima, quando la tabella è conservata per sempre, quando dichiara un TTL di deposito sopra 0, o su un motore che non può collegare un host.

In ogni caso visibilità, maschere e regole di riga si applicano alle righe, come per qualsiasi tabella. Passi gli argomenti come qualsiasi parametro di tabella: condizioni `_nf_` in SQL, argomenti di campo in GraphQL.

### Eseguire su un host remoto {: #running-on-a-remote-host }

Quando il tuo codice ha bisogno di una macchina diversa dal server, come una macchina con GPU o un host dentro un'altra rete, costruisci la sua immagine ed eseguila lì. L'origine passa allora alla modalità costruita, e le sue tabelle, concessioni e lettori restano gli stessi.

**Costruire l'immagine.**

- Salva prima l'origine in modalità dev, così il suo codice viene letto e fissato.
- In **Remote host** nel suo modulo, scegli l'architettura (Linux x86-64 o ARM64) e dove va l'immagine: uno dei registri di container della tua organizzazione (**Admin → Container registries**), o un download.
- Provisa assembla l'immagine da solo, su una base minima, senza demone Docker: il codice al suo hash fissato, le sue dipendenze installate per quella piattaforma dal suo lockfile tramite i tuoi registri di pacchetti, il runtime fissato e il programma di host remoto. Nulla nell'immagine viene eseguito mentre viene costruita.
- Il tag dell'immagine porta l'hash del codice e la versione di Provisa, e le sue label nominano il runtime, l'hash del codice e le esportazioni.

**Eseguirla.** Il modulo mostra un servizio compose per l'immagine. Carica prima un'immagine scaricata con `docker load -i <file>`. Il container è in ascolto su 7443 e conserva la sua identità sul suo volume. Scegli la porta dell'host con `PROVISA_HOST_PORT`; il servizio non parte senza.

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

Al primo avvio il container stampa nel suo log una riga che inizia con `pairing code:`.

**Collegarla.**

- Nel modulo, inserisci l'host a cui Provisa raggiunge il container, la porta che hai pubblicato e il codice di abbinamento. Su una rete Docker condivisa o in Kubernetes, inserisci il nome del container o del Service e 7443, senza pubblicare nulla.
- Provisa si abbina al container tramite TLS reciproco. Il codice si usa una volta; da lì in poi ciascun lato accetta solo l'altro.
- Provisa verifica che il container esegua il codice dell'origine e ne offra le esportazioni, poi passa l'origine alla modalità costruita.
- Ogni processo Provisa mantiene la propria connessione all'host e richiama se l'host smette di rispondere. Finché non risponde, le letture delle tabelle dell'origine falliscono e nominano l'host.

**Se il codice cambia.**

- Un container che torna eseguendo altro codice viene rifiutato finché uno steward non esamina il codice e lo rifissa dal modulo.
- Per cambiare il codice stesso, riporta l'origine in modalità dev, salva il nuovo codice e costruisci di nuovo.

### I servizi di Provisa {: #provisas-services }

Il tuo codice può importare un modulo che Provisa fornisce sempre sul server: `provisa_services` in Python, `@provisa/services` in Node.js, Bun e Deno. Non c'è nulla da installare, e non è in nessun registro di pacchetti. Fuori da Provisa non esiste, quindi i tuoi test lo simulano.

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

| Chiamata | Cosa fa |
|---|---|
| `secrets.get(name)` | Uno dei segreti dichiarati dell'origine, letto ora dal vault. Vedi [Segreti](#secrets). |
| `settings.get(name, default)`, `settings.all()` | Le impostazioni dell'origine, da **Settings** nel suo modulo, che uno steward può cambiare senza cambiare il codice. Una mancante senza valore predefinito solleva un errore. |
| `cache.get(key, default)`, `cache.set(key, value, ttl)`, `cache.delete(key)` | La cache propria dell'origine, condivisa da tutti i suoi worker. Un valore è qualsiasi cosa JSON possa contenere, fino a 1 MB, conservato per `ttl` secondi o finché non viene sostituito. |
| `vectors.upsert(items, model)`, `vectors.search(query, k, model)`, `vectors.embed(texts, model)`, `vectors.delete(ids)` | La collezione di vettori propria dell'origine, per i suoi dati di riferimento. Un elemento ha un `id` e un `text` o un `vector`; un testo viene vettorizzato da uno dei modelli vettoriali della tua organizzazione. Non può cercare nelle tue tabelle governate, i cui dati raggiungono il codice solo come parametri. |
| `http.get(url)`, `http.post(url, ...)` | Una richiesta che Provisa fa per il codice, tramite il proxy in uscita e le autorità di certificazione della distribuzione. L'host deve essere nella lista di uscita consentita, e ogni richiesta viene registrata nella traccia di audit. |
| `llm.chat(model, messages)`, `llm.embeddings(model, input)`, `llm.base_url()` (`llm.baseUrl()` in JavaScript) | Il servizio LLM di Provisa. Vedi [Chiamare un modello](#calling-a-model). |
| `log(message, level, fields)`, `metric(name, value, kind, attributes)` | Una riga di log, e un punto di metrica chiamato `code_source.<name>`, collegati alla richiesta che il tuo codice serve. |

- Una chiamata che il servizio rifiuta solleva un `ServiceError` che nomina il servizio e dice perché.
- I servizi rispondono al codice che gira sul server Provisa. Su un host remoto ogni chiamata solleva un errore.

#### Segreti {: #secrets }

La chiave di un'API a pagamento va nel vault della tua organizzazione, non nel codice.

- In **Secrets** nel modulo dell'origine, scegli i segreti dell'organizzazione che il codice può leggere. In un file di configurazione, elencali nel mapping dell'origine:

    ```yaml
    mapping:
      secrets: [openrouter]
    ```

- Un nome da solo è il segreto dell'organizzazione con quel nome. Per leggere invece una variabile d'ambiente della distribuzione, dai il riferimento: `secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`.
- L'elenco rimanda al vault e non copia mai un valore, quindi una configurazione che nomina un segreto che nessuno ha ancora salvato si carica comunque.

Il tuo codice legge un segreto con `secrets.get(name)`, o direttamente da `$PROVISA_SERVICES/secrets/<name>`:

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

- `PROVISA_SERVICES` è l'unica variabile d'ambiente che il tuo codice riceve. La richiesta passa per lo stesso proxy di uscita del resto del suo traffico, e il proxy risponde da sé; nient'altro di Provisa è raggiungibile così.
- L'indirizzo porta un token che solo il worker di questa origine possiede. Legge solo i nomi che l'origine elenca, e smette di funzionare quando il worker si ferma.
- Ogni lettura risolve il segreto nel vault in quel momento, quindi un segreto ruotato vale dalla lettura successiva.
- Un nome che l'origine non elenca viene rifiutato, come un segreto che non si può risolvere. Il rifiuto nomina il segreto, mai il suo valore.
- Ogni lettura viene registrata nella traccia di audit dell'organizzazione con l'origine, il nome e l'ora. Nessun valore viene mai scritto in un log.
- L'host dell'API deve comunque essere nella lista di uscita consentita.

#### Chiamare un modello {: #calling-a-model }

Il tuo codice può chiamare un modello linguistico tramite Provisa invece di tenere una chiave del fornitore. `$PROVISA_SERVICES/llm/v1` è un endpoint compatibile con OpenAI, quindi qualsiasi SDK OpenAI funziona con l'URL di base puntato lì. L'indirizzo è la credenziale, quindi la chiave che l'SDK chiede può essere qualsiasi testo.

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

- **Per ruolo.** Nomina un modello come `provisa:<role>` e ottieni quello che la tua organizzazione assegna a quel ruolo in **Admin → AI Models**; segue quella scelta quando un amministratore la cambia. Puoi anche nominare un modello direttamente quando gli è assegnato un ruolo.
- **Embeddings.** `$PROVISA_SERVICES/llm/v1/embeddings` accetta uno qualsiasi dei modelli vettoriali della tua organizzazione.
- Qualsiasi altro modello viene rifiutato.
- La chiamata esce tramite i fornitori, gli endpoint e le chiavi della tua organizzazione, e ciò che invia passa prima per le regole di oscuramento di Provisa.
- Lo streaming non è offerto. Invia `stream: false`, che è il valore predefinito.

## Protezioni {: #safeguards }

### Il sandbox {: #the-sandbox }

- Il tuo codice gira nel proprio processo worker, in un sandbox. Può leggere il proprio codice installato e nient'altro sul disco.
- Raggiunge solo gli host della lista di uscita consentita della distribuzione, tramite un proxy che Provisa esegue, e mai le porte di Provisa.
- Un host che non può fornire il sandbox rifiuta di avviare il worker e nomina ciò che manca; il codice non gira mai senza confinamento. Su Windows, le origini di codice vengono rifiutate.
- Su un host remoto, la policy di rete del container è il limite di uscita del codice. La lista di uscita consentita del server vale per la modalità dev.

### Rifiuti {: #refusals }

Ognuno di questi casi viene rifiutato, prima che il tuo codice venga eseguito quando possibile:

- Un tag sconosciuto o un tipo che Provisa non sa mappare, tutti i problemi di una funzione in una volta.
- Una lettura che omette un parametro obbligatorio.
- Una dichiarazione di delega o di `loader` che i tipi della funzione non possono accettare.
- Un pacchetto con dipendenze e senza lockfile.
- Una directory del server fuori da quelle consentite, o su un server con molte organizzazioni.
- Un host remoto che esegue codice diverso da quello a cui l'origine è fissata.
- Un segreto che l'origine non elenca, un modello non consentito, una chiamata di servizio a cui il servizio non risponde.

### Misurazione e limiti {: #metering }

- Ogni chiamata a un modello viene registrata per l'origine, con il modello, i suoi token e il suo costo secondo la voce del modello in **Polly model prices**, e scritta nella traccia di audit dell'organizzazione.
- Per limitare quanto spende un'origine, imposta **LLM monthly cap** nel suo modulo, in USD. In un file di configurazione va nel mapping:

    ```yaml
    mapping:
      llm: {cap_usd: 20}
    ```

- Un'origine con un limite può chiamare solo modelli che lì hanno un prezzo.
- Quando la sua spesa del mese (UTC) raggiunge il limite, le sue chiamate vengono rifiutate fino al cambio del mese. Le chiamate già in corso quando si raggiunge il limite terminano comunque, quindi il mese può chiudersi di poco sopra il limite.

### Governance sui risultati {: #governance }

- Le righe della tua funzione vengono verificate rispetto alle colonne della tabella.
- Visibilità, maschere e regole di riga si applicano poi per ogni lettore, come per qualsiasi tabella.
- Il tuo codice vede i dati di una tabella governata solo come i parametri che una lettura gli passa.

### Tracciamento {: #tracing }

- Ogni chiamata al tuo codice viene tracciata come parte della richiesta che l'ha avviata: la funzione, il suo tipo, quanti argomenti ha ricevuto (mai i loro valori), le righe e i lotti restituiti, la sua durata e la CPU del worker.
- Ogni chiamata di servizio viene tracciata allo stesso modo.
- Al livello di dettaglio normale queste informazioni stanno sullo span della richiesta; al livello di debug ogni chiamata è uno span a sé.
- Se il tuo codice usa OpenTelemetry, i suoi span si uniscono alla stessa traccia.
