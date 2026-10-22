# Sorgenti Node e Python

A volte i dati che vi servono stanno dietro codice che il vostro team ha già scritto: un modello di scoring, un client per un servizio interno, uno script che sistema un'esportazione. Una sorgente Node o Python vi permette di servire quel codice come tabelle. Marcate le funzioni che volete condividere, indicate a Provisa dove si trova il codice, e ogni funzione marcata diventa una tabella che ogni ruolo legge con le stesse regole di qualsiasi altra tabella.

## Cosa diventa una funzione {: #what-a-function-becomes }

Provisa legge le dichiarazioni del vostro codice: tag JSDoc e tipi TypeScript per Node, docstring e type hint per Python. Contano tre tag.

| Tag | Diventa |
|---|---|
| `@table` | Una tabella. La funzione restituisce una lista o un iterabile, e ogni elemento è una riga |
| `@command` | Un command, eseguito su richiesta |
| `@stream` | Una tabella live. La funzione è un generatore, e ogni riga viene inviata appena prodotta |

Una funzione senza tag viene ignorata. Un tag che Provisa non conosce viene rifiutato, come un tipo che non sa mappare. Il rifiuto nomina la funzione ed elenca tutti i problemi insieme, così li correggete in un solo passaggio.

Il tipo di riga dà le colonne. Testo, numeri, booleani, date e timestamp corrispondono ai loro tipi di colonna, le liste restano liste e gli oggetti annidati diventano tipi con nome. In TypeScript un tipo di riga è un'interfaccia, un alias di tipo o una classe. In Python è una dataclass, un `TypedDict`, un `NamedTuple`, un modello Pydantic o qualsiasi classe con campi annotati. Le righe di un modello Pydantic vengono inviate come le restituisce `model_dump(by_alias=True)`, quindi un campo con alias diventa una colonna con il nome del suo alias. La documentazione di un campo diventa la descrizione della sua colonna quando registrate la tabella: un commento di documentazione su una proprietà TypeScript, o un `Field(description=...)` di Pydantic. Quando il tipo di una riga è generico (`any`, `unknown` o un `Record` in TypeScript, `Any` o un `dict` in Python), Provisa chiama la funzione una volta, senza argomenti, e suggerisce le colonne dalle righe restituite. Se la funzione ha un parametro obbligatorio non può essere chiamata così, e tipizzate voi le colonne quando registrate la tabella.

I parametri della funzione diventano i parametri della tabella. Un parametro senza valore predefinito è obbligatorio, quindi una lettura che lo omette viene rifiutata prima che il codice venga eseguito. Un parametro con valore predefinito è facoltativo; se lo omettete, vale il vostro predefinito. Uno steward può comunque rendere obbligatorio un parametro facoltativo.

## Da dove viene il codice {: #where-the-code-comes-from }

Il codice è fissato, così una sorgente esegue sempre esattamente ciò che è stato registrato:

- **Un pacchetto**: un pacchetto npm per Node o un pacchetto PyPI per Python, a una versione esatta.
- **Un repository Git**: un commit completo di 40 caratteri, con una sottodirectory se il codice non è in cima.
- **Un caricamento**: un `.zip`, `.tgz` o `.tar.gz` del codice.

Il codice Node gira su Node.js, Bun o Deno, a vostra scelta. Il codice Python gira sul Python fornito con Provisa. Quando il codice ha più di un modulo Python di primo livello, indicate in **Modulo di ingresso** quello da cui si entra.

## Installare le dipendenze {: #installing-dependencies }

Il codice con dipendenze ha bisogno di un lockfile, e ogni installazione lo segue alla lettera. Senza lockfile, un pacchetto che dichiara dipendenze viene rifiutato, e il rifiuto vi dice di aggiungerne uno.

| Runtime | Lockfile |
|---|---|
| Node.js | `package-lock.json` o `npm-shrinkwrap.json` |
| Bun | `bun.lock` o `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock` (con il suo `pyproject.toml`), `pylock.toml` o un `requirements.txt` con `--hash` su ogni riga |

Un lockfile è facoltativo, ma senza di esso due installazioni dello stesso codice possono differire, quindi un test che oggi passa può fallire domani per una nuova versione di una dipendenza. Per un'installazione identica ogni volta, il codice deve includere un lockfile, oppure ne create uno e lo fornite qui. Un'immagine costruita è già fissata. Una volta costruita l'immagine della sorgente ([Costruisci immagine](#running-on-a-remote-host)), le sue dipendenze sono al suo interno, e se è quell'immagine a passare per i vostri test e la vostra CI/CD, l'immagine testata è quella che viene eseguita. Un lockfile conta per la modalità dev e per ogni nuova build dell'immagine. Il codice che include il proprio lockfile si installa da quello: un repository Git o un caricamento che ne ha uno, oppure un pacchetto npm il cui autore ha pubblicato `npm-shrinkwrap.json` (npm non pubblica mai `package-lock.json`). Il codice senza lockfile si installa con dipendenze non fissate: un repository Git o un caricamento senza lockfile, la maggior parte dei pacchetti npm e i pacchetti Python da PyPI, i cui wheel non hanno posto per uno. Fornite un lockfile accanto a quel codice perché ogni installazione sia identica. Alcuni codici non ne hanno bisogno, perché il modo in cui sono distribuiti fissa già ogni dipendenza.

Nessun pacchetto può eseguire i propri script di installazione: npm installa con `--ignore-scripts`, e Python installa solo wheel, senza mai compilare da una distribuzione sorgente.

I pacchetti arrivano dai registri della vostra organizzazione, se ne avete. Un amministratore inserisce una volta registri npm, indici PyPI e host Git in **Admin → Registri di pacchetti**, e ogni sorgente Node e Python dell'organizzazione installa attraverso di essi. Il registro npm pubblico e il PyPI pubblico non richiedono voci. Vedere [Registri di pacchetti](package-registries.md).

Un'installazione viene messa in cache per hash del contenuto del codice, così lo stesso codice si installa una volta sola.

## La sandbox {: #the-sandbox }

Il vostro codice gira in un proprio processo worker, in una sandbox. Può leggere il proprio codice installato e nient'altro sul disco. Raggiunge solo gli host della allow-list di uscita del deployment, tramite un proxy gestito da Provisa, e mai le porte di Provisa stesso. Un host che non può fornire la sandbox si rifiuta di avviare il worker e indica cosa manca, invece di eseguire il codice senza restrizioni. Su Windows le sorgenti di codice vengono rifiutate.

## Secret {: #secrets }

Il codice che chiama un'API a pagamento ha bisogno della sua chiave, e la chiave sta nel vault della vostra organizzazione, non nel codice. In **Secrets**, nel modulo della sorgente, scegliete i secret dell'organizzazione che il codice può leggere. In un file di configurazione, elencateli nel mapping della sorgente:

```yaml
mapping:
  secrets: [openrouter]
```

Un nome da solo è il secret dell'organizzazione con quel nome. Per leggere invece una variabile d'ambiente del deployment, indicate il riferimento: `secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`. L'elenco è un riferimento al vault, mai una copia di un valore, quindi una configurazione che nomina un secret non ancora salvato da nessuno si carica comunque.

Il vostro codice riceve una variabile d'ambiente, `PROVISA_SERVICES`, e legge un secret da `$PROVISA_SERVICES/secrets/<name>` quando gli serve. In Python:

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

In TypeScript:

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

La richiesta passa per lo stesso proxy di uscita del resto del traffico del vostro codice, e il proxy risponde direttamente. Nient'altro di Provisa è raggiungibile per quella via. L'indirizzo porta un token che solo il worker di questa sorgente possiede; legge solo i nomi che la sorgente elenca, e smette di funzionare quando il worker si ferma. Ogni lettura risolve il secret nel vault in quel momento, quindi un secret ruotato vale dalla lettura successiva.

Un nome che la sorgente non elenca viene rifiutato, così come un secret che non si riesce a risolvere. Il rifiuto nomina il secret, mai il suo valore. Ogni lettura è registrata nell'audit trail dell'organizzazione con la sorgente, il nome e l'ora, e nessun valore viene mai scritto in un log. L'host dell'API deve comunque essere nella lista di uscita consentita.

## Chiamare un modello {: #calling-a-model }

Il vostro codice può chiamare un modello linguistico tramite Provisa invece di tenere una chiave del fornitore. `$PROVISA_SERVICES/llm/v1` è un endpoint compatibile con OpenAI, quindi qualsiasi SDK OpenAI funziona se puntate lì il suo URL di base. L'indirizzo è la credenziale, quindi la chiave che l'SDK chiede può essere un testo qualsiasi. In Python:

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

Nominate un modello per ruolo, come `provisa:<role>`: ottenete il modello che la vostra organizzazione assegna a quel ruolo in **Admin → AI Models**, e segue quella scelta quando un amministratore la cambia. Potete anche nominare un modello direttamente quando è assegnato a un ruolo. Per gli embedding, `$PROVISA_SERVICES/llm/v1/embeddings` accetta uno qualsiasi dei modelli vettoriali della vostra organizzazione. Qualsiasi altro modello viene rifiutato per nome. La chiamata esce attraverso i fornitori, gli endpoint e le chiavi della vostra organizzazione, e ciò che invia passa prima per le regole di oscuramento di Provisa. Lo streaming non è offerto: inviate `stream: false`, che è il valore predefinito.

Ogni chiamata è registrata per la sorgente, con il modello, i suoi token e il suo costo secondo la voce del modello in **Polly model prices**, e scritta nell'audit trail dell'organizzazione. Per limitare la spesa di una sorgente, impostate **LLM monthly cap** nel suo modulo, in USD. In un file di configurazione va nel mapping:

```yaml
mapping:
  llm: {cap_usd: 20}
```

Una sorgente con un tetto può chiamare solo modelli che lì hanno un prezzo. Quando la sua spesa del mese (UTC) raggiunge il tetto, le sue chiamate sono rifiutate finché il mese non cambia. Le chiamate già in corso quando il tetto viene raggiunto terminano comunque, quindi il mese può chiudersi leggermente sopra il tetto.

## I servizi di Provisa {: #provisas-services }

Il vostro codice può importare un modulo che Provisa fornisce sempre, ovunque venga eseguito: `provisa_services` in Python e `@provisa/services` in Node.js, Bun e Deno. Non c'è niente da installare, e non si trova in nessun registro di pacchetti. Fuori da Provisa il modulo non esiste, quindi i test delle vostre funzioni lo simulano.

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
| `secrets.get(name)` | Uno dei secret dichiarati della sorgente, letto ora dal vault. Vedere [Secret](#secrets). |
| `settings.get(name, default)` | Una delle impostazioni della sorgente, da **Settings** nel suo modulo, che uno steward può cambiare senza cambiare il codice. Se manca e non c'è un valore predefinito, solleva un errore. |
| `cache.get(key, default)`, `cache.set(key, value, ttl)`, `cache.delete(key)` | La cache propria della sorgente, condivisa da tutti i suoi worker. Un valore è qualunque cosa JSON possa contenere, fino a 1 MB, conservato per `ttl` secondi o finché non viene sostituito. |
| `vectors.upsert(items, model)`, `vectors.search(query, k, model)`, `vectors.embed(texts, model)`, `vectors.delete(ids)` | La collezione vettoriale propria della sorgente, per i suoi dati di riferimento. Un elemento ha un `id` e un `text` o un `vector`; un testo viene vettorizzato da uno dei modelli vettoriali della vostra organizzazione. Non può cercare nelle vostre tabelle governate: i loro dati arrivano al codice solo come parametri. |
| `http.get(url)`, `http.post(url, ...)` | Una richiesta che Provisa fa per il codice, attraverso il proxy in uscita e le autorità di certificazione del deployment. L'host deve essere nella lista di uscita consentita, e ogni richiesta è registrata nell'audit trail. |
| `llm.chat(model, messages)`, `llm.embeddings(model, input)`, `llm.base_url()` (`llm.baseUrl()` in JavaScript) | Il servizio LLM di Provisa. Vedere [Chiamare un modello](#calling-a-model). |
| `log(message, level, fields)`, `metric(name, value, kind, attributes)` | Una riga di log e un punto di metrica chiamato `code_source.<name>`, legati alla richiesta che il vostro codice sta servendo. |

Una chiamata che il servizio rifiuta solleva un `ServiceError` che nomina il servizio e ne spiega il motivo.

I servizi rispondono al codice eseguito sul server Provisa. Su un host remoto non sono ancora raggiungibili, e ogni chiamata solleva un errore.

Ogni chiamata al vostro codice è tracciata come parte della richiesta che l'ha avviata, con la funzione, il suo tipo, quanti argomenti ha ricevuto (mai i loro valori), le righe e i lotti restituiti, la sua durata e la CPU del worker. Ogni chiamata a un servizio è tracciata allo stesso modo. Al livello di dettaglio normale queste informazioni sono sullo span della richiesta; al livello debug ogni chiamata è uno span a sé. Se il vostro codice usa OpenTelemetry, i suoi span si uniscono alla stessa traccia.

## Eseguire su un host remoto {: #running-on-a-remote-host }

Una sorgente gira per impostazione predefinita sul server Provisa; questa è la modalità dev. Quando il suo codice ha bisogno di una macchina che il server non è, come una macchina con GPU o un host dentro un'altra rete, costruite la sua immagine ed eseguitela lì. La sorgente passa allora alla modalità built, e le sue tabelle, i permessi e i lettori restano gli stessi.

**Costruire l'immagine.** Salvate prima la sorgente in modalità dev, così il suo codice viene letto e fissato. In **Remote host** nel suo modulo, scegliete l'architettura (Linux x86-64 o ARM64) e dove va l'immagine: uno dei registri di container della vostra organizzazione, inseriti in **Admin → Container registries**, oppure un download. Provisa assembla l'immagine da sé. Contiene il codice al suo hash fissato, le sue dipendenze installate per quella piattaforma dal lockfile del codice attraverso i vostri registri di pacchetti, il runtime fissato e il programma dell'host remoto, su un'immagine di base minima. Nulla nell'immagine viene eseguito mentre è costruita, e non interviene alcun daemon Docker. Il tag dell'immagine porta l'hash del codice e la versione di Provisa, e le sue label nominano il runtime, l'hash del codice e gli export.

**Eseguirla.** Il modulo mostra un servizio compose per l'immagine. Un'immagine scaricata si carica prima con `docker load -i <file>`. Il container ascolta internamente sulla porta 7443 e conserva la sua identità sul volume. La porta dell'host la scegliete con `PROVISA_HOST_PORT`; senza, il servizio non parte:

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

Al primo avvio il container scrive nel suo log una riga che inizia con `pairing code:`.

**Collegarla.** Tornati nel modulo, inserite l'host a cui Provisa raggiunge il container, la porta che avete pubblicato e il codice di abbinamento. Su una rete Docker condivisa o in Kubernetes, inserite il nome del container o del Service e 7443, senza pubblicare nulla. Provisa si abbina al container con TLS reciproco. Il codice si usa una volta, e da allora ciascun lato accetta solo l'altro. Provisa verifica poi che il container esegua il codice della sorgente e ne offra gli export, e porta la sorgente in modalità built. Ogni processo Provisa mantiene la propria connessione con l'host e si ricollega se l'host smette di rispondere. Finché l'host non risponde, le letture delle tabelle della sorgente falliscono nominando l'host.

**Se il codice cambia.** Un container che torna con un altro codice viene rifiutato, nominando l'hash a cui è fissata la sorgente e quello che l'host riporta, finché uno steward non esamina il codice e lo rifissa dal modulo. Per cambiare il codice stesso, riportate la sorgente in dev, salvate il nuovo codice e ricostruite.

Nel container, la sua policy di rete è il confine di uscita del codice; la lista di uscita consentita del server vale per la modalità dev.

## Leggere una tabella {: #reading-a-table }

La tabella di una sorgente di codice viene materializzata localmente, mai letta sul posto. Provisa chiama la funzione con gli argomenti della lettura, controlla ogni riga rispetto alle colonne e conserva le righe sotto quegli argomenti per il TTL di cache della tabella. Visibilità, maschere e regole di riga si applicano poi alle righe materializzate, come per qualsiasi tabella.

Gli argomenti si passano come qualsiasi parametro di tabella: come condizioni `_nf_` in SQL e come argomenti di campo in GraphQL.

## Chiavi a lotti: il parametro `loader` {: #batched-keys }

Un join può raggiungere una tabella di codice: le righe di un'altra tabella forniscono i suoi parametri, come i clienti uniti al loro punteggio di abbandono. Per impostazione predefinita Provisa chiama la funzione una volta per ogni insieme distinto di argomenti fornito dalle righe unite, e invia tutte queste chiamate al worker in una sola richiesta.

Una funzione che accetta un parametro chiamato `loader` viene invece chiamata una volta per lotto, lo schema reso familiare da DataLoader di GraphQL. `loader` è un hash: un dict (un oggetto in TypeScript) che associa un hash stabile di ogni chiave alla chiave stessa, un oggetto JSON con i valori delle righe unite per gli altri parametri della funzione. Provisa elimina le chiavi duplicate, così ogni combinazione distinta di valori compare una volta. Una chiave è sparsa: un campo senza valore viene omesso invece di essere inviato come null, così vale il valore predefinito della funzione stessa, come in una chiamata singola.

La funzione restituisce la stessa forma: un hash da ogni chiave ricevuta alle righe di quella chiave, righe normali della tabella. Provisa assegna le righe di ogni chiave a tutte le righe unite che condividevano quella chiave, e una chiave può corrispondere a zero righe o a molte. Associare tramite l'hash, e non tramite i valori delle righe, mantiene associata una chiave sparsa: la funzione riempie i campi mancanti con i valori predefiniti, quindi i valori che vede differiscono da quelli della riga unita. Provisa calcola l'hash; la funzione si limita a restituire le chiavi ricevute. La funzione dichiara comunque solo il suo tipo di riga, una lista di righe, e le righe di ogni chiave sono di quel tipo. I suoi parametri accanto a `loader` devono avere valori predefiniti, perché una chiamata con loader passa solo `loader`.

Una funzione può limitare la dimensione dei suoi lotti, come fa `maxBatchSize` di DataLoader, dichiarando `loader N` in uno di tre punti: una riga `@loader 100` accanto a `@table` nella docstring o nel JSDoc, un commento `/* @loader 100 */` subito prima del parametro `loader` in TypeScript, oppure una stringa `Annotated` sul parametro in Python, `loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`. Provisa divide allora le chiavi in lotti di quella dimensione, chiama la funzione una volta per lotto e unisce le risposte tramite l'hash. Senza dimensione, un solo lotto contiene tutte le chiavi. La scoperta rifiuta per nome una dimensione su una funzione senza parametro `loader`, una dimensione che non è un intero positivo e due dimensioni sulla stessa funzione.

Usate `loader` quando una chiamata su molte chiavi rende meglio di molte chiamate su una:

- Valutare un modello su un intero lotto in un solo passaggio vettorizzato.
- Raggruppare richieste a un'API o a un modello linguistico che accetta molti input alla volta.
- Preparare una volta per lotto, come caricare un modello o aprire una connessione, invece che una volta per chiave.

Questa funzione Python valuta ogni cliente di un lotto in un solo passaggio:

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

Unita a una tabella di clienti, viene chiamata con un `loader` come `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}`. La seconda chiave porta solo `customer_id`, quindi piano e regione prendono i valori predefiniti. La funzione restituisce `{"9f2c...": [...], "41ab...": [...], ...}`, e Provisa assegna ogni lista di righe a ogni riga cliente che condivideva quella chiave.

La stessa funzione in TypeScript:

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

## Un esempio {: #an-example }

Questo modulo Python valuta quanto è probabile che un account se ne vada:

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

Registratelo come sorgente `python` dal suo pacchetto PyPI, dal commit Git o da un caricamento. La discovery offre allora `churn_risk` come tabella con quattro colonne (`account_id`, `score`, `computed_at`, `reasons`), un parametro obbligatorio `account_id` e uno facoltativo, `as_of`. Quando uno steward ha registrato la tabella, leggetela in SQL:

```sql
SELECT score, reasons
FROM churn_risk
WHERE _nf_account_id = 'A-1042'
```

La stessa funzione in TypeScript si presenta così:

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
