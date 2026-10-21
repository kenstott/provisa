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

Il tipo di riga dà le colonne. Testo, numeri, booleani, date e timestamp corrispondono ai loro tipi di colonna, le liste restano liste e gli oggetti annidati diventano tipi con nome. Quando il tipo di una riga è generico (`any`, `unknown` o un `Record` in TypeScript, `Any` o un `dict` in Python), Provisa chiama la funzione una volta, senza argomenti, e suggerisce le colonne dalle righe restituite. Se la funzione ha un parametro obbligatorio non può essere chiamata così, e tipizzate voi le colonne quando registrate la tabella.

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

## Leggere una tabella {: #reading-a-table }

La tabella di una sorgente di codice viene materializzata localmente, mai letta sul posto. Provisa chiama la funzione con gli argomenti della lettura, controlla ogni riga rispetto alle colonne e conserva le righe sotto quegli argomenti per il TTL di cache della tabella. Visibilità, maschere e regole di riga si applicano poi alle righe materializzate, come per qualsiasi tabella.

Gli argomenti si passano come qualsiasi parametro di tabella: come condizioni `_nf_` in SQL e come argomenti di campo in GraphQL.

## Chiavi a lotti: il parametro `loader` {: #batched-keys }

Un join può raggiungere una tabella di codice: le righe di un'altra tabella forniscono i suoi parametri, come i clienti uniti al loro punteggio di abbandono. Per impostazione predefinita Provisa chiama la funzione una volta per ogni insieme distinto di argomenti fornito dalle righe unite, e invia tutte queste chiamate al worker in una sola richiesta.

Una funzione che accetta un parametro chiamato `loader` viene invece chiamata una volta per lotto, lo schema reso familiare da DataLoader di GraphQL. `loader` è una lista di oggetti JSON, una chiave ciascuno, con i valori delle righe unite per gli altri parametri della funzione. Provisa elimina le chiavi duplicate, così ogni combinazione distinta di valori compare una volta. La lista è sparsa: un campo senza valore viene omesso dalla sua chiave invece di essere inviato come null, così vale il valore predefinito della funzione stessa, come in una chiamata singola.

La funzione restituisce righe normali che portano le loro colonne chiave, e Provisa le riunisce alle righe unite su quelle colonne, mai per posizione. Le righe possono tornare in qualsiasi ordine, e una chiave può dare zero righe o molte.

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
    plan: str
    region: str
    score: float


def churn_score(
    customer_id: str, plan: str = "basic", region: str = "US", loader: list[CustomerKey] | None = None
) -> list[Score]:
    """Churn score of each customer in the batch.

    @table
    """
    keys = loader or [{"customer_id": customer_id, "plan": plan, "region": region}]
    rows = [{"plan": "basic", "region": "US", **key} for key in keys]
    scores = MODEL.predict(features(rows))  # one vectorized call for the whole batch
    return [Score(r["customer_id"], r["plan"], r["region"], s) for r, s in zip(rows, scores)]
```

Unita a una tabella di clienti, viene chiamata con `loader=[{"customer_id": "637", "plan": "pro", "region": "EU"}, {"customer_id": "638"}, ...]`. La seconda chiave porta solo `customer_id`, quindi piano e regione prendono i valori predefiniti. Ogni riga restituita porta `customer_id`, `plan` e `region`, e Provisa la associa al suo cliente in base a questi.

La stessa funzione in TypeScript:

```typescript
export interface CustomerKey {
  customer_id: string;
  plan?: string;
  region?: string;
}

export interface Score {
  customer_id: string;
  plan: string;
  region: string;
  score: number;
}

/** Churn score of each customer in the batch.
 * @table
 */
export async function churnScore(
  customer_id: string,
  plan = "basic",
  region = "US",
  loader?: CustomerKey[],
): Promise<Score[]> {
  const keys = loader ?? [{ customer_id, plan, region }];
  const rows = keys.map((key) => ({ plan: "basic", region: "US", ...key }));
  const scores = await model.predict(rows); // one call for the whole batch
  return rows.map((row, i) => ({ ...row, score: scores[i] }) as Score);
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
