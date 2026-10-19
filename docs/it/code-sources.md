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

Un pacchetto pubblicato raramente include il suo lockfile, perciò il modulo ne accetta uno accanto al riferimento del pacchetto.

Nessun pacchetto può eseguire i propri script di installazione: npm installa con `--ignore-scripts`, e Python installa solo wheel, senza mai compilare da una distribuzione sorgente.

I pacchetti arrivano dai registri della vostra organizzazione, se ne avete. Un amministratore inserisce una volta registri npm, indici PyPI e host Git in **Admin → Registri di pacchetti**, e ogni sorgente Node e Python dell'organizzazione installa attraverso di essi. Il registro npm pubblico e il PyPI pubblico non richiedono voci. Vedere [Registri di pacchetti](package-registries.md).

Un'installazione viene messa in cache per hash del contenuto del codice, così lo stesso codice si installa una volta sola.

## La sandbox {: #the-sandbox }

Il vostro codice gira in un proprio processo worker, in una sandbox. Può leggere il proprio codice installato e nient'altro sul disco. Raggiunge solo gli host della allow-list di uscita del deployment, tramite un proxy gestito da Provisa, e mai le porte di Provisa stesso. Un host che non può fornire la sandbox si rifiuta di avviare il worker e indica cosa manca, invece di eseguire il codice senza restrizioni. Su Windows le sorgenti di codice vengono rifiutate.

## Leggere una tabella {: #reading-a-table }

La tabella di una sorgente di codice viene materializzata localmente, mai letta sul posto. Provisa chiama la funzione con gli argomenti della lettura, controlla ogni riga rispetto alle colonne e conserva le righe sotto quegli argomenti per il TTL di cache della tabella. Visibilità, maschere e regole di riga si applicano poi alle righe materializzate, come per qualsiasi tabella.

Gli argomenti si passano come qualsiasi parametro di tabella: come condizioni `_nf_` in SQL e come argomenti di campo in GraphQL.

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
