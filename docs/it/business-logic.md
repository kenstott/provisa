# La logica di business in Provisa

## Il problema {: #the-problem }

La logica di business finisce ovunque: nei job ETL, nelle dashboard e nelle API, in mano a team diversi e catalogata da nessuno. Il «ricavo netto» viene calcolato in un modo nel job del warehouse, in un altro nella dashboard finanziaria e in un terzo nell'API di fatturazione, e nessuno sa dire quale sia giusto.

Non è colpa di nessuno. Non c'era un modello comune e semplice in cui mettere le regole.

## Cos'è la logica di business {: #definition }

La logica di business sono le regole che creano, aggiornano, eliminano o trasformano i dati.

La terminologia di business è un'altra cosa. I nomi e le descrizioni di domini, tabelle e colonne, e i termini del [glossario](glossary.md), dicono cosa significano i dati. La logica di business dice cosa se ne fa.

## Tre ruoli {: #roles }

Ogni regola svolge uno di tre ruoli, e ogni ruolo si può scrivere in tre modi:

| Ruolo | SQL | Chiamata esterna | Resolver di codice |
|---|---|---|---|
| **Dataset** | vista, comando di istruzione | API o procedura che restituisce righe | `@table`, `@stream` |
| **Valore** | espressione di vista, metrica | — | — |
| **Modifica** | `INSERT`, `UPDATE`, `DELETE`, comando di istruzione | scrittura via API, mutation, procedura | `@command` |

- **Chiamata esterna.** L'API GraphQL, REST o gRPC di un altro sistema, o la stored procedure di un database, registrata come comando.
- **Metrica.** Un aggregato con nome, definito una volta, come `net_revenue` dato da `SUM(orders.amount) - SUM(orders.refunds)`. Ogni query lo scompone per le dimensioni che chiede, così warehouse, dashboard e API leggono lo stesso numero.

## Effetti collaterali nascosti {: #side-effects }

Un effetto collaterale nascosto è uno che non si vede dalla definizione della regola.

Ogni regola ha un nome e un tipo, ed è registrata, sottoposta ad audit e protetta.

| | Possibili effetti collaterali | Limitati da |
|---|---|---|
| **SQL** | Nessuno | — |
| **Chiamata esterna** | I flussi di lavoro, le notifiche e le cascate propri dell'altro sistema | Approvazione |
| **Resolver di codice** | Chiamate a servizi e reti esterni; risultati che variano; costo | Sandbox, misurazione |

Una chiamata esterna è tipizzata dallo schema proprio del sistema, o dagli argomenti e dalle colonne di ritorno che dichiari quando registri il comando.

Questi limiti contengono gli effetti collaterali ma non possono eliminarli. Ciò che fa un sistema esterno o il tuo stesso codice dipende in ultima analisi da esso.

## Cosa ti dà il modello {: #what-you-get }

- Ogni regola è registrata in un dominio, con permessi.
- Ogni regola è governata allo stesso modo, comunque sia scritta.
- Ogni regola è visibile nel [lineage](lineage.md).

## Da leggere {: #read-next }

- [Viste](configuration.md#views-governed-computed-datasets)
- [Metriche](modeling.md#metrics)
- [Comandi](commands.md)
- [Resolver di codice](code-sources.md)
- [Controlli di qualità dei dati](sources.md#data-quality-checkers)
