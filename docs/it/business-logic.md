# La logica di business in Provisa

## Il problema {: #the-problem }

Ogni organizzazione ha degli standard per la logica di business, e la maggior parte dei team li segue. Eppure, negli anni, mentre i sistemi vengono costruiti e sostituiti e i team cambiano, la logica si sparpaglia tra job ETL, dashboard, API e stored procedure. Ogni pezzo ha senso da solo. Spesso nessuno ha pianificato l'insieme, e più spesso ancora nessuno ha pianificato l'insieme nel tempo. Il risultato è noto: il «ricavo netto» viene calcolato in tre modi in tre posti, e nessuno sa dire quale sia giusto.

## Cos'è la logica di business {: #definition }

La logica di business sono le regole che creano, aggiornano, eliminano o trasformano i dati.

La terminologia di business è un'altra cosa. I nomi e le descrizioni di domini, tabelle e colonne, e i termini del [glossario](glossary.md), dicono cosa significano i dati. La logica di business dice cosa se ne fa.

## Tre ruoli {: #roles }

Ogni regola svolge uno di tre ruoli, e ogni ruolo si può scrivere in tre modi:

| Ruolo | SQL | Chiamata esterna | Codice |
|---|---|---|---|
| **Dataset** | vista, comando di istruzione | API o procedura che restituisce righe | resolver di codice `@table`, `@stream`; comando `python` o `script` |
| **Valore** | espressione di vista, metrica | — | comando `python` o `script` |
| **Modifica** | `INSERT`, `UPDATE`, `DELETE`, comando di istruzione | scrittura via API, mutation, procedura | `@command`; comando `python` o `script` |

- **Chiamata esterna.** L'API GraphQL, REST o gRPC di un altro sistema, o la stored procedure di un database, registrata come comando.
- **Tabelle come argomenti.** Un comando può ricevere tabelle come argomenti, così un dataset può essere costruito da altri dataset.
- **Metrica.** Un aggregato con nome, definito una volta, come `net_revenue` dato da `SUM(orders.amount) - SUM(orders.refunds)`. Ogni query lo scompone per le dimensioni che chiede, così warehouse, dashboard e API leggono lo stesso numero.

## Effetti collaterali nascosti {: #side-effects }

Un effetto collaterale nascosto è uno che non si vede dalla definizione della regola.

Ogni regola ha un nome e un tipo, ed è registrata, sottoposta ad audit e protetta. Un comando può anche richiedere un'approvazione prima di essere eseguito.

| | Possibili effetti collaterali | Limitati da |
|---|---|---|
| **SQL** | Nessuno | — |
| **Chiamata esterna** | I flussi di lavoro, le notifiche e le cascate propri dell'altro sistema | Lista di uscita consentita (HTTP, gRPC); credenziali conservate come segreti (procedure, operazioni di sorgente) |
| **Resolver di codice** | Chiamate a servizi e reti esterni; risultati che variano; costo | Codice fissato, impronta dell'host fissata, sandbox, misurazione |
| **Codice sull'host** | Legge tutto ciò che il server può leggere (memoria, credenziali, file, rete); può affamare il server o farlo cadere | `commands.allow_host_code`, disattivato per impostazione predefinita |

Una chiamata esterna è tipizzata dallo schema proprio del sistema, o dagli argomenti e dalle colonne di ritorno che dichiari quando registri il comando.

Il codice di un resolver di codice è fissato per versione del pacchetto, commit Git o hash dell'archivio, così viene promosso con il modello.

Il codice sull'host è un comando `python` o `script`. Viene eseguito dentro il server con tutto il suo accesso, così un codice malevolo, difettoso o vorace può leggere tutto ciò che il server può, affamarlo o farlo cadere. È comodo, e sicuro quando ti fidi del codice. Il modello contiene solo un riferimento (`module:attr` o una riga di comando), così il codice non viaggia con la promozione. Viene eseguito solo dove `commands.allow_host_code` è attivo.

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
