# Creare una vista bitemporale {: #create-a-bitemporal-view }

Una vista bitemporale conserva entrambe le cose: quando qualcosa è accaduto e quando è stato registrato. Puoi quindi leggerla a una data, o come era registrata a una data, e una correzione successiva non nasconde mai ciò che si sapeva prima.

Una vista diventa bitemporale associando tre delle sue colonne:

- **Chiave di business**: la colonna, o le colonne, che identificano lo stesso record tra le versioni, come `transaction_id`. Un pagamento corretto resta lo stesso pagamento, quindi entrambe le versioni hanno la stessa chiave.
- **Tempo di validità**: la colonna con la data che la tua attività dà al record, ad esempio quando è stato concordato, registrato in contabilità o è entrato in vigore.
- **Tempo di registrazione**: la colonna con il momento in cui questa versione è stata inserita o modificata, come `updated_at`.

Il tempo di registrazione si chiama tempo di transazione nella letteratura sui database temporali e tempo di sistema in SQL:2011; il tempo di validità si chiama tempo applicativo in SQL:2011, o data di efficacia.

I tre valori associano colonne che la tua sorgente ha già. Provisa decide come conservare le versioni; non c'è una modalità di archiviazione da scegliere.

## Tre esempi {: #three-examples }

| Esempio | Tempo di validità | Tempo di registrazione |
| ------- | ----------------- | ---------------------- |
| Un pagamento di 100 per il 3 ott. viene corretto a 120 il 5 ott. | 3 ott., mantenuto dalla correzione. | 3 ott. per la prima versione; 5 ott. per la correzione. |
| Una misurazione sul campo presa il 3 ott. alle 14:10 viene digitata il 6 ott. Un refuso viene corretto il 7 ott. | 3 ott. 14:10, mantenuto dalla correzione. | 6 ott.; la correzione riceve il 7 ott. |
| Un'operazione viene registrata all'esecuzione, data operazione 3 ott., alle 10:02. Il 4 ott. la quantità risulta errata e viene rettificata. | Data operazione 3 ott., mantenuta dalla rettifica. | 3 ott. 10:02; la rettifica riceve il 4 ott. |

Letta come comunicata il 3 ott., l'operazione mostra la quantità errata. Letta secondo quanto si sa oggi, mostra quella corretta.

## Creare la vista {: #create-the-view }

Una vista che conserva la cronologia è materializzata, così le sue versioni vengono archiviate.

1. Apri **Views** e fai clic su **+ View**.
2. Definisci la vista in una delle due schede:
    - **Metrics**: dai un nome alla vista, scegli dominio, metriche e dimensioni, poi fai clic su **Continue**.
    - **SQL**: apri l'editor SQL, scrivi ed esegui la `SELECT`, fai clic su **+ View** nell'editor, dai un nome alla vista e fai clic su **Continue**.
3. Le impostazioni della vista si aprono nelle stesse sezioni del suo editor. In **Load and timeliness**, spunta **Materialized View**. Compare la sezione **Time Travel**.
4. Imposta **Keep history** su **Bitemporal**.
5. Scegli le colonne **Business key**, **Valid time** e **Recorded time**.
6. Imposta il resto nelle altre sezioni, poi fai clic su **Save**. Fino ad allora non viene registrato nulla.

Le tre si dichiarano insieme o per niente. Provisa rifiuta una vista che ne nomina solo alcune e indica quali mancano.

Una tabella riceve le stesse sezioni quando la registri: sceglila in **+ Table**, fai clic su **Continue**, e la sua sezione Time Travel è in **Load and timeliness**. Un modello riceve la sezione quando lo crei (**+ Model**). Per un modello, la sua chiave è la chiave di business, quindi la sezione chiede solo i due tempi.

## Una tabella aggiornata sul posto {: #a-table-updated-in-place }

Alcune tabelle tengono ogni versione come riga a sé, quindi una correzione aggiunge una riga con la stessa chiave di business. Altre tengono una riga per chiave e la sovrascrivono, quindi la loro chiave di business è la loro chiave primaria. Per una tabella del secondo tipo, Provisa tiene le versioni da sé in un registro delle versioni:

- Ogni acquisizione legge le righe registrate dopo l'ultima ora di registrazione presente nel registro, e aggiunge quelle cambiate.
- Il ritardo ammesso del registro allarga questa finestra, così una riga confermata in ritardo con un'ora di registrazione precedente viene comunque letta.
- Un'acquisizione non vede una riga che la sorgente ha eliminato. **Full capture** rilegge l'intera tabella e registra ogni riga eliminata come scomparsa.
- Il registro inizia dalla sua prima acquisizione: prima di essa, ogni riga ha solo la versione che aveva allora.

Il registro compare in **Cache**, nella scheda **Materialized Store**, come **Version log of** la tabella. Le sue **Settings** stabiliscono ogni quanto viene acquisito e il ritardo ammesso; **Full capture** è accanto. Una lettura semplice della tabella legge la tabella; una lettura a una data, o come registrata a una data, legge il suo registro.

## Leggerla {: #read-it }

La vista compare nell'elenco **Views** con il badge **HISTORY**. Il suo pannello di dettaglio indica che conserva la cronologia e rimanda all'explorer SQL con la vista selezionata. Imposta i selettori **As of** e **As recorded at** dell'explorer sui momenti che vuoi, poi esegui la query.

Gli stessi tempi raggiungono ogni altra superficie. Una sessione pgwire li imposta una volta per ogni istruzione che esegue:

```sql
SET provisa.as_of = '2026-10-03T12:00:00';
SET provisa.as_recorded = '2026-10-03T23:59:59';
```

Le intestazioni HTTP, gli argomenti di `resolve` e i parametri REST sono elencati in [Viste con cronologia](configuration.md#views-with-history-time-travel).

## Modificarla in seguito {: #change-it-later }

L'editor mostra la sezione Time Travel nella scheda **Load and timeliness** della vista. Una volta costruita la vista, le sue versioni sono registrate sotto la sua chiave di business e il suo tempo di validità, quindi l'editor non offre più questi due, né l'interruttore che disattiva Time Travel. Il tempo di registrazione può ancora cambiare. Per organizzare la cronologia con un'altra chiave, crea una nuova vista.
