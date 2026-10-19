# Repliche {: #replicas }

Una replica è una copia di una tabella di origine che Provisa conserva nello store di materializzazione del motore e legge al posto dell'origine. Se una tabella sia replicata o meno è stabilito da [`replicate`](configuration.md#the-replicate-setting) e da ciò che il motore può raggiungere; vedere [What an engine changes](engines.md#what-an-engine-changes).

## Le repliche sono indicizzate {: #replicas-are-indexed }

Provisa indicizza una replica nel modo in cui il modello usa la sua tabella.

- **La chiave primaria dichiarata**, imposta come chiave.
- **Le colonne su cui si uniscono le relazioni dichiarate**:
  - la chiave esterna di ogni relazione la cui origine è questa tabella;
  - la colonna di destinazione di ogni relazione la cui destinazione è questa tabella, a meno che quella colonna non sia già la chiave;
  - entrambe le chiavi esterne di una tabella ponte (junction) attraversata da una relazione.

Prendi questa relazione:

```yaml
relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

La replica di `orders` riceve la sua chiave e un indice su `customer_id`. La replica di `customers` riceve la sua chiave e, quando `id` è quella chiave, non le serve altro. Un join tra le due legge quindi un indice su entrambi i lati, in uno store che usa gli indici per i join (vedere [What each store builds](#what-each-store-builds)). Una relazione su una colonna calcolata che la replica non contiene non riceve alcun indice.

Dichiara la chiave con `is_primary_key: true` sulla colonna. Una replica senza chiave dichiarata riceve solo i suoi indici di join.

### Leggibile prima, indicizzata dopo {: #readable-first-indexed-after }

Un caricamento non attende mai gli indici. La replica è leggibile dal momento in cui viene sostituita; il replica runner costruisce poi i suoi indici uno alla volta, in un passaggio separato, sotto un lock di indici per replica. Aspettati che le prime query dopo un caricamento girino senza di essi.

Ogni caricamento registra gli indici richiesti, quelli costruiti e quelli falliti, per nome, nel record della replica nel piano di controllo (`replica_state.index_state`).

- **Un indice fallito non fa mai fallire il caricamento.** Viene registrato nel log con la causa e richiesto di nuovo dal caricamento successivo. La riga di log recita `replica of <source>.<schema>.<table>: index <name> on (<columns>) failed: <cause>`.
- **Righe che ripetono una chiave dichiarata sono un errore a sé**, con codice `replica.index_duplicate_key`, che nomina le colonne della chiave. L'origine contiene duplicati di un insieme di colonne che il modello dichiara univoco. Un nuovo tentativo non lo risolve: correggi i dati nell'origine, oppure correggi la chiave dichiarata.
- **Un caricamento che sostituisce la tabella durante una costruzione** lascia indietro le costruzioni precedenti; si fermano e non registrano nulla, e la nuova tabella ottiene le proprie. Su uno store PostgreSQL la sostituzione annulla una costruzione ancora in corso sulla tabella che sostituisce.
- **Il piano segue il modello a ogni caricamento.** Una relazione aggiunta in seguito riceve il suo indice al caricamento successivo della replica.

L'interfaccia di amministrazione non mostra il record degli indici. Leggi il log del server, oppure la colonna `index_state` della riga della replica.

### Che cosa costruisce ogni store {: #what-each-store-builds }

| Store | Che cosa viene costruito | Come |
| --- | --- | --- |
| PostgreSQL: il database proprio del motore `pg`, lo store PostgreSQL di Trino o DuckDB, il motore `sqlalchemy` su PostgreSQL | Indici di chiave e di join | `CREATE INDEX CONCURRENTLY` su una connessione propria, così lettori e scrittori non sono bloccati. La chiave è costruita come indice univoco, poi resa chiave primaria con `ADD CONSTRAINT ... PRIMARY KEY USING INDEX`. Un indice non valido lasciato da una costruzione annullata viene eliminato e ricostruito. |
| Altri database dietro un motore relazionale (`mysql`, `oracle`, `mssql` e il resto) | Indici di chiave e di join | Il DDL proprio del dialetto. Un database che rifiuta il DDL registra l'indice come fallito. |
| File DuckDB embedded, MotherDuck | Indici di chiave e di join | Tramite il broker dello store: `ALTER TABLE ... ADD PRIMARY KEY`, `CREATE INDEX IF NOT EXISTS`. |
| ClickHouse | Solo la chiave di ordinamento | Una replica ClickHouse è ordinata per la sua chiave dichiarata (`ORDER BY`), impostata al caricamento. ClickHouse non ha un indice B-tree secondario da costruire. |
| Snowflake, BigQuery, Databricks, Fabric, Synapse | Nulla | Il warehouse organizza da sé la tabella. |

Un indice aiuta solo un join che gira dove si trova l'indice. Due motori leggono il proprio store senza usarne gli indici per i join:

- **DuckDB con il suo file embedded.** Il motore non collega mai il file. Ogni istruzione legge la propria copia locale di ogni tabella dello store che nomina, rifatta ogni volta che lo store è cambiato. I join girano come hash join su quella copia, quindi gli indici di join non li accelerano; gli indici del file impongono la chiave e servono ciò che legge il file sul posto.
- **DuckDB con uno store PostgreSQL.** DuckDB legge lo store tramite la sua estensione `postgres` con il filter pushdown disattivato (bug upstream duckdb/duckdb-postgres#564), quindi scansiona la replica e filtra e unisce in DuckDB. Gli indici dello store sono costruiti ma queste letture non li usano.

Uno store MotherDuck è collegato e letto sul posto.

## Usare un database relazionale come store delle repliche dove il motore esegue SQL al suo interno {: #use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it }

Quando il motore esegue i suoi join dentro un database relazionale, conserva lì le repliche. I join leggono allora gli indici descritti sopra. Tre configurazioni lo fanno:

- **Il motore `pg`.** Le repliche atterrano nel database PostgreSQL del motore stesso e ogni istruzione gira lì, attraverso i loro indici.
- **Un motore relazionale** (`mysql`, `oracle`, `mssql`, il generico `sqlalchemy` e il resto). Ogni origine atterra in quel database e vi è interrogata.
- **Trino con il suo store PostgreSQL.** PostgreSQL è l'unico store che Trino supporta; Trino legge le repliche tramite il suo connettore PostgreSQL. Un filtro o un join che Trino spinge verso PostgreSQL usa gli indici; il lavoro che Trino svolge da sé no.

Punta un motore a uno store con `PROVISA_MATERIALIZE_URL` o `materialize_store_url`; lo store registrato da un'organizzazione ha la precedenza su entrambi.

```bash
PROVISA_MATERIALIZE_URL="postgresql://provisa:secret@replicas.internal:5432/replicas"
```

È una raccomandazione generale, non una regola. Se convenga dipende dal motore, dalle origini, dallo store e dalle dimensioni delle tabelle. Valuta questi punti:

- **Raggiungibilità per query.** Uno store che il motore raggiunge in rete aggiunge un viaggio di andata e ritorno e un trasferimento a ogni lettura. Il file DuckDB embedded è locale e colonnare, e scansiona veloce.
- **Costo di caricamento.** Scrivere una replica in un database relazionale e poi costruirne gli indici costa più che scrivere un file locale.
- **Il carico del database stesso.** Letture delle repliche, caricamenti e costruzioni di indici girano tutti su quel database. Dimensionalo per essi.
- **Scansioni grandi.** Un indice non fa nulla per una query che legge la maggior parte di una tabella grande. Uno store colonnare la scansiona più velocemente di uno a righe.

## Scegliere dove vivono le repliche {: #choosing-where-replicas-live }

Lo store è una leva tra tante, e si sovrappongono. Sistema prima le altre; lo store decide solo ciò che lasciano.

| Leva | Che cosa fa | Dove aiuta |
| --- | --- | --- |
| Tabelle hot | Una piccola tabella di lookup è tenuta in Redis e inserita in ogni query che la legge come CTE (common table expression) `VALUES` inline. | Join verso piccoli lookup, su qualsiasi store: le righe viaggiano dentro la query. La destinazione di una relazione molti-a-uno, o una tabella con al più `hot_tables.auto_threshold` righe (1.000 per default), diventa hot alla prima lettura. `hot: true` ve la forza e `hot: false` la esclude. |
| Viste materializzate che definisci | Una vista con `materialize: true` è memorizzata e aggiornata. Una query che legge la vista è servita dalle righe memorizzate quando è aggiornata e chi legge la vede per intero: nessun input porta una regola di riga più stretta di tutto. | Aggregazioni su tabelle grandi. Decidi tu quali esistono; nessuna è creata per te. Vedere [Views](configuration.md#views-governed-computed-datasets). |
| Relazione con `materialize: true` | Una relazione tra origini ottiene una vista padre-figlio memorizzata e pre-unita. Una richiesta il cui join corrisponde al pattern, in tutto o in parte, viene riscritta per leggerla, sia che arrivi come SQL sia che la costruisca il compilatore. | Join su uno store che non costruisce indici di join. Vedere [Auto-Materialization](configuration.md#auto-materialization). |
| Posizionamento (Placement) | Le origini che il motore può leggere dal vivo sono lette dal vivo; le altre sono replicate. Ciò che finisce nello store dipende dal motore scelto. | La scelta del motore decide la maggior parte di ciò che lo store deve contenere. |

La materializzazione delle relazioni e i join indicizzati rispondono allo stesso bisogno in due modi. Su uno store colonnare o warehouse (il file DuckDB, ClickHouse, Snowflake, BigQuery, Databricks, Fabric, Synapse) non viene costruito alcun indice di join, quindi una vista pre-unita è il modo per rendere economico un join frequente. Su uno store relazionale le colonne di join sono già indicizzate, e gli stessi join possono girare veloci senza materializzarli.

Ciò che dipende ancora dallo store: scansioni e join ad hoc su tabelle grandi che nessuna vista materializzata copre e nessuna tabella hot serve. Girano sugli indici dello store in un database relazionale, o sulla sua velocità di scansione in uno colonnare.

I risultati variano con il carico di lavoro. Misura prima e dopo una modifica con lo [strumento di dimensionamento](sizing.md); le sue [esecuzioni live-o-replica](sizing.md#live-or-replica-reads-and-the-ttl) danno il costo di una lettura da replica rispetto a una dal vivo sulla tua distribuzione.

## Freschezza {: #freshness }

Fai della freschezza parte della decisione, accanto alla velocità. Quanto vecchia può essere una risposta e quanto costa all'origine tenerla aggiornata decidono se una tabella è letta dal vivo o da una replica, e quindi che cosa lo store deve contenere.

Le origini aziendali raramente supportano il tempo reale. I dati arrivano con caricamenti batch o ETL notturni, i feed di modifiche ritardano e molti sistemi non possono sostenere letture dal vivo al volume delle query. Parti dalla freschezza di cui ogni tabella ha bisogno, poi scegli la strategia più economica che la soddisfa. Le regole lato operatore sono in [Freshness and load: who decides](configuration.md#freshness-and-load-who-decides).

| Strategia | Si imposta con | Che cosa garantisce | Che cosa costa all'origine |
| --- | --- | --- | --- |
| Lettura live | `replicate: -1` | Aggiornata al momento della query | Ogni query legge l'origine |
| TTL | `change_signal: ttl`, `cache_ttl` | Una replica non più vecchia di `cache_ttl` | Una ricostruzione per `cache_ttl`; con un `watermark_column`, solo le righe oltre il watermark |
| Sonda di modifica | `change_signal: probe` o `ttl_probe`, `cache_ttl` | Ricostruita solo dopo che la sonda segnala una modifica; la sonda gira ogni `cache_ttl` | Una sonda per intervallo e una ricostruzione solo in caso di modifica |
| Feed di modifiche in push | Un'origine Kafka o WebSocket; `change_signal: debezium` su un'origine con blocco `cdc`; `change_signal: native` su PostgreSQL, MongoDB o Cosmos DB | Aggiornata entro il ritardo del feed e il debounce | Kafka, WebSocket e Debezium: nulla dopo la prima costruzione, il feed porta le righe. PostgreSQL, MongoDB e Cosmos DB: una ricostruzione per raffica di modifiche |
| Segnale esterno | `change_signal: signal` | Ricostruita ogni volta che `POST /data/sources/{source_id}/signal` dice che l'origine è cambiata; mai a orario | Una ricostruzione per segnale |
| TTL per ruolo | `role_ttl` su una tabella | Un ruolo elencato accetta una replica fino al proprio TTL; il `cache_ttl` della tabella resta il minimo | Meno ricostruzioni: la lettura di un ruolo tollerante non ne richiede una |
| Protezione dal carico | `load_protected: true`, con `cache_ttl`, una sonda o una `off_peak_window` | Le query non leggono mai l'origine; solo il runner aggiorna la replica, e con una finestra fuori picco solo mentre è aperta | Solo gli aggiornamenti pianificati |
| Freshness gate | `freshness_gate: true` su un'origine | Ogni lettura è prima verificata rispetto a `change_signal` e `cache_ttl` dell'origine; un verdetto di obsolescenza aggiorna prima della lettura | Un aggiornamento nel percorso di lettura quando obsoleta |
| Non scade mai | `cache_ttl: -1` su un'origine o una tabella | Caricata una volta; ogni lettura è servita dalla replica, e la lettura di una tabella non ancora caricata è rifiutata indicandone il nome | Solo quel caricamento: un'origine le cui tabelle sono tutte caricate non viene più collegata, sondata né letta, e non richiede credenziali |

Una sonda che non ha nulla da confrontare (nessun watermark, sentinel o query di sonda) degrada al TTL.

### Quasi tempo reale da un feed di modifiche {: #near-real-time-from-a-change-feed }

Quando un'origine non può sostenere carico di query dal vivo ma pubblica già le proprie modifiche, un feed di modifiche dà dati quasi in tempo reale senza toccare l'origine al momento della query. Le query leggono la replica locale, che è veloce e non carica l'origine, mentre il ciclo di eventi applica le modifiche man mano che arrivano. I dati ritardano solo del feed, in genere secondi.

La presenza del feed non basta. Provisa lo segue solo per una tabella collegata ad esso:

- **Un'origine Kafka.** La tabella indica il proprio topic e ogni modifica è applicata alla replica per chiave primaria, cancellazioni incluse.

    ```yaml
  live:
    strategy: kafka
    kafka:
      topic: orders.cdc
  push_debounce_quiet: 0      # seconds of quiet before a batch lands; 0 lands each message
  push_debounce_max_delay: 5  # a batch never waits longer than this
  ```

- **Un'origine WebSocket.** Il suo flusso è applicato allo stesso modo.
- **Una tabella con `change_signal: debezium`** (sulla tabella o sulla sua origine) su un'origine il cui blocco `cdc` indica il trasporto Kafka e il prefisso di topic del connettore. La prima lettura costruisce la replica; poi ogni evento di modifica sul topic `{topic_prefix}.{schema}.{table}` è applicato per chiave primaria, eliminazioni comprese, e l'offset è confermato solo dopo che la modifica è stata caricata.
- **Una tabella di una tale origine con `change_signal: kafka`**. Le sue modifiche sono lette dal topic di delta che il suo `live.kafka.topic` indica, sullo stesso trasporto, e applicate allo stesso modo; una tabella con questo segnale che non indica alcun topic è rifiutata per nome al salvataggio della tabella o della sua origine e al caricamento della configurazione.
- **Una tabella MongoDB o Cosmos DB con `change_signal: native`** (sulla tabella o sulla sua origine). Ogni raffica di modifiche sul change stream richiede una ricostruzione della replica.
- **Una tabella PostgreSQL con `change_signal: native`** (sulla tabella o sulla sua origine). Provisa legge la replica logica del database da uno slot proprio, `provisa_<source id>`, su una pubblicazione con lo stesso nome che elenca le tabelle native dell'origine. Kafka non è coinvolto. Ogni raffica di modifiche a una tabella chiede una ricostruzione della sua replica. Il database richiede ciò che richiede Debezium: `wal_level` impostato a `logical`, un ruolo con il diritto di replica che possa creare la pubblicazione, e un primario anziché una replica in lettura; un'origine a cui manca uno di questi è rifiutata per nome al salvataggio e al caricamento della configurazione. Lo slot e la pubblicazione sono creati quando la prima tabella li segue ed eliminati quando l'ultima smette o l'origine viene eliminata. All'avvio un server controlla ogni origine PostgreSQL registrata ed elimina uno slot di Provisa che nessuna tabella nativa segue, così uno slot lasciato da una configurazione cambiata mentre nessun server era attivo non continua a trattenere il WAL dell'origine.

Una tabella in push richiede una chiave primaria dichiarata; una senza viene saltata e registrata nel log.

Una tabella con un segnale di modifica in push conta come aggiornata nel percorso di lettura, perché il suo feed è il suo controllo di freschezza. Per questo un segnale push che nulla riceverebbe è rifiutato: `change_signal: native` su qualunque tipo diverso da Kafka, WebSocket, ingest, PostgreSQL, MongoDB o Cosmos DB è rifiutato al salvataggio dell'origine o della tabella e al caricamento della configurazione, e così `debezium` su un'origine senza blocco `cdc`.
