# Server pgwire di Provisa {: #provisa-pgwire-server }

Provisa espone un endpoint del protocollo wire di PostgreSQL (pgwire). Qualsiasi strumento che parli il protocollo client di PostgreSQL, come psycopg2, asyncpg, DBeaver, Tableau o JDBC, può connettersi e interrogare i dati Provisa attraverso la stessa pipeline di governance che governa l'API HTTP.

Le query attraversano l'intero stack di governance: applicazione della sicurezza a livello di riga, regole di mascheramento, protezioni sulle relazioni, controlli di accesso al dominio. L'interfaccia pgwire non è un modo per aggirare i controlli.

---

## Dettagli di connessione {: #connection-details }

Il server si avvia quando `PROVISA_PGWIRE_PORT` è impostato su un intero diverso da zero. È disabilitato per impostazione predefinita.

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS.** Impostare `PROVISA_PGWIRE_CERT` e `PROVISA_PGWIRE_KEY` sui percorsi di un certificato e di una chiave PEM. Quando entrambi sono presenti, il server incapsula le connessioni in entrata in TLS. Quando sono assenti, TLS è disattivato e il server risponde `N` alle richieste di negoziazione SSL.

**Versione del server segnalata.** I client vedono `14.0.provisa`. Gli strumenti che attivano funzionalità in base al numero di versione possono comportarsi come se fossero connessi a PostgreSQL 14.

---

## Autenticazione {: #authentication }

Il pacchetto di startup porta un nome utente e un solo campo segreto, senza alcuno schema che dica cosa sia quel segreto. Provisa decide dal segreto stesso, così un client non ha bisogno di configurazione oltre a `user` e `password`:

| Il segreto è | Riconosciuto da | Si risolve in |
| --------------- | --------------- | ------------- |
| Un token di accesso personale | il suo prefisso `provisa_pat_` | il proprietario e il ruolo del token |
| Un token bearer OIDC / del provider | il fatto che il provider configurato sia un provider di token | l'identità che il token afferma |
| Una password | qualsiasi altra cosa | l'account nel provider configurato (`basic` o `simple`) |

La decisione viene presa una sola volta. Una credenziale rifiutata dal validatore scelto non viene ritentata con un altro, quindi un rifiuto non diventa un secondo tentativo.

La modalità trust (`provider: none`, o middleware di autenticazione inattivo) è l'eccezione: il nome utente viene usato direttamente come `role_id` e il segreto viene ignorato. Non usarla su una connessione non cifrata.

**SCRAM-SHA-256.** Con `provider: basic` e `auth.scram: true`, il server annuncia SASL (codice di autenticazione 10) con `SCRAM-SHA-256` e la password viene dimostrata anziché inviata. `SCRAM-SHA-256-PLUS` non viene offerto. A un utente il cui verifier non è ancora stato scritto — i verifier non possono essere derivati dagli hash bcrypt — viene risposto con uno scambio fittizio, così la rete non rivela chi ha migrato; quell'utente si autentica con password in chiaro su TLS finché l'immissione successiva della password non ne scrive uno. Con `auth.scram` disattivato, il server usa il tipo di autenticazione PG 3 (password in chiaro). MD5 non è supportato in nessuno dei due casi.

**Certificati client.** Imposta `PROVISA_MTLS_CLIENT_CA` e il server verifica un certificato client durante l'handshake, prima di esaminare qualsiasi credenziale. Con `PROVISA_MTLS_BIND_PRINCIPAL` il common name del certificato deve coincidere con lo `user` con cui la connessione si autentica subito dopo. Vedi [Configurazione](configuration.md#mutual-tls).

**I tentativi falliti vengono contati.** Cinque fallimenti in cinque minuti bloccano l'account per quindici minuti. Il contatore è condiviso con HTTP e Bolt, quindi un blocco ottenuto su una superficie vale su tutte.

**Scegliere un'organizzazione.** In un deployment multi-organizzazione, connettiti a `<org>.<your-domain>` e pgwire legge l'organizzazione dall'hostname nel ClientHello TLS, allo stesso modo in cui HTTP la legge dall'header `Host`. L'hostname richiede un'organizzazione; non la concede, e un principal che non ne è membro viene rifiutato. Connettersi tramite indirizzo IP non richiede alcuna organizzazione.

---

## Cosa funziona {: #what-works }

### SELECT

Tutte le istruzioni SELECT attraversano la pipeline di governance (`_pipeline.py`). La pipeline:

1. Riscrive l'SQL semantico in SQL fisico (`rewrite_semantic_to_physical`)
2. Applica la governance (sicurezza a livello di riga, mascheramento, accesso al dominio)
3. Valida rispetto allo schema registrato
4. Instrada verso Trino o direttamente verso il pool dell'origine

Sono supportate query semplici multi-istruzione. Le istruzioni separate da punto e virgola vengono suddivise ed eseguite in ordine.

Le query parametrizzate (`$1`, `$2`, ...) sono supportate sia in modalità query semplice sia in modalità query estesa (Bind/Execute). I parametri vengono sostituiti come letterali prima dell'esecuzione.

`SELECT * FROM fn(args)` e `SELECT fn(args)` — dove `fn` indica una funzione registrata e tracciata — vengono intercettati prima della pipeline di governance e instradati attraverso l'unico esecutore governato (`invoke_tracked_function`). Il risultato è un insieme di righe tipizzato identico a quello restituito da ogni altra superficie per quel comando. L'assegnazione del comando (`visible_to`), il diritto `write` per una mutazione e le regole di governance vengono applicate all'interno dell'esecutore.

### Cursori (DECLARE / FETCH / MOVE / CLOSE) {: #cursors-declare-fetch-move-close }

Provisa gestisce la sintassi testuale dei cursori SQL sul protocollo simple-query. I client che inviano `DECLARE`/`FETCH`/`MOVE`/`CLOSE` come normali istruzioni SQL ottengono un vero streaming lato server, senza errori di protocollo e senza caricare l'intero risultato nella memoria del client.

**È diverso dallo streaming basato su portali.** Il protocollo di query estesa — Bind/Execute con un campo di limite di righe — è un meccanismo diverso del protocollo wire. asyncpg e la maggior parte dei driver PostgreSQL moderni lo usano automaticamente, senza SQL di cursore. Se il vostro driver usa già i portali, non vi serve questa sintassi. `DECLARE CURSOR` è per i client che inviano letteralmente SQL di cursore: i cursori con nome/lato server di psycopg2 (`conn.cursor(name="...")`), alcuni strumenti BI e alcuni vecchi driver JDBC.

**DECLARE**

```sql
DECLARE c1 CURSOR FOR SELECT id, amount FROM orders WHERE region = 'us-west';
DECLARE c2 SCROLL CURSOR FOR SELECT * FROM events ORDER BY ts;
DECLARE "CaseSensitive" NO SCROLL CURSOR FOR SELECT * FROM large_table;
```

La SELECT interna passa dalla normale pipeline governata di Provisa, con la stessa applicazione della RLS, lo stesso mascheramento e gli stessi controlli di accesso per dominio di qualsiasi SELECT autonoma. Dichiarare un cursore non aggira la governance.

La parola chiave `SCROLL` controlla sia la navigazione consentita sia la memoria che usa il cursore:

| Dichiarazione | Navigazione all'indietro | Costo di memoria |
|---|---|---|
| `DECLARE c CURSOR FOR ...` (predefinito) | Non consentita | Limitato a un batch di FETCH |
| `DECLARE c NO SCROLL CURSOR FOR ...` | Non consentita | Limitato a un batch di FETCH |
| `DECLARE c SCROLL CURSOR FOR ...` | Consentita | Risultato completo nel buffer della memoria Python |

Quando non è scritto né `SCROLL` né `NO SCROLL`, il predefinito è `NO SCROLL` — come il predefinito di PostgreSQL. Un cursore `NO SCROLL` rimuove dal suo buffer interno le righe consumate dopo ogni passo in avanti, così una scansione `FETCH FORWARD` di un grande risultato non tiene mai in memoria Python più di circa un batch alla volta. Un cursore `SCROLL` conserva ogni riga che ha recuperato — è il prezzo del supporto a `FETCH BACKWARD`, `ABSOLUTE`, `FIRST` e `LAST`. Prima di dichiarare un cursore `SCROLL` su un grande risultato, mettete in conto l'intera dimensione del risultato come heap Python.

Ridichiarare un nome di cursore già aperto chiude prima il cursore precedente. `WITH HOLD` e `WITHOUT HOLD` sono accettati nella sintassi; vedere la durata del cursore qui sotto.

**FETCH e MOVE**

```sql
FETCH 100 FROM c1            -- next 100 rows
FETCH FORWARD 100 FROM c1    -- same
FETCH ALL FROM c1            -- all remaining rows
FETCH BACKWARD 10 FROM c1    -- previous 10 rows (SCROLL cursors only)
FETCH ABSOLUTE 50 FROM c1    -- the row at absolute position 50 (SCROLL cursors only)
FETCH RELATIVE -5 FROM c1    -- 5 positions back (SCROLL cursors only)
FETCH FIRST FROM c1          -- row 1 (SCROLL cursors only)
FETCH LAST FROM c1           -- the final row (SCROLL cursors only)
MOVE 100 FROM c1             -- advance position without returning rows
```

`FETCH` restituisce righe e invia il tag di comando `FETCH <n>`, dove `n` è il numero effettivamente restituito — che può essere minore di quello richiesto quando il cursore raggiunge la fine. `MOVE` avanza la posizione senza restituire righe e invia `MOVE <n>`.

`FROM` e `IN` sono intercambiabili per introdurre il nome del cursore. I nomi di cursore senza virgolette vengono portati in minuscolo; i nomi tra virgolette (`"CaseSensitive"`) conservano maiuscole e minuscole.

La navigazione all'indietro (`BACKWARD`, `ABSOLUTE`, `FIRST`, `LAST`, `RELATIVE` negativo) su un cursore `NO SCROLL` restituisce SQLSTATE 55000 (`object_not_in_prerequisite_state`), come il rifiuto di PostgreSQL stesso. Un FETCH o MOVE che nomina un cursore mai dichiarato restituisce SQLSTATE 34000 (`invalid_cursor_name`).

**CLOSE**

```sql
CLOSE c1;    -- release one cursor and free its result stream
CLOSE ALL;   -- release every open cursor on this connection
```

`CLOSE` rilascia il flusso di risultati sottostante e rimuove il cursore dalla sessione. Disconnettersi senza emettere `CLOSE` è sicuro: `ProvisaSession.close()` rilascia automaticamente tutti i cursori ancora aperti alla disconnessione.

**Durata di un cursore.** Il server pgwire di Provisa non ha una macchina a stati delle transazioni (`ProvisaSession.in_transaction()` restituisce sempre `False`). Ogni cursore si comporta quindi come se fosse dichiarato `WITH HOLD`: vive quanto la connessione, e `COMMIT` non lo chiude. Le parole chiave `WITH HOLD` e `WITHOUT HOLD` vengono accettate e ignorate, perché non c'è un ciclo di vita del cursore per transazione da applicare.

### Definizioni (CREATE, ALTER, DROP) {: #definitions-create-alter-drop }

Nulla viene definito tramite pgwire. Un'istruzione che crea, modifica o elimina un oggetto (`CREATE TABLE`, `CREATE TABLE … AS SELECT`, `CREATE VIEW`, `ALTER`, `DROP`, un indice, una sequenza o uno schema) viene rifiutata con SQLSTATE 0A000, qualunque cosa detenga il ruolo; l'eccezione è la tabella temporanea propria di una sessione (vedere [Tabelle temporanee](#temporary-tables)). Una tabella o una vista è un oggetto del modello: createla nel modello (pagine di amministrazione, API di amministrazione o configurazione), oppure createla nella sorgente dati e ammettetela nel modello. `TRUNCATE` non definisce nulla: è una scrittura di dati (vedere [Scritture di dati](#data-writes-insert-update-delete-merge-copy-from-stdin)).

Il rifiuto avviene una volta, nella pipeline che ogni superficie SQL attraversa, così SQL su HTTP, Flight e MCP rifiutano le stesse istruzioni con lo stesso messaggio.

### Tabelle temporanee {: #temporary-tables }

Una sessione può creare, scrivere, leggere ed eliminare tabelle temporanee proprie:

```sql
CREATE TEMP TABLE busy AS SELECT customer_id, COUNT(*) AS n FROM sales.orders GROUP BY customer_id;
INSERT INTO busy VALUES (0, 0);
UPDATE busy SET n = n + 1 WHERE customer_id = 0;
DELETE FROM busy WHERE n < 5;
SELECT c.name, b.n FROM sales.customers c JOIN busy b ON b.customer_id = c.id;
DROP TABLE busy;
```

- **Creare** con `CREATE TEMP TABLE name (column type, ...)` o `CREATE TEMP TABLE name AS SELECT ...`. Il nome non prende uno schema. Una `SELECT` da cui viene creata è una lettura come un'altra: le regole di riga, le maschere, i dati fittizi e il limite di righe del ruolo si applicano alle righe che prende.
- **Tipi di colonna**: `smallint`, `integer`, `bigint`, `real`, `double precision`, `boolean`, `date`, `timestamp`, `text`, `varchar` e `DECIMAL(precision, scale)`. Un decimale conserva precisione e scala. Qualsiasi altro tipo viene rifiutato, indicando la colonna e il tipo: un valore non viene mai memorizzato come un tipo diverso dal proprio. Una colonna creata da una `SELECT` prende il tipo dei suoi valori; una senza un valore da cui dedurre un tipo viene rifiutata, quindi applicate un `CAST` nella `SELECT`.
- **Limite di righe.** Le righe che prende un `CREATE ... AS SELECT` o un `INSERT ... SELECT` vengono lette come il ruolo legge qualsiasi cosa, quindi si applica il limite di righe del ruolo. Una tabella temporanea riempita da una lettura troncata dal limite porta lo stesso avviso di istruzione di qualsiasi lettura troncata (`statement.rows_cut`), con il limite.
- **Scrivere** con `INSERT ... VALUES`, `INSERT ... SELECT`, `UPDATE` e `DELETE`, senza `RETURNING` e senza `FROM`. Una scrittura in una tabella temporanea non è una mutazione: non raggiunge alcuna sorgente né alcun dato dell'ambiente, quindi funziona qualunque sia la gestione delle mutazioni dell'ambiente e non richiede diritti di scrittura.
- **Leggere** con il suo nome, da sola o in join con tabelle del modello. Il motore calcola la lettura.
- **Durata.** Una tabella temporanea non è un oggetto del modello, nessun'altra sessione la vede, e termina con la sessione. Su pgwire una sessione è la connessione. Su una superficie senza connessione propria (SQL su HTTP, Flight, MCP) una sessione è una richiesta, quindi una tabella temporanea vive per le istruzioni inviate insieme in quella richiesta.
- **Memorizzazione.** Le righe sono conservate in uno schema proprio della sessione nello store del motore ed eliminate alla fine della sessione. Un `INSERT`, `UPDATE` o `DELETE` riscrive l'intera tabella tramite il motore, quindi una tabella temporanea è adatta a dati di lavoro, non a grandi tabelle scritte riga per riga. Un processo che si arresta senza terminare le sue sessioni lascia i loro schemi nello store; vengono rimossi quando l'ambiente viene ritirato.

Le stesse istruzioni funzionano su ogni superficie SQL: sono gestite nella pipeline che tutte condividono.

### Scritture di dati (INSERT, UPDATE, DELETE, MERGE, COPY FROM STDIN) {: #data-writes-insert-update-delete-merge-copy-from-stdin }

Le righe di una tabella registrata esistente si scrivono tramite pgwire come su ogni altra superficie SQL, attraverso l'unica ammissione delle scritture (`provisa/compiler/write_admission.py`):

1. **Il diritto.** Il ruolo detiene `write`.
2. **L'autorizzazione.** Il ruolo detiene un'autorizzazione di scrittura sulla tabella per l'operazione: un `INSERT` richiede un'autorizzazione `insert`, un `UPDATE` una `update`, un `DELETE` una `delete`. Un `MERGE` richiede un'autorizzazione `insert` e una `update` su ogni riga, e un `INSERT ... ON CONFLICT DO UPDATE` una `update` su ogni riga.
3. **Le colonne.** Ogni colonna scritta dall'istruzione è una che l'autorizzazione nomina. Una colonna che l'autorizzazione predefinisce è dell'autorizzazione: l'istruzione non può fornirla, e il valore dell'autorizzazione (una variabile di sessione, un letterale o il momento della scrittura) viene aggiunto.
4. **Le righe che può toccare.** Il predicato `rows` dell'autorizzazione viene aggiunto al `WHERE` di un `UPDATE` o `DELETE`. Un'autorizzazione che non ne indica nessuno prende il filtro di righe del ruolo, lo stesso predicato che portano le sue letture.
5. **Le righe che lascia.** Le righe di un `INSERT`, e i nuovi valori di un `UPDATE` che imposta una colonna letta dal `check` dell'autorizzazione, vengono verificati rispetto al `check` prima che venga inviato qualcosa alla sorgente. Un'autorizzazione che non indica un `check` prende il filtro di righe del ruolo.

`TRUNCATE` è anch'essa una scrittura di dati, eseguita come tale e mai riscritta come `DELETE`. Richiede il diritto `write`, un'autorizzazione `delete` su ogni riga della tabella e una sorgente che accetti le cancellazioni. Svuota una tabella alla volta, e un ruolo la cui autorizzazione limita le righe (un predicato `rows`, o il filtro di righe del ruolo dove l'autorizzazione non ne indica) viene rifiutato, perché un `TRUNCATE` non può applicare il predicato: usare `DELETE`, che lo applica.

Ciò che non si può decidere viene rifiutato per nome anziché ammesso nel dubbio (un `INSERT … SELECT`, un valore che non è né un letterale né un parametro associato, un filtro che legge una colonna che l'istruzione non fornisce).

### COPY

Sono supportati sia `COPY ... TO STDOUT` sia `COPY ... FROM STDIN`.

**COPY TO STDOUT** — esporta i risultati delle query nel formato wire COPY di PG. Funzionano due forme:

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```

Formati supportati: `text` (separato da tabulazioni, predefinito), `csv` e `binary`, il formato COPY binario di PostgreSQL, con cui legge l'estensione postgres di DuckDB.

**COPY FROM STDIN** è una scrittura di dati in una tabella registrata esistente: è ammessa dalla stessa ammissione di scrittura di un `INSERT` delle stesse righe, con la condizione aggiuntiva che l'autorizzazione `insert` non abbia preset (vedi [Scritture di dati](#data-writes-insert-update-delete-merge-copy-from-stdin)). Legge `text` (predefinito) e `csv`. Qualsiasi altro formato viene rifiutato con SQLSTATE 0A000, nominandolo, prima che venga inviato qualsiasi dato.

### Transazioni e comandi di sessione {: #transactions-and-session-commands }

SET, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, DISCARD, RESET e DEALLOCATE vengono intercettati e ricevono una risposta di successo vuota. Il server non tiene stato di transazione, quindi non c'è isolamento delle transazioni né rollback.

---

## Intercettazione del catalogo {: #catalog-intercept }

Le query su `information_schema` e `pg_catalog` vengono risolte localmente senza un round-trip verso Trino. Il livello di intercettazione costruisce un database DuckDB in memoria per ogni richiesta, popolato a partire dal contesto di compilazione del ruolo.

Tabelle intercettate:

**information_schema:** `schemata`, `tables`, `columns`, `views`, `table_constraints`, `key_column_usage`, `referential_constraints`

**pg_catalog:** `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_attrdef`, `pg_description`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_roles`, `pg_auth_members`, `pg_database`, `pg_settings`, `pg_tables`, `pg_stat_user_tables`, `pg_statio_user_tables`, `pg_am`, `pg_extension`, `pg_enum`, `pg_stat_activity`

`pg_constraint` viene popolata con dati reali di chiave primaria e chiave esterna derivati dai campi `pk_columns` e `joins` del modello di dominio. Gli strumenti di BI che ispezionano le relazioni di chiave esterna (Tableau, DBeaver, ecc.) vedranno il grafo di join che Provisa conosce. I join a colonna singola tra la stessa coppia origine/destinazione le cui colonne di destinazione formano insieme la chiave primaria composita della destinazione vengono raggruppati in un'unica riga FK con array `conkey`/`confkey` a più elementi.

Una relazione basata su tabella di giunzione non produce righe FK. È un arco attraverso una tabella associativa, e `pg_constraint` non ha una forma per due salti, quindi il modello di dominio la lascia fuori da `joins` e la tabella di giunzione compare come tabella normale con le proprie chiavi esterne verso ciascun estremo. I client SQL la raggiungono unendo quella tabella; i client Cypher la percorrono come un'unica relazione.

`pg_index` viene popolata con una riga per ogni vincolo di chiave primaria e UNIQUE (`indrelid` = oid della tabella, `indkey` = attnum delle chiavi in ordine, `indisprimary`/`indisunique` impostati). I client che risolvono le colonne chiave tramite `pg_index.indkey` anziché tramite `pg_constraint` — DataGrip, ad esempio — individuano le colonne corrette tramite il join standard `pg_index` → `pg_attribute`.

Vengono intercettate anche le seguenti espressioni scalari:

- `current_user`, `session_user` → il `role_id` autenticato
- `current_database()` → `"provisa"`
- `current_schema()` → `"public"`
- `version()` → `"PostgreSQL 14.0 on Provisa"`
- `pg_backend_pid()` → `0`
- `current_setting(...)` → restituisce un valore da una tabella di impostazioni fissa
- `SHOW <setting>` → restituisce un valore dalla stessa tabella di impostazioni

---

## Codifica binaria dei parametri {: #binary-parameter-encoding }

Il protocollo di query estesa (Bind/Execute) supporta parametri codificati in binario. I seguenti OID di tipo vengono decodificati dal binario:

| OID | Tipo PG | Tipo Python |
| ----- | --------- | ------------- |
| 16 | bool | bool |
| 17 | bytea | bytes |
| 20 | int8 | int |
| 21 | int2 | int |
| 23 | int4 | int |
| 25 | text | str |
| 700 | float4 | float |
| 701 | float8 | float |
| 1043 | varchar | str |
| 1082 | date | datetime.date |
| 1114 | timestamp | datetime.datetime |
| 1184 | timestamptz | datetime.datetime (UTC) |
| 1700 | numeric | decimal.Decimal |
| 2950 | uuid | str |

Qualsiasi OID non presente in questa tabella genera `"Unsupported binary parameter type: <oid>"`.

Anche le colonne dei risultati vengono inviate in binario quando il client lo richiede, per lo stesso insieme di tipi più ARRAY, JSON, INTERVAL e BIGINT.

---

## Raccomandazioni sui driver {: #driver-recommendations }

**Driver Python nativi (psycopg2, asyncpg).** Questi negoziano il protocollo di query estesa per impostazione predefinita e utilizzano la codifica binaria per la maggior parte dei tipi. La fedeltà dei tipi qui è massima — le colonne `NUMERIC` arrivano come `Decimal`, `TIMESTAMP` come `datetime`, e così via. Usarli per ETL basati su Python, script o integrazione diretta.

**JDBC (driver JDBC di PostgreSQL).** Usarlo per gli strumenti dell'ecosistema Java: DBeaver, Tableau, Power BI, Metabase, operatori JDBC di Airflow. JDBC utilizza per impostazione predefinita il protocollo di query semplice, che evita complicazioni legate alla codifica binaria. Stringa di connessione:

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

Alcuni strumenti di BI basati su JDBC inviano, alla connessione, una raffica di query verso `information_schema` e `pg_catalog` per popolare il proprio browser di schema. Tutte vengono risolte dal livello di intercettazione del catalogo — non viene generato traffico verso Trino durante l'ispezione dello schema.

**Quando preferire l'uno o l'altro.** Se il client è in Python, usare psycopg2 o asyncpg per una migliore gestione dei tipi. Se il client è uno strumento di BI o qualsiasi applicazione JVM, usare JDBC. Evitare di mescolare aspettative di protocollo binario e testuale sulla stessa connessione se si osservano anomalie nella conversione dei tipi — il comportamento in modalità testo di JDBC è più semplice da interpretare.

---

## Avvertenze e vincoli {: #caveats-and-constraints }

**Solo SQL.** Il listener pgwire analizza ed esegue SQL; non accetta stringhe GraphQL o Cypher. Le scritture di dati nelle tabelle esistenti sono ammesse dai diritti del ruolo, dalle sue concessioni sulle colonne e dal suo filtro di riga; vedi [Scritture di dati](#data-writes-insert-update-delete-merge-copy-from-stdin).

**Nessuna definizione.** `CREATE`, `ALTER` e `DROP` vengono rifiutati con SQLSTATE 0A000 qualunque cosa detenga il ruolo, tranne la tabella temporanea propria di una sessione; vedere [Tabelle temporanee](#temporary-tables). `TRUNCATE` è una scrittura di dati.

**Nessun supporto reale alle transazioni.** BEGIN/COMMIT/ROLLBACK vengono accettati e ignorati silenziosamente. Ogni istruzione viene eseguita in modo indipendente.

**Timeout delle query.** Un'istruzione può usare il timeout di richiesta proprio di pgwire, `limits.request_timeouts.pgwire`, o quello condiviso `limits.request_timeout` quando il primo non è impostato; entrambi sono impostazioni dell'operatore.

**L'input di COPY è text o csv.** `COPY ... TO STDOUT` scrive `text`, `csv` e `binary`; `COPY ... FROM STDIN` legge `text` e `csv`.

**I parametri restano legati.** I valori di `$1`, `$2`, ... non vengono mai inseriti nel testo dell'istruzione: viaggiano legati fino al motore o all'origine, così tutti i valori condividono un solo testo SQL (i prepare lato server e le cache basate sul testo SQL continuano a funzionare) e un valore non può mai cambiare la forma governata. Solo l'intercettazione del catalogo, che risponde da sé alle query su `information_schema` e `pg_catalog`, legge i valori inline.

**DECLARE SCROLL mette in buffer il risultato completo.** Un cursore `SCROLL` accumula nell'heap Python ogni riga che recupera dal motore, così da poter servire `FETCH BACKWARD`, `ABSOLUTE`, `FIRST` e `LAST` su richiesta. Con un grande risultato, l'intero risultato vive in memoria finché il cursore è aperto. Usate il predefinito `NO SCROLL` per lo streaming solo in avanti.

**`pg_stat_activity`, `pg_stat_user_tables`, `pg_extension`, `pg_enum`, `pg_attrdef`, `pg_proc`.** Queste tabelle esistono nel livello di catalogo ma sono stub vuoti. Gli strumenti di monitoraggio che le interrogano riceveranno zero righe anziché errori. (`pg_index` è popolata — vedere Intercettazione del catalogo.)
