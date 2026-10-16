# Server pgwire di Provisa {: #provisa-pgwire-server }

Provisa espone un endpoint del protocollo di rete di PostgreSQL (pgwire). Qualsiasi strumento che parla il protocollo client di PostgreSQL — psycopg2, asyncpg, DBeaver, Tableau, JDBC — può connettersi ed eseguire query sui dati di Provisa attraverso la stessa pipeline di governance che governa l'API HTTP. (REQ-266)

Le query attraversano l'intero stack di governance: applicazione della sicurezza a livello di riga, regole di mascheramento, protezioni sulle relazioni, controlli di accesso al dominio. (REQ-001, REQ-002, REQ-263) L'interfaccia pgwire non è un modo per aggirare i controlli. (REQ-002, REQ-266)

---

## Dettagli di connessione {: #connection-details }

Il server si avvia quando `PROVISA_PGWIRE_PORT` è impostato su un intero diverso da zero. È disabilitato per impostazione predefinita. (REQ-527) [tool-verified: `app.py:1739`]

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS.** Impostare `PROVISA_PGWIRE_CERT` e `PROVISA_PGWIRE_KEY` sui percorsi di un certificato e di una chiave PEM. Quando entrambi sono presenti, il server incapsula le connessioni in entrata in TLS. Quando sono assenti, TLS è disattivato e il server risponde `N` alle richieste di negoziazione SSL. (REQ-530) [tool-verified: `server.py:1746-1750`]

**Versione del server segnalata.** I client vedono `14.0.provisa`. Gli strumenti che attivano funzionalità in base al numero di versione possono comportarsi come se fossero connessi a PostgreSQL 14. (REQ-579) [tool-verified: `server.py:208`]

---

## Autenticazione {: #authentication }

Il pacchetto di startup porta un nome utente e un solo campo segreto, senza alcuno schema che dica cosa sia quel segreto. Provisa decide dal segreto stesso, così un client non ha bisogno di configurazione oltre a `user` e `password`:

| Il segreto è | Riconosciuto da | Si risolve in |
| --------------- | --------------- | ------------- |
| Un token di accesso personale | il suo prefisso `provisa_pat_` | il proprietario e il ruolo del token (REQ-1263) |
| Un token bearer OIDC / del provider | il fatto che il provider configurato sia un provider di token | l'identità che il token afferma (REQ-890) |
| Una password | qualsiasi altra cosa | l'account nel provider configurato (`basic` o `simple`) |

La decisione viene presa una sola volta. Una credenziale rifiutata dal validatore scelto non viene ritentata con un altro, quindi un rifiuto non diventa un secondo tentativo.

La modalità trust (`provider: none`, o middleware di autenticazione inattivo) è l'eccezione: il nome utente viene usato direttamente come `role_id` e il segreto viene ignorato. Non usarla su una connessione non cifrata.

**SCRAM-SHA-256.** Con `provider: basic` e `auth.scram: true`, il server annuncia SASL (codice di autenticazione 10) con `SCRAM-SHA-256` e la password viene dimostrata anziché inviata. (REQ-1394) `SCRAM-SHA-256-PLUS` non viene offerto. A un utente il cui verifier non è ancora stato scritto — i verifier non possono essere derivati dagli hash bcrypt — viene risposto con uno scambio fittizio, così la rete non rivela chi ha migrato; quell'utente si autentica con password in chiaro su TLS finché l'immissione successiva della password non ne scrive uno. Con `auth.scram` disattivato, il server usa il tipo di autenticazione PG 3 (password in chiaro). MD5 non è supportato in nessuno dei due casi.

**Certificati client.** Imposta `PROVISA_MTLS_CLIENT_CA` e il server verifica un certificato client durante l'handshake, prima di esaminare qualsiasi credenziale. (REQ-1228) Con `PROVISA_MTLS_BIND_PRINCIPAL` il common name del certificato deve coincidere con lo `user` con cui la connessione si autentica subito dopo. Vedi [Configurazione](configuration.md#mutual-tls).

**I tentativi falliti vengono contati.** Cinque fallimenti in cinque minuti bloccano l'account per quindici minuti, e il contatore è condiviso con HTTP e Bolt: un blocco ottenuto su una superficie vale su tutte. (REQ-1393)

**Scegliere un'organizzazione.** In un deployment multi-organizzazione, connettiti a `<org>.<your-domain>` e pgwire legge l'organizzazione dall'hostname nel ClientHello TLS, allo stesso modo in cui HTTP la legge dall'header `Host`. (REQ-1234) L'hostname richiede un'organizzazione; non la concede, e un principal che non ne è membro viene rifiutato. Connettersi tramite indirizzo IP non richiede alcuna organizzazione.

---

## Cosa funziona {: #what-works }

### SELECT

Tutte le istruzioni SELECT attraversano la pipeline di governance (`_pipeline.py`). (REQ-001, REQ-262, REQ-266) La pipeline:

1. Riscrive l'SQL semantico in SQL fisico (`rewrite_semantic_to_physical`)
2. Applica la governance (sicurezza a livello di riga, mascheramento, accesso al dominio) (REQ-263)
3. Valida rispetto allo schema registrato (REQ-011)
4. Instrada verso Trino o direttamente verso il pool dell'origine (REQ-027, REQ-028)

Sono supportate query semplici multi-istruzione. Le istruzioni separate da punto e virgola vengono suddivise ed eseguite in ordine. (REQ-580) [tool-verified: `server.py:318-381`]

Le query parametrizzate (`$1`, `$2`, ...) sono supportate sia in modalità query semplice sia in modalità query estesa (Bind/Execute). I parametri vengono sostituiti come letterali prima dell'esecuzione. (REQ-581) [tool-verified: `server.py:78-85`]

`SELECT * FROM fn(args)` e `SELECT fn(args)` — dove `fn` indica una funzione registrata e tracciata — vengono intercettati prima della pipeline di governance e instradati attraverso l'unico esecutore governato (`invoke_tracked_function`). Il risultato è un insieme di righe tipizzato identico a quello restituito da ogni altra superficie per quel comando. `writable_by` e le regole di governance vengono applicate all'interno dell'esecutore. (REQ-1156) [tool-verified: `provisa/pgwire/function_call.py:74-88`]

### Cursori (DECLARE / FETCH / MOVE / CLOSE) {: #cursors-declare-fetch-move-close }

Provisa gestisce la sintassi testuale dei cursori SQL sul protocollo di query semplice. (REQ-1862) I client che inviano `DECLARE`/`FETCH`/`MOVE`/`CLOSE` come semplici istruzioni SQL ottengono un vero streaming lato server invece di un errore di protocollo o di un buffer con l'intero risultato caricato nella memoria del client.

**È diverso dallo streaming basato su portali.** Il protocollo di query estesa — Bind/Execute con un campo di limite di righe — è un meccanismo diverso del protocollo wire. asyncpg e la maggior parte dei driver PostgreSQL moderni lo usano automaticamente, senza SQL di cursore. Se il vostro driver usa già i portali, non vi serve questa sintassi. `DECLARE CURSOR` è per i client che inviano letteralmente SQL di cursore: i cursori con nome/lato server di psycopg2 (`conn.cursor(name="...")`), alcuni strumenti BI e alcuni vecchi driver JDBC.

**DECLARE**

```sql
DECLARE c1 CURSOR FOR SELECT id, amount FROM orders WHERE region = 'us-west';
DECLARE c2 SCROLL CURSOR FOR SELECT * FROM events ORDER BY ts;
DECLARE "CaseSensitive" NO SCROLL CURSOR FOR SELECT * FROM large_table;
```
La SELECT interna passa per la normale pipeline governata di Provisa — la stessa applicazione della RLS, lo stesso mascheramento e gli stessi controlli di accesso ai domini di qualsiasi SELECT autonoma. [tool-verified: `server.py:1367` — `ctx.execute_sql(inner_sql)`, the same call path as a plain SELECT] Dichiarare un cursore non aggira la governance.

La parola chiave `SCROLL` controlla sia la navigazione consentita sia la memoria che usa il cursore:

| Dichiarazione | Navigazione all'indietro | Costo di memoria |
|---|---|---|
| `DECLARE c CURSOR FOR ...` (predefinito) | Non consentita | Limitato a un batch di FETCH |
| `DECLARE c NO SCROLL CURSOR FOR ...` | Non consentita | Limitato a un batch di FETCH |
| `DECLARE c SCROLL CURSOR FOR ...` | Consentita | Risultato completo nel buffer della memoria Python |

Quando non è scritto né `SCROLL` né `NO SCROLL`, il predefinito è `NO SCROLL` — come il predefinito di PostgreSQL. [tool-verified: `server.py:1361-1362`] Un cursore `NO SCROLL` rimuove dal suo buffer interno le righe consumate dopo ogni passo in avanti, così una scansione `FETCH FORWARD` di un grande risultato non tiene mai in memoria Python più di circa un batch alla volta. [tool-verified: `server.py:443-447`] Un cursore `SCROLL` conserva ogni riga che ha recuperato — è il prezzo del supporto a `FETCH BACKWARD`, `ABSOLUTE`, `FIRST` e `LAST`. Prima di dichiarare un cursore `SCROLL` su un grande risultato, mettete in conto l'intera dimensione del risultato come heap Python. [tool-verified: `server.py:383-389`]

Ridichiarare un nome di cursore già aperto chiude prima il cursore precedente. [tool-verified: `server.py:1369-1371`] `WITH HOLD` e `WITHOUT HOLD` sono accettati nella sintassi; vedere la durata del cursore qui sotto.

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
`FETCH` restituisce righe e invia il tag di comando `FETCH <n>`, dove `n` è il numero effettivamente restituito — che può essere minore di quello richiesto quando il cursore raggiunge la fine. `MOVE` avanza la posizione senza restituire righe e invia `MOVE <n>`. [tool-verified: `server.py:1417`, `server.py:1419`]

`FROM` e `IN` sono intercambiabili per introdurre il nome del cursore. I nomi di cursore senza virgolette vengono portati in minuscolo; i nomi tra virgolette (`"CaseSensitive"`) conservano maiuscole e minuscole. [tool-verified: `test_cursor.py:74-79`]

La navigazione all'indietro (`BACKWARD`, `ABSOLUTE`, `FIRST`, `LAST`, `RELATIVE` negativo) su un cursore `NO SCROLL` restituisce SQLSTATE 55000 (`object_not_in_prerequisite_state`), come il rifiuto di PostgreSQL stesso. [tool-verified: `server.py:1402-1406`] Un FETCH o MOVE che nomina un cursore mai dichiarato restituisce SQLSTATE 34000 (`invalid_cursor_name`). [tool-verified: `server.py:1398-1400`]

**CLOSE**

```sql
CLOSE c1;    -- release one cursor and free its result stream
CLOSE ALL;   -- release every open cursor on this connection
```
`CLOSE` rilascia il flusso di risultati sottostante e rimuove il cursore dalla sessione. [tool-verified: `server.py:1421-1436`] Disconnettersi senza emettere `CLOSE` è sicuro: `ProvisaSession.close()` rilascia automaticamente tutti i cursori ancora aperti alla disconnessione. [tool-verified: `test_cursor.py:335-346`]

**Durata del cursore.** Il server pgwire di Provisa non ha una macchina a stati delle transazioni (`ProvisaSession.in_transaction()` è fisso a `False`). Ogni cursore si comporta quindi come se fosse dichiarato `WITH HOLD`: vive per tutta la connessione, non fino a `COMMIT`. [tool-verified: `server.py:200` — `in_transaction()` always returns `False`] Le parole chiave `WITH HOLD` e `WITHOUT HOLD` sono accettate e ignorate senza avvisi — non c'è un ciclo di vita dei cursori per transazione da applicare.

### Definizioni (CREATE, ALTER, DROP) {: #definitions-create-alter-drop }

Nulla viene definito tramite pgwire. Un'istruzione che crea, modifica o elimina un oggetto (`CREATE TABLE`, `CREATE TABLE … AS SELECT`, `CREATE VIEW`, `ALTER`, `DROP`, un indice, una sequenza o uno schema) viene rifiutata con SQLSTATE 0A000, qualunque cosa detenga il ruolo; l'eccezione è la tabella temporanea propria di una sessione (vedere [Tabelle temporanee](#temporary-tables)). Una tabella o una vista è un oggetto del modello: createla nel modello (pagine di amministrazione, API di amministrazione o configurazione), oppure createla nella sorgente dati e ammettetela nel modello. `TRUNCATE` non definisce nulla: è una scrittura di dati (vedere [Scritture di dati](#data-writes-insert-update-delete-merge-copy-from-stdin)). [tool-verified: `provisa/compiler/definitions.py`]

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

Le stesse istruzioni funzionano su ogni superficie SQL: sono gestite nella pipeline che tutte condividono. [inferred: verified by `tests/integration/test_session_temp_tables.py` over HTTP; the pgwire connection-long session is not yet covered by a test]

### Scritture di dati (INSERT, UPDATE, DELETE, MERGE, COPY FROM STDIN) {: #data-writes-insert-update-delete-merge-copy-from-stdin }

Le righe di una tabella registrata esistente si scrivono tramite pgwire come su ogni altra superficie SQL, attraverso l'unica ammissione delle scritture (`provisa/compiler/write_admission.py`):

1. **Il diritto.** Il ruolo detiene `write`. (REQ-868)
2. **Le colonne.** Ogni colonna che l'istruzione scrive nomina il ruolo nel suo `writable_by`; una colonna che non ne dichiara nessuno non è scrivibile da nessuno. (REQ-663)
3. **Le righe che può toccare.** Il filtro di riga del ruolo, lo stesso predicato che portano le sue letture, viene aggiunto al `WHERE` di un `UPDATE` o `DELETE`.
4. **Le righe che lascia.** Un ruolo filtrato può scrivere solo righe che potrebbe poi leggere: le righe di un `INSERT`, e i nuovi valori di un `UPDATE` che imposta una colonna letta dal filtro, vengono verificati rispetto al filtro prima che qualcosa venga inviato alla sorgente.

Anche `TRUNCATE` è una scrittura di dati, eseguita come tale e mai riscritta in un `DELETE`. Richiede il diritto `write`, tutte le colonne della tabella scrivibili dal ruolo e una sorgente che accetti le eliminazioni. Svuota una tabella alla volta, e viene rifiutata a un ruolo con un filtro di righe sulla tabella, perché un `TRUNCATE` non può applicare il filtro: usate `DELETE`, che lo fa. (REQ-1942) [tool-verified: `provisa/compiler/write_admission.py` `admit_write`]

Ciò che non si può decidere viene rifiutato per nome anziché ammesso nel dubbio (un `INSERT … SELECT`, un valore che non è né un letterale né un parametro associato, un filtro che legge una colonna che l'istruzione non fornisce). [tool-verified: `provisa/compiler/write_admission.py`, `provisa/pgwire/copy_handler.py`]

### COPY

Sono supportati sia `COPY ... TO STDOUT` sia `COPY ... FROM STDIN`. (REQ-585)

**COPY TO STDOUT** — esporta i risultati delle query nel formato wire COPY di PG. Funzionano due forme:

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```
Formati supportati: `text` (separato da tabulazioni, predefinito), `csv` e `binary`, il formato COPY binario di PostgreSQL, con cui legge l'estensione postgres di DuckDB. (REQ-883) [tool-verified: `provisa/pgwire/copy_handler.py`, `provisa/pgwire/copy_binary.py`]

**COPY FROM STDIN** è una scrittura di dati in una tabella registrata esistente: è ammessa dalla stessa ammissione di scrittura di un `INSERT` delle stesse righe (vedere [Scritture di dati](#data-writes-insert-update-delete-merge-copy-from-stdin)). Legge `text` (predefinito) e `csv`. Qualsiasi altro formato viene rifiutato con SQLSTATE 0A000, nominandolo, prima che venga inviato alcun dato. [tool-verified: `_parse_copy_data`, `provisa/pgwire/copy_handler.py`]

### Transazioni e comandi di sessione {: #transactions-and-session-commands }

SET, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, DISCARD, RESET e DEALLOCATE vengono intercettati e restituiscono una risposta di successo vuota. (REQ-587) Il server è stateless rispetto alle transazioni — non esiste isolamento delle transazioni né supporto per il rollback. (REQ-587) [tool-verified: `catalog.py:27-31`, `catalog.py:1129-1132`]

---

## Intercettazione del catalogo {: #catalog-intercept }

Le query su `information_schema` e `pg_catalog` vengono risolte localmente senza un round-trip verso Trino. (REQ-532) Il livello di intercettazione costruisce un database DuckDB in memoria per ogni richiesta, popolato a partire dal contesto di compilazione del ruolo. (REQ-532) [tool-verified: `catalog.py:210-213`]

Tabelle intercettate:

**information_schema:** `schemata`, `tables`, `columns`, `views`, `table_constraints`, `key_column_usage`, `referential_constraints`

**pg_catalog:** `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_attrdef`, `pg_description`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_roles`, `pg_auth_members`, `pg_database`, `pg_settings`, `pg_tables`, `pg_stat_user_tables`, `pg_statio_user_tables`, `pg_am`, `pg_extension`, `pg_enum`, `pg_stat_activity`

[tool-verified: `catalog.py:39-67`]

`pg_constraint` viene popolata con dati reali di chiave primaria e chiave esterna derivati dai campi `pk_columns` e `joins` del modello di dominio. (REQ-392, REQ-399) Gli strumenti di BI che ispezionano le relazioni di chiave esterna (Tableau, DBeaver, ecc.) vedranno il grafo di join che Provisa conosce. [tool-verified: `catalog.py:551-632`] I join a colonna singola tra la stessa coppia origine/destinazione le cui colonne di destinazione formano insieme la chiave primaria composita della destinazione vengono raggruppati in un'unica riga FK con array `conkey`/`confkey` a più elementi. (REQ-1094) [tool-verified: `catalog_constraints.py`]

Una relazione basata su una tabella di giunzione (junction) (REQ-1586) non produce alcuna riga FK. È un arco attraverso una tabella associativa, non una coppia di colonne, e `pg_constraint` non ha una forma per due salti — perciò il modello di dominio la lascia fuori da `joins` e la tabella di giunzione compare come una tabella ordinaria con le proprie chiavi esterne verso ciascun estremo. I client SQL la raggiungono facendo join con quella tabella; i client Cypher la attraversano come un'unica relazione. [tool-verified: `provisa/compiler/schema_gen.py:302-306`]

`pg_index` viene popolata con una riga per ogni vincolo di chiave primaria e UNIQUE (`indrelid` = oid della tabella, `indkey` = attnum delle chiavi in ordine, `indisprimary`/`indisunique` impostati). I client che risolvono le colonne chiave tramite `pg_index.indkey` anziché tramite `pg_constraint` — DataGrip, ad esempio — individuano le colonne corrette tramite il join standard `pg_index` → `pg_attribute`. (REQ-1095) [tool-verified: `catalog_constraints.py:340-384`]

Vengono intercettate anche le seguenti espressioni scalari: (REQ-588)

- `current_user`, `session_user` → il `role_id` autenticato
- `current_database()` → `"provisa"`
- `current_schema()` → `"public"`
- `version()` → `"PostgreSQL 14.0 on Provisa"`
- `pg_backend_pid()` → `0`
- `current_setting(...)` → restituisce un valore da una tabella di impostazioni fissa
- `SHOW <setting>` → restituisce un valore dalla stessa tabella di impostazioni

[tool-verified: `catalog.py:168-207`, `catalog.py:1076-1120`]

---

## Codifica binaria dei parametri {: #binary-parameter-encoding }

Il protocollo di query estesa (Bind/Execute) supporta parametri codificati in binario. (REQ-589) I seguenti OID di tipo vengono decodificati dal binario: [tool-verified: `postgres.py:69-97`]

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

Qualsiasi OID non presente in questa tabella genera `"Unsupported binary parameter type: <oid>"`. (REQ-589) [tool-verified: `postgres.py:579`]

Anche le colonne dei risultati vengono inviate in binario quando il client lo richiede, per lo stesso insieme di tipi più ARRAY, JSON, INTERVAL e BIGINT. (REQ-589) [tool-verified: `postgres.py:191-244`]

---

## Raccomandazioni sui driver {: #driver-recommendations }

**Driver Python nativi (psycopg2, asyncpg).** Questi negoziano il protocollo di query estesa per impostazione predefinita e utilizzano la codifica binaria per la maggior parte dei tipi. La fedeltà dei tipi qui è massima — le colonne `NUMERIC` arrivano come `Decimal`, `TIMESTAMP` come `datetime`, e così via. Usarli per ETL basati su Python, script o integrazione diretta.

**JDBC (driver JDBC di PostgreSQL).** Usarlo per gli strumenti dell'ecosistema Java: DBeaver, Tableau, Power BI, Metabase, operatori JDBC di Airflow. JDBC utilizza per impostazione predefinita il protocollo di query semplice, che evita complicazioni legate alla codifica binaria. Stringa di connessione:

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

Alcuni strumenti di BI basati su JDBC inviano, alla connessione, una raffica di query verso `information_schema` e `pg_catalog` per popolare il proprio browser di schema. Tutte vengono risolte dal livello di intercettazione del catalogo — non viene generato traffico verso Trino durante l'ispezione dello schema. (REQ-532)

**Quando preferire l'uno o l'altro.** Se il client è in Python, usare psycopg2 o asyncpg per una migliore gestione dei tipi. Se il client è uno strumento di BI o qualsiasi applicazione JVM, usare JDBC. Evitare di mescolare aspettative di protocollo binario e testuale sulla stessa connessione se si osservano anomalie nella conversione dei tipi — il comportamento in modalità testo di JDBC è più semplice da interpretare.

---

## Avvertenze e vincoli {: #caveats-and-constraints }

**Solo SQL.** Il listener pgwire analizza ed esegue solo SQL — le stringhe GraphQL e Cypher non sono accettate. (REQ-614) Le scritture di dati in tabelle esistenti vengono ammesse in base ai diritti del ruolo, alle sue concessioni di colonna e al suo filtro di riga; vedere [Scritture di dati](#data-writes-insert-update-delete-merge-copy-from-stdin).

**Nessuna definizione.** `CREATE`, `ALTER` e `DROP` vengono rifiutati con SQLSTATE 0A000 qualunque cosa detenga il ruolo, tranne la tabella temporanea propria di una sessione; vedere [Tabelle temporanee](#temporary-tables). `TRUNCATE` è una scrittura di dati. (REQ-615, REQ-616)

**Nessun supporto reale alle transazioni.** BEGIN/COMMIT/ROLLBACK vengono accettati e ignorati silenziosamente. Ogni istruzione viene eseguita in modo indipendente. (REQ-587) [tool-verified: `server.py:146-158` — `in_transaction()` restituisce sempre `False`]

**Timeout delle query.** Un'istruzione può usare il timeout di richiesta proprio di pgwire, `limits.request_timeouts.pgwire`, o quello condiviso `limits.request_timeout` quando il primo non è impostato; entrambi sono impostazioni dell'operatore. (REQ-590, REQ-1905) [tool-verified: `provisa/core/limits.py`, `server.py`]

**L'input di COPY è text o csv.** `COPY ... TO STDOUT` scrive `text`, `csv` e `binary`; `COPY ... FROM STDIN` legge `text` e `csv`. [tool-verified: `_parse_copy_data`, `provisa/pgwire/copy_handler.py`]

**La sostituzione dei parametri è letterale.** I parametri `$1`, `$2`, ... vengono sostituiti come letterali SQL prima dell'esecuzione, non inviati come parametri di bind al motore sottostante. Ciò significa che il motore sottostante non vede mai un'istruzione preparata. Per Trino ciò non ha alcun impatto pratico; per le origini a pool diretto, elude la cache delle istruzioni preparate. (REQ-581) [tool-verified: `server.py:78-85`]

**DECLARE SCROLL mette in buffer il risultato completo.** Un cursore `SCROLL` accumula nell'heap Python ogni riga che recupera dal motore, così da poter servire `FETCH BACKWARD`, `ABSOLUTE`, `FIRST` e `LAST` su richiesta. Con un grande risultato, l'intero risultato vive in memoria finché il cursore è aperto. Usate il predefinito `NO SCROLL` per lo streaming solo in avanti. (REQ-1862) [tool-verified: `server.py:383-389`]

**`pg_stat_activity`, `pg_stat_user_tables`, `pg_extension`, `pg_enum`, `pg_attrdef`, `pg_proc`.** Queste tabelle esistono nel livello di catalogo ma sono stub vuoti. Gli strumenti di monitoraggio che le interrogano riceveranno zero righe anziché errori. (REQ-532) [tool-verified: `catalog.py:519-535`, `catalog.py:639-934`] (`pg_index` è popolata — vedere Intercettazione del catalogo.)
