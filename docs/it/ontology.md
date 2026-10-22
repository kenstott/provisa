# Ontologia {: #ontology }

L'ontologia è un vocabolario vivo sopra il vostro modello di dati. Ogni colonna fisica del modello corrisponde a un termine, e le colonne che portano lo stesso concetto condividono un solo termine, comunque lo scrivano. Ogni termine può avere una definizione, un insieme di relazioni tipizzate con altri termini e un elenco di esperti di dominio a cui appartiene il suo significato.

È un grafo di concetti, delle loro definizioni e delle relazioni tra essi. Provisa lo mappa sulle strutture di database che contengono i valori, ed è questo che gli permette di rispondere a domande sui tuoi dati. Un concetto è una bozza finché non raggiunge i dati, direttamente o tramite altri concetti. L'ontologia è distinta dalle metriche: un termine dice che cos'è un concetto, una metrica dice come si calcola un numero.

Quel vocabolario condiviso collega il linguaggio del business ai dati fisici. Un agente di IA che sa che "customer" indica ogni colonna con un identificativo cliente non deve indovinare quale tra `cust_id`, `customerId` e `CUSTOMER_KEY` sia quella giusta. Corrispondono tutte allo stesso termine, ed è il termine a portare la definizione.

## Come vengono derivati i termini {: #how-terms-are-derived }

Provisa deriva automaticamente un termine da ogni nome di colonna, con una regola di normalizzazione
deterministica: normalizzazione delle maiuscole, tokenizzazione per separatori e
camelCase, espansione delle abbreviazioni e rimozione dei token proxy finali.

L'**espansione delle abbreviazioni** porta le abbreviazioni aziendali comuni alla forma completa: `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity` e così via. Sia `id` sia `key` diventano `identifier`. La tabella è fissa e prudente: abbreviazioni ambigue come `st`, `min` e `no` restano come sono, per non tirare a indovinare.

**La rimozione dei token proxy** elimina un token finale `identifier`, `code`, `index` o
`reference`. Una colonna chiamata `cust_id` non sta nominando l'identificatore in sé; sta nominando
un cliente attraverso un valore surrogato. Rimuovere il proxy fa atterrare sia `cust_id` sia
`customerId` sul termine `customer`. Vengono rimossi solo i token finali, e mai l'ultimo token
rimasto: una colonna `id` isolata si espande in `identifier` e lì resta.

La **deduplicazione** è lo scopo. La regola di normalizzazione è deterministica, quindi `cust_id`, `customerId` e `CUSTOMER_KEY` producono tutti `customer`. Ogni colonna riceve un riferimento a quell'unico termine, e la curatela ha un solo posto dove aggiungere la definizione invece di tre.

### Frasi generiche {: #generic-phrases }

Alcune espressioni normalizzate sono troppo generiche per essere un concetto da sole. Una colonna `name`, `date` o `identifier` da sola indica un attributo del concetto della sua tabella, che non esiste separato da quella tabella. I dipendenti hanno un nome, i prodotti hanno un nome, ma non sono la stessa cosa.

Quando una frase rientra nell'insieme generico ed è disponibile un contesto di tabella, il termine
viene qualificato in `<table concept> <phrase>`: `employees.first_name` si normalizza in
`employee first name` e `orders.id` si normalizza in `order`, perché la rimozione del proxy fa poi
collassare la frase qualificata sul concetto che essa identifica. Quest'ultimo caso è importante: la
chiave primaria di `orders` e ogni chiave esterna `order_id` sulle altre tabelle atterrano tutte su
`order`, senza bisogno di ulteriore curatela.

L'insieme generico copre i nomi di attributo (`name`, `date`, `status`, `type`, `amount`,
`quantity`), le frasi della traccia di audit (`created_at`, `modified_by`, `submitted_timestamp`) e
una manciata di altre che compaiono su quasi ogni tabella.

### Il nome di business, non il nome fisico {: #the-business-name-not-the-physical-name }

Un termine derivato segue il **nome di business** della colonna: il suo alias se il modellatore ne ha impostato uno, altrimenti il nome fisico. Quando `usr_nm` ha l'alias `user name`, il termine derivato è `user name`, dove un'espansione di `usr_nm` avrebbe potuto dare `user number`.

Dare un alias alla colonna è la correzione più forte. Un alias arriva a ogni superficie che legge la colonna (SQL, GraphQL, agenti di IA, il catalogo), così il modello si descrive correttamente ovunque. Rinominare il termine corregge una voce del catalogo e lascia la colonna come `usr_nm` per il lettore successivo. Il banner del termine proposto nella UI lo dice: prima date un alias alla colonna, e rinominate il termine solo quando il nome della colonna è giusto e il vocabolario no.

Cambiare l'alias di una colonna rideriva il suo termine proposto, così l'ontologia segue il modello e non chiede mai due volte la stessa correzione. Quando un curatore ha già aggiunto a un termine una definizione, una relazione o un esperto, una modifica dell'alias lascia il riferimento dov'è, perché quel lavoro è del curatore e resta.

### Nomi di tabella che descrivono un percorso di accesso {: #access-path-table-names }

Alcuni nomi di tabella descrivono un percorso di accesso e non un concetto: `user_by_name` è un utente raggiunto tramite una ricerca per nome, non un tipo di entità a sé. Quando Provisa ricava il concetto della tabella per qualificare le espressioni generiche, taglia il nome al connettivo, così `user_by_name` diventa `user` e `orders_by_customer` diventa `order`.

Senza il taglio, la chiave surrogata di `user_by_name` verrebbe normalizzata in `user name` e si scontrerebbe con il vero attributo `users.name`, dando un termine che contiene sia una cosa sia uno dei suoi stessi campi. Il taglio vale solo per i concetti di tabella. In un nome di colonna, `by` fa parte del sostantivo composto, quindi `pet_by_name` e `pet_name` si normalizzano nello stesso termine, `pet name`.

## Che cosa rende curato un termine {: #what-makes-a-term-curated }

Un termine nato dalla normalizzazione delle colonne parte vuoto, come una proposta che non è ancora vocabolario. Diventa curato quando vale una qualsiasi di queste condizioni:

- È stata salvata una definizione.
- È stato aggiunto un arco di relazione.
- È stato assegnato un esperto in materia.
- Un curatore lo ha ritirato manualmente.

La curatela conta per il ciclo di vita del termine. Quando l'ultima colonna fisica di un termine curato esce dal modello, il termine viene deprecato e conservato: esce dal servizio, mantiene il contenuto fornito dai suoi editor e torna automaticamente se la stessa colonna ricompare. Un termine non curato senza più colonne viene semplicemente rimosso.

## Risincronizzazione dalle tabelle {: #re-syncing-from-tables }

Ogni volta che una tabella viene salvata o ricaricata, `sync_table_refs` riconcilia le colonne di
quella tabella con i ref esistenti. Le colonne nuove creano o collegano termini; le colonne
scomparse perdono i propri ref; e la regola rimuovi-o-deprecalo definisce ogni termine che perde
l'ultimo ref.

Vengono ricavati di nuovo solo i termini non curati. Se avete dato un alias a una colonna e ora il termine proposto è diverso, il riferimento passa al nuovo termine. Se il termine è curato, il collegamento resta, perché una modifica dell'alias non prevale sulla scelta del termine fatta dal curatore.

Un termine astratto il cui unico percorso verso i dati fisici passava attraverso un termine in
uscita viene deprecato anziché rimosso, preservando la struttura concettuale fino a quando non viene
ricollegata.

## Relazioni {: #relationships }

I termini si relazionano ad altri termini attraverso archi tipizzati. I tipi di relazione supportati
sono:

| Tipo | Significato |
| --- | --- |
| `KIND_OF` | Il termine di origine è un tipo del termine di destinazione. |
| `PART_OF` | Il termine di origine è un componente del termine di destinazione. |
| `SYNONYM_OF` | I due termini sono intercambiabili in questo dominio. |
| `RELATED_TO` | Un'associazione debole — nessun'altra affermazione più forte è adatta. |
| `VALID_VALUE_OF` | L'origine è un valore ammesso dell'enumerazione o del dominio di destinazione. |
| `DERIVED_FROM` | L'origine è calcolata o proviene dalla destinazione. |
| `REPLACES` | L'origine sostituisce la destinazione deprecata. |
| `PREFERRED_TERM_FOR` | L'origine è il termine preferito rispetto alla destinazione sconsigliata. |
| `TRANSLATION_OF` | L'origine è la traduzione della destinazione in una lingua o in un locale. |
| `ANTONYM_OF` | L'origine è l'opposto semantico della destinazione. |

Le relazioni sono direzionali. La UI mostra sia gli archi in uscita (questo termine → un altro) sia
quelli in entrata (un altro termine → questo termine), etichettando ogni direzione con una frase in
linguaggio corrente.

Gli archi vivono in `glossary_term_edges`, una tabella associativa dichiarata come relazione di giunzione (junction): la sua colonna `rel_type` è il discriminatore, quindi ciascuno dei tipi qui sopra è un tipo di relazione Cypher distinto fra due nodi `GlossaryTerm`, non una proprietà su un nodo reificato. La tabella viene provisionata con il resto dello schema di metadati e non è mostrata come nodo nei client grafo: è l'arco. Nulla in essa è specifico dell'ontologia: si dichiara esattamente come dichiareresti una giunzione sulle tue tabelle, ed è letta dallo stesso codice.

## Termini astratti {: #abstract-terms }

Un termine astratto non ha ref propri verso colonne fisiche. Se ne usa uno per un concetto di business che copre più termini concreti: un ombrello che poi si collega ai termini specifici che detengono davvero le colonne. `revenue`, per esempio, potrebbe essere astratto, con archi `PART_OF` che puntano a esso da `order amount`, `adjustment amount` e `refund amount`.

Un termine astratto che non può raggiungere alcuna colonna fisica attraverso il grafo delle relazioni è una proposta sospesa. Non compare nella ricerca di termini degli agenti né nell'esportazione dei metadati, perché un termine che non nomina dati non può rispondere a nulla.

## La regola di ammissione per le superfici consumatrici {: #the-admission-rule-for-consuming-surfaces }

Un termine che una superficie consumatrice può offrire deve soddisfare tre condizioni:

1. **In servizio** — non ritirato (un curatore lo ha tolto dal servizio) e non deprecato (ha perso
   l'ultima colonna ed è stato mantenuto solo perché eliminarlo avrebbe lasciato qualcosa in
   sospeso).
2. **Definito** — porta con sé una definizione. Un termine derivato da un nome di colonna è un
   token, non un significato. Senza una definizione è una proposta in attesa di un curatore, mai
   vocabolario su cui un agente possa ancorare una domanda.
3. **Ancorato** — collegato, attraverso termini in servizio, ad almeno un termine che detiene un ref
   verso una colonna fisica. L'ontologia è un punto d'ingresso nei dati, quindi ogni catena deve
   terminare su una colonna.

La connettività si propaga nel grafo: un termine astratto raggiunge i dati attraverso qualsiasi vicino in servizio che li raggiunga. I termini fuori servizio non trasmettono, quindi un termine ritirato non tiene in vita i suoi dipendenti.

## Esportazione dei metadati {: #metadata-export }

L'ontologia viene pubblicata nei cataloghi di dati esterni come parte dell'esportazione dei metadati. Vale la stessa regola di ammissione, con una restrizione: se un termine è radicato si giudica solo sulle colonne che vengono davvero pubblicate. Un termine le cui colonne sono tutte escluse dall'esportazione, perché le loro tabelle non sono marcate come data product o perché filtri tecnici le escludono, non è radicato ai fini dell'esportazione, anche se ha riferimenti nel piano di controllo.

Gli archi di relazione vengono pubblicati solo quando entrambi i termini agli estremi vengono
pubblicati.

Gli asset di colonna vengono esportati in modo indipendente. L'esclusione di un termine non nasconde
i dati sottostanti.

### Escludere un termine dall'esportazione {: #excluding-a-term-from-export }

Alcune colonne trasportano dettagli tecnici anziché dati di business: identificatori di batch ETL,
versioni di riga, timestamp di acquisizione. Un termine derivato da una colonna del genere può avere
una definizione perfettamente accurata che semplicemente non è vocabolario di business.
Il controllo **Escludi dall'esportazione dei metadati** trattiene il termine, e ogni arco di
relazione che termina su di esso, dai cataloghi verso cui Provisa pubblica, mentre le colonne stesse
continuano a essere esportate come asset.

Conta se il business usa la parola; quanto sia buona la definizione non lo decide. Un identificativo di batch ETL ha un significato chiaro che appartiene all'ontologia per gli ingegneri, ma non a un catalogo di business accanto a `customer` e `revenue`.

## Lavorare con l'ontologia {: #working-with-the-ontology }

Aprire **Ontologia** nella barra di navigazione della UI. Il pannello di sinistra elenca ogni termine; fare clic su uno
per aprirne la vista di dettaglio. Da lì:

- **Rinominare** il termine per cambiarne la formulazione senza spostarne le colonne.
- **Aggiungere una definizione** scrivendola oppure facendo clic sul pulsante di bozza AI per
  generare un punto di partenza dal nome del termine, dalle sue colonne fisiche e dalle sue
  relazioni. La bozza non viene salvata fino alla conferma.
- **Spostare un ref** per consolidare due termini: scegliere il termine di destinazione dal menu a
  discesa accanto a un ref fisico qualsiasi. Se il termine di origine perde l'ultimo ref, viene
  definito automaticamente secondo la regola rimuovi-o-deprecalo.
- **Aggiungere una relazione** tra questo termine e un altro, scegliendo il tipo dall'insieme
  chiuso. Ritipizzare sul posto un arco esistente anziché eliminarlo e riaggiungerlo.
- **Assegnare esperti** tramite ID utente, con tipo `expert` o `author`.
- **Ritirare** un termine per toglierlo dal servizio. Conserva le proprie colonne e resta
  modificabile qui, ma la ricerca di termini degli agenti e l'esportazione dei metadati lo saltano
  entrambe. Può essere ripristinato in seguito se il concetto ritorna.
- **Generare definizioni in blocco** (**Definizioni** nel menu ✨) per riempire ogni definizione vuota in un'unica passata.
  Vengono scritte solo le definizioni vuote; il testo umano non viene mai sovrascritto.
- **Generare relazioni in blocco** (**Relazioni** nel menu ✨) per proporre archi tipizzati sull'intero elenco dei termini. Le
  proposte malformate — nomi di termine sconosciuti, archi verso se stessi, tipi non riconosciuti —
  vengono scartate automaticamente.

Il banner **Proposto** su un termine privo di definizione indica se il termine è indefinito
(assegnare un alias alla colonna oppure aggiungere una definizione) o non ancorato (metterlo in
relazione con un termine che ha colonne). Quando lo si vede, il termine non è ancora raggiungibile
da agenti o cataloghi.

## Vedi anche {: #see-also }

- [Esportazione dei metadati](metadata-export.md) — come termini e relazioni vengono pubblicati
  verso cataloghi dati esterni, inclusi i termini che la regola di ammissione all'esportazione
  ammette.
- [Derivazione a livello di colonna](lineage.md) — l'esploratore della derivazione e come
  `columnDependents` riporta i collegamenti dell'ontologia come dipendenti di una colonna fisica.
