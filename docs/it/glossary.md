# Glossario aziendale

Il glossario aziendale è un vocabolario vivo sopra il modello dei dati. Ogni colonna fisica del
livello semantico si risolve in un termine — un unico termine condiviso ogni volta che più colonne
portano lo stesso concetto, per quanto diversamente lo scrivano. Ogni termine può contenere una
definizione, un insieme di relazioni tipizzate verso altri termini e un elenco di esperti in materia
che ne detengono il significato.

Quel vocabolario condiviso è il ponte tra il linguaggio di business e i dati fisici. Un agente AI
che sa che «customer» nomina ogni colonna che porta un identificatore di cliente non deve indovinare
quale sia quella giusta tra `cust_id`, `customerId` e `CUSTOMER_KEY` — si risolvono tutte nello
stesso termine, e il termine porta con sé la definizione.

## Come vengono derivati i termini

Provisa deriva automaticamente un termine da ogni nome di colonna, con una regola di normalizzazione
deterministica (REQ-1387): normalizzazione delle maiuscole, tokenizzazione per separatori e
camelCase, espansione delle abbreviazioni e rimozione dei token proxy finali.

**L'espansione delle abbreviazioni** mappa le sigle aziendali più comuni sulla loro forma estesa:
`cust` → `customer`, `txn` → `transaction`, `qty` → `quantity` e così via. Sia `id` sia `key` si
espandono in `identifier`. La tabella è fissa e prudente — le sigle ambigue come `st`, `min` e `no`
restano come sono scritte anziché rischiare un'ipotesi sbagliata.

**La rimozione dei token proxy** elimina un token finale `identifier`, `code`, `index` o
`reference`. Una colonna chiamata `cust_id` non sta nominando l'identificatore in sé; sta nominando
un cliente attraverso un valore surrogato. Rimuovere il proxy fa atterrare sia `cust_id` sia
`customerId` sul termine `customer`. Vengono rimossi solo i token finali, e mai l'ultimo token
rimasto: una colonna `id` isolata si espande in `identifier` e lì resta.

**La deduplicazione** è il punto. La regola di normalizzazione è deterministica, quindi `cust_id`,
`customerId` e `CUSTOMER_KEY` producono tutti `customer`. Ogni colonna ottiene un ref sull'unico
termine risultante anziché su tre termini separati. La curatela ha così un solo posto in cui
aggiungere la definizione, non tre.

### Frasi generiche

Alcune frasi normalizzate sono troppo generiche per essere un concetto a sé stante. Una colonna
`name`, `date` o `identifier` isolata nomina un attributo del concetto della propria tabella, non un
concetto indipendente da quella tabella. I dipendenti hanno un nome; i prodotti hanno un nome; non
sono la stessa cosa.

Quando una frase rientra nell'insieme generico ed è disponibile un contesto di tabella, il termine
viene qualificato in `<concetto della tabella> <frase>`: `employees.first_name` si normalizza in
`employee first name` e `orders.id` si normalizza in `order`, perché la rimozione del proxy fa poi
collassare la frase qualificata sul concetto che essa identifica. Quest'ultimo caso è importante: la
chiave primaria di `orders` e ogni chiave esterna `order_id` sulle altre tabelle atterrano tutte su
`order`, senza bisogno di ulteriore curatela.

L'insieme generico copre i nomi di attributo (`name`, `date`, `status`, `type`, `amount`,
`quantity`), le frasi della traccia di audit (`created_at`, `modified_by`, `submitted_timestamp`) e
una manciata di altre che compaiono su quasi ogni tabella.

### Il nome di business, non il nome fisico

Un termine derivato segue il **nome di business** della colonna — il suo alias quando chi modella ne
ha impostato uno, il suo nome fisico quando non l'ha fatto (REQ-1581). Quando `usr_nm` ha come alias
`user name`, il termine derivato è `user name`, non `user number` né una qualche espansione di
`usr_nm`.

Assegnare un alias a una colonna è la correzione più forte. Un alias viaggia verso ogni superficie
che legge la colonna — SQL, GraphQL, agenti AI, il catalogo — così il modello si descrive
correttamente ovunque. Rinominare un termine sistema una sola voce di catalogo e lascia la colonna
che si legge `usr_nm` a chi verrà dopo. Il banner del termine proposto nella UI lo dice
esplicitamente: prima assegnare un alias alla colonna; rinominare il termine solo quando il nome
della colonna è corretto e il vocabolario no.

Riassegnare l'alias di una colonna ne rideriva il termine proposto, così il glossario segue il
modello anziché chiedere due volte la stessa correzione. Una volta che un curatore ha aggiunto una
definizione, una relazione o un esperto a un termine, una modifica dell'alias non sposta il ref —
quel lavoro appartiene al curatore, e resta.

### Nomi di tabella che descrivono un percorso di accesso

Alcuni nomi di tabella descrivono un percorso di accesso anziché un concetto: `user_by_name` è un
utente raggiunto attraverso una ricerca per nome, non un tipo di entità distinto. Quando Provisa
deriva il concetto della tabella per qualificare le frasi generiche, taglia il nome in
corrispondenza del connettivo (REQ-1582). `user_by_name` diventa `user`; `orders_by_customer`
diventa `order`.

Senza il taglio, la chiave surrogata di `user_by_name` si normalizzerebbe in `user name` e
colliderebbe con il vero attributo `users.name` — un unico termine che contiene una cosa e uno dei
suoi stessi campi. Il taglio si applica solo ai concetti di tabella. In un nome di colonna, `by` fa
parte del nome composto: `pet_by_name` e `pet_name` si normalizzano nello stesso termine, `pet
name`.

## Che cosa rende curato un termine

Un termine nato dalla normalizzazione di una colonna nasce vuoto — una proposta, non ancora
vocabolario. Diventa curato quando è vera una qualsiasi di queste condizioni:

- È stata salvata una definizione.
- È stato aggiunto un arco di relazione.
- È stato assegnato un esperto in materia.
- Un curatore lo ha ritirato manualmente.

La curatela conta per il ciclo di vita del termine. Quando l'ultima colonna fisica di un termine
curato viene rimossa dal modello, il termine viene deprecato anziché eliminato: esce dal servizio,
conserva il contenuto fornito da chi lo ha redatto e viene ripristinato automaticamente se la stessa
colonna ricompare. Un termine non curato rimasto senza colonne viene semplicemente rimosso.

## Risincronizzazione dalle tabelle

Ogni volta che una tabella viene salvata o ricaricata, `sync_table_refs` riconcilia le colonne di
quella tabella con i ref esistenti. Le colonne nuove creano o collegano termini; le colonne
scomparse perdono i propri ref; e la regola rimuovi-o-deprecalo definisce ogni termine che perde
l'ultimo ref.

La riderivazione avviene solo per i termini non curati. Se è stato assegnato un alias a una colonna
e il termine proposto ora è diverso, il ref si sposta sul nuovo termine. Se il termine è curato, il
collegamento resta — la modifica dell'alias non ha scavalcato la scelta del termine fatta dal
curatore.

Un termine astratto il cui unico percorso verso i dati fisici passava attraverso un termine in
uscita viene deprecato anziché rimosso, preservando la struttura concettuale fino a quando non viene
ricollegata.

## Relazioni

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

## Termini astratti

Un termine astratto non ha ref propri verso colonne fisiche. Se ne usa uno per un concetto di
business che copre più termini concreti — un ombrello che poi si collega ai termini specifici che
detengono davvero le colonne. `revenue`, per esempio, potrebbe essere astratto, con archi `PART_OF` che
puntano a esso da `order amount`, `adjustment amount` e `refund amount`.

Un termine astratto che non riesce a raggiungere alcuna colonna fisica attraverso il grafo delle
relazioni è una proposta sospesa. Non compare nella ricerca di termini degli agenti né
nell'esportazione dei metadati — un termine che non nomina alcun dato non può rispondere a nulla.

## La regola di ammissione per le superfici consumatrici

Un termine che una superficie consumatrice può offrire deve soddisfare tre condizioni (REQ-1387):

1. **In servizio** — non ritirato (un curatore lo ha tolto dal servizio) e non deprecato (ha perso
   l'ultima colonna ed è stato mantenuto solo perché eliminarlo avrebbe lasciato qualcosa in
   sospeso).
2. **Definito** — porta con sé una definizione. Un termine derivato da un nome di colonna è un
   token, non un significato. Senza una definizione è una proposta in attesa di un curatore, mai
   vocabolario su cui un agente possa ancorare una domanda.
3. **Ancorato** — collegato, attraverso termini in servizio, ad almeno un termine che detiene un ref
   verso una colonna fisica. Il glossario è un punto d'ingresso nei dati, quindi ogni catena deve
   terminare su una colonna.

La connettività si propaga nel grafo: un termine astratto raggiunge i dati attraverso qualsiasi
vicino in servizio che li raggiunga. I termini fuori servizio non conducono — un termine ritirato
non tiene in vita chi dipende da lui.

## Esportazione dei metadati

Il glossario viene pubblicato verso cataloghi dati esterni nell'ambito dell'esportazione dei
metadati. Si applica la stessa regola di ammissione, con un restringimento: il radicamento di un
termine è valutato solo rispetto alle colonne che vengono effettivamente pubblicate. Un termine le
cui colonne sono tutte escluse dall'esportazione — perché le loro tabelle non sono contrassegnate
come prodotti dati, oppure perché filtri tecnici le escludono — non è radicato ai fini
dell'esportazione, anche se detiene ref nel control plane.

Gli archi di relazione vengono pubblicati solo quando entrambi i termini agli estremi vengono
pubblicati.

Gli asset di colonna vengono esportati in modo indipendente. L'esclusione di un termine non nasconde
i dati sottostanti.

### Escludere un termine dall'esportazione

Alcune colonne trasportano dettagli tecnici anziché dati di business: identificatori di batch ETL,
versioni di riga, timestamp di acquisizione. Un termine derivato da una colonna del genere può avere
una definizione perfettamente accurata che semplicemente non è vocabolario di business (REQ-1583).
Il controllo **Escludi dall'esportazione dei metadati** trattiene il termine, e ogni arco di
relazione che termina su di esso, dai cataloghi verso cui Provisa pubblica, mentre le colonne stesse
continuano a essere esportate come asset.

Il criterio è se il business usi questa parola, non se la definizione sia buona. Un identificatore
di batch ETL ha un significato chiaro che nel glossario è utile a chi fa ingegneria; non ha posto in
un catalogo di business accanto a `customer` e `revenue`.

## Lavorare con il glossario

Aprire **Admin → Glossario** nella UI. Il pannello di sinistra elenca ogni termine; fare clic su uno
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
- **Generare definizioni in blocco** per riempire ogni definizione vuota in un'unica passata.
  Vengono scritte solo le definizioni vuote; il testo umano non viene mai sovrascritto.
- **Generare relazioni in blocco** per proporre archi tipizzati sull'intero elenco dei termini. Le
  proposte malformate — nomi di termine sconosciuti, archi verso se stessi, tipi non riconosciuti —
  vengono scartate automaticamente.

Il banner **Proposto** su un termine privo di definizione indica se il termine è indefinito
(assegnare un alias alla colonna oppure aggiungere una definizione) o non ancorato (metterlo in
relazione con un termine che ha colonne). Quando lo si vede, il termine non è ancora raggiungibile
da agenti o cataloghi.

## Vedi anche

- [Esportazione dei metadati](metadata-export.md) — come termini e relazioni vengono pubblicati
  verso cataloghi dati esterni, inclusi i termini che la regola di ammissione all'esportazione
  ammette.
- [Derivazione a livello di colonna](lineage.md) — l'esploratore della derivazione e come
  `columnDependents` riporta i collegamenti del glossario come dipendenti di una colonna fisica.
