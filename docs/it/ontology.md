# Ontologia {: #ontology }

L'ontologia è un vocabolario vivo sopra il vostro modello di dati. Ogni colonna fisica corrisponde a un termine, e le colonne con lo stesso significato condividono un solo termine, comunque lo scrivano. Ogni termine può avere una definizione, relazioni tipizzate con altri termini, un elenco di esperti di dominio e tag come `pii` che ogni colonna con quel significato porta con sé.

È un grafo di concetti, delle loro definizioni e delle relazioni tra loro. Provisa lo collega alle tabelle e alle colonne che contengono i valori, ed è questo che gli permette di rispondere a domande sui vostri dati. Un termine è una bozza finché non ha una definizione e non raggiunge i dati. L'ontologia è separata dalle metriche: un termine dice che cos'è un concetto, una metrica dice come si calcola un numero.

Questo vocabolario condiviso collega il linguaggio del business ai dati fisici. Un agente IA che sa che «customer» indica ogni colonna con un identificativo cliente non deve indovinare quale tra `cust_id`, `customerId` e `CUSTOMER_KEY` sia quella giusta. Corrispondono tutte allo stesso termine, e il termine porta la definizione.

## Classi, proprietà e concetti {: #classes-properties-and-concepts }

Ogni termine ha esattamente un ruolo, derivato da ciò a cui è ancorato. Non lo impostate mai voi.

| Ruolo | Ancorato a | Esempio |
| --- | --- | --- |
| **Classe** | Una o più tabelle o viste. Le sue istanze sono le loro righe. | `customer`, ancorato a `customers` e `crm_contacts` |
| **Proprietà** | Colonne che non sono chiavi. | `customer email`, con `customers.email` |
| **Concetto** | Niente direttamente. Raggiunge i dati tramite le sue relazioni. | `revenue`, con archi `PART_OF` da `order amount` e `refund amount` |

Un termine è l'uno o l'altro. Provisa rifiuta di ancorare una tabella a un termine che ha colonne non chiave, e rifiuta di spostare una colonna non chiave su un termine ancorato a tabelle. Ogni rifiuto nomina il termine e ciò che già contiene.

### Ogni tabella con chiave propone una Classe {: #every-keyed-table-proposes-a-class }

Quando si salva una tabella con chiave primaria, Provisa la ancora a un termine Classe in bozza, chiamato come il concetto della tabella: `orders` propone `order`, `order_lines` propone `order line`. Le tabelle dello stesso concetto condividono una Classe. Una tabella senza chiave primaria non può essere una Classe, e il suo ancoraggio viene rifiutato nominando la tabella.

Un ancoraggio fatto da voi, nell'interfaccia, nella configurazione o tramite Polly, è curato. La sincronizzazione non lo sposta mai, e ancorare una tabella a un termine curato sostituisce la sua proposta in bozza. Quando un concetto è già una Proprietà con colonne non chiave, le sue tabelle non ricevono proposte; l'ontologia le elenca invece in **Da rivedere**.

### Le chiavi appartengono alla Classe {: #keys-belong-to-the-class }

Una chiave primaria non ha un termine proprio. Ogni sua colonna, comprese le parti di una chiave composta, mette il proprio riferimento sulla Classe della sua tabella.

Una chiave esterna funziona allo stesso modo. Ogni colonna del lato «molti» di una relazione dichiarata mette il proprio riferimento sulla Classe della tabella di destinazione, comunque si chiami. `manager_id` su `employees`, che punta di nuovo a `employees`, finisce su `employee`; il ruolo «manager» vive nel nome della relazione. Una colonna senza relazione dichiarata è una Proprietà, anche quando il suo nome corrisponde a una Classe.

Provisa colloca i riferimenti delle chiavi solo per derivazione, ogni volta che una tabella si sincronizza, una relazione cambia o cambia la Classe di una tabella. Non potete spostare a mano una colonna chiave; il rifiuto nomina la colonna, la sua tabella e la sua Classe. Ancorate una tabella a un altro termine e le sue chiavi la seguono subito.

### La stessa entità in più tabelle {: #the-same-entity-in-several-tables }

Le istanze di tabelle diverse sono distinte finché non dite il contrario. Quando `customers` e `crm_contacts` descrivono lo stesso cliente, segnate come **stessa entità** la relazione che le unisce. Deve unire due tabelle ancorate alla stessa Classe, uno a uno, su una chiave di ciascuna: la chiave primaria o un vincolo di unicità dichiarato.

Finché una relazione di stessa entità esiste su una tabella, la sua Classe non può essere ritirata né sostituita. Togliete prima il segno alla relazione.

Una Classe ancorata a più tabelle che le relazioni di stessa entità non uniscono tutte viene segnalata in **Da rivedere** come *tabelle non collegate*.

## Come vengono derivati i termini {: #how-terms-are-derived }

Provisa crea automaticamente un termine in bozza da ogni nome di colonna, con una regola di normalizzazione deterministica: uniformazione delle maiuscole, suddivisione su separatori e camelCase, espansione delle abbreviazioni e rimozione dei token proxy finali.

L'**espansione delle abbreviazioni** riporta le abbreviazioni aziendali comuni alla forma estesa: `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity` e così via. Sia `id` sia `key` si espandono in `identifier`. La tabella è fissa e prudente. Le abbreviazioni ambigue come `st`, `min` e `no` restano come sono, così nulla viene indovinato male.

La **rimozione dei token proxy** toglie un token finale `identifier`, `code`, `index` o `reference`. Una colonna chiamata `cust_id` non nomina l'identificativo in sé; nomina un cliente tramite un valore surrogato. Togliere il proxy fa finire sia `cust_id` sia `customerId` sul termine `customer`. Si tolgono solo i token finali, e mai l'ultimo rimasto, quindi una colonna `id` da sola si espande in `identifier` e resta lì.

La **deduplicazione** è lo scopo. La regola è deterministica, quindi `cust_id`, `customerId` e `CUSTOMER_KEY` producono tutti `customer`. Ogni colonna riceve un riferimento a quell'unico termine, e la curatela ha un solo posto in cui aggiungere la definizione invece di tre.

Le colonne chiave saltano questa regola. I loro riferimenti vanno a una Classe, come descritto sopra.

### Frasi generiche {: #generic-phrases }

Alcune frasi normalizzate sono troppo generiche per essere un concetto a sé. Una colonna `name`, `date` o `identifier` da sola nomina un attributo del concetto della sua tabella, che non esiste al di fuori di quella tabella. I dipendenti hanno un nome e i prodotti hanno un nome, ma non sono la stessa cosa.

Quando una frase rientra nell'insieme generico ed è disponibile un contesto di tabella, il termine si qualifica come `<concetto della tabella> <frase>`: `employees.first_name` si normalizza in `employee first name`.

L'insieme generico copre i sostantivi di attributo (`name`, `date`, `status`, `type`, `amount`, `quantity`), le frasi di audit (`created_at`, `modified_by`, `submitted_timestamp`) e poche altre che compaiono su quasi ogni tabella.

### I termini seguono il nome di business {: #terms-follow-the-business-name }

Un termine derivato segue il **nome di business** della colonna: il suo alias quando il modellatore ne ha impostato uno, altrimenti il nome fisico. Quando `usr_nm` ha l'alias `user name`, il termine derivato è `user name`, dove espandere `usr_nm` avrebbe potuto dare `user number`.

Dare un alias a una colonna è la correzione più forte. Un alias arriva a ogni superficie che legge la colonna (SQL, GraphQL, agenti IA, il catalogo), quindi il modello si descrive correttamente ovunque. Rinominare il termine corregge una voce di catalogo e lascia la colonna con il nome `usr_nm` per il lettore successivo. Il banner Bozza dell'interfaccia lo dice: date prima un alias alla colonna, e rinominate il termine solo quando il nome della colonna è giusto e il vocabolario no.

Cambiare l'alias di una colonna ne riderive il termine in bozza, così l'ontologia segue il modello e non chiede mai due volte la stessa correzione. Una volta che un curatore ha aggiunto a un termine una definizione, una relazione, un esperto o un tag, una modifica dell'alias lascia la colonna dov'è, perché quel lavoro è del curatore e resta.

### Nomi di tabella che indicano un percorso di accesso {: #access-path-table-names }

Alcuni nomi di tabella descrivono un percorso di accesso invece di un concetto: `user_by_name` è un utente raggiunto tramite una ricerca per nome, e non un tipo di entità a sé. Quando Provisa deriva il concetto di una tabella, taglia il nome al connettivo, quindi `user_by_name` diventa `user` e `orders_by_customer` diventa `order`.

Senza il taglio, `user_by_name` proporrebbe una Classe `user name` in conflitto con l'attributo reale `users.name`. Il taglio vale solo per i concetti di tabella. In un nome di colonna, `by` fa parte del sostantivo composto, quindi `pet_by_name` e `pet_name` si normalizzano nello stesso termine, `pet name`.

## Stato {: #status }

Un termine ha un solo stato alla volta.

| Stato | Significato | Come ci arriva |
| --- | --- | --- |
| **Bozza** | Non disponibile per agenti, ricerca e cataloghi finché non ha una definizione e non raggiunge i dati. | Ogni termine derivato parte da qui. |
| **Attivo** | Ammesso: offerto ad agenti, ricerca e cataloghi. | Definito, ancorato e in servizio. |
| **Deprecato** | In uscita. | Un curatore depreca un termine attivo, oppure un termine curato perde la sua ultima colonna. |
| **Ritirato** | Fuori servizio, conservato come registro. | Un curatore lo ritira e nomina un successore. |

### La regola di ammissione {: #the-admission-rule }

Un termine è attivo quando soddisfa tre condizioni.

1. **In servizio.** Non è ritirato e non è diventato deprecato perdendo la sua ultima colonna. Un termine che un curatore ha deprecato resta in servizio fino al ritiro previsto.
2. **Definito.** Ha una definizione. Un termine derivato da un nome di colonna è solo un token. Senza definizione è una bozza in attesa di un curatore, mai vocabolario su cui un agente possa basare una domanda.
3. **Ancorato.** È collegato, tramite termini in servizio, a un termine che ha una colonna o una tabella. L'ontologia è un punto d'ingresso nei dati, quindi ogni catena deve terminare nei dati.

La connettività si propaga nel grafo. Un Concetto raggiunge i dati tramite qualsiasi vicino in servizio che li raggiunga. I termini fuori servizio non conducono, quindi un termine ritirato non tiene in vita i suoi dipendenti.

### Deprecare un termine {: #deprecating-a-term }

Deprecate un termine attivo quando è in uscita ma ancora in uso. Indicate un motivo, obbligatorio, e facoltativamente una data di ritiro prevista, oggi o più avanti. Il termine resta in servizio, e continua a valere per la regola di ammissione, fino a quella data. Poi lo scheduler lo ritira.

Un termine in scadenza che ha ancora colonne o tabelle non può essere ritirato per data, perché solo un successore può prenderle. Aspetta, e **Da rivedere** lo elenca come *ritiro bloccato* finché non lo ritirate con un successore.

Un termine curato che perde la sua ultima colonna diventa deprecato automaticamente e viene conservato. Esce dal servizio, mantiene il contenuto fornito dai suoi redattori e torna se la stessa colonna ricompare. Un termine non curato rimasto senza colonne viene rimosso.

### Ritirare un termine {: #retiring-a-term }

Ritirare un termine sposta tutto ciò che ha su un **successore**, poi lo mette fuori servizio.

- Le colonne di una Proprietà passano al successore. Deve essere una Proprietà o un Concetto.
- Le tabelle di una Classe passano al successore, e le loro chiavi le seguono. Deve essere una Classe o un Concetto.
- Un Concetto non ha nulla, quindi il suo successore è facoltativo.

Il ritiro richiede un motivo e scrive un arco `REPLACES` dal successore al termine ritirato. Il termine ritirato resta come registro inerte: definizione, tag, relazioni, esperti e il motivo. Non conduce nulla.

Poiché le sue colonne ora appartengono al successore, il pannello di un termine ritirato mostra la sua **ultima applicazione**: colonne e tabelle come il modello le aveva nel commit precedente al ritiro. Il pannello rimanda anche al termine che lo sostituisce.

### Ripristinare un termine {: #restoring-a-term }

**Ripristina** annulla ciò che un curatore ha dichiarato. Un termine deprecato smette di esserlo. Un termine ritirato torna come Concetto vuoto e in bozza, dato che le sue colonne sono andate al successore, e il suo arco `REPLACES` viene rimosso.

### Che cosa rende curato un termine {: #what-makes-a-term-curated }

Un termine nato dalla normalizzazione delle colonne parte vuoto. Diventa curato quando ha uno di questi elementi:

- una definizione salvata
- un arco di relazione
- un esperto di dominio assegnato
- un tag, come `pii`
- un ritiro

I termini curati vengono conservati quando le loro colonne se ne vanno. La sincronizzazione non riderive mai altrove le colonne di un termine curato.

## Risincronizzare dalle tabelle {: #re-syncing-from-tables }

Ogni volta che una tabella viene salvata o ricaricata, Provisa riconcilia le sue colonne con l'ontologia. Le nuove colonne creano o collegano termini, quelle sparite perdono i riferimenti, e ogni termine rimasto senza riferimenti viene rimosso o deprecato secondo la regola sopra. Le colonne chiave vanno alla loro Classe, e una tabella con chiave senza Classe ne propone una.

Vengono riderivati solo i termini non curati. Se date un alias a una colonna e il suo termine in bozza cambia, la colonna passa al nuovo termine. Se il termine è curato, la colonna resta, perché una modifica dell'alias non prevale sulla scelta di un curatore.

Quando rimuovere un termine lascerebbe un Concetto senza percorso verso i dati, Provisa depreca invece quel termine, così la struttura che avete costruito resta finché non la ricollegate.

## Relazioni {: #relationships }

I termini sono collegati ad altri termini tramite archi tipizzati.

| Tipo | Significato |
| --- | --- |
| `KIND_OF` | Il termine sorgente è un tipo del termine di destinazione. |
| `PART_OF` | Il termine sorgente è un componente del termine di destinazione. |
| `SYNONYM_OF` | I due termini sono intercambiabili in questo dominio. |
| `RELATED_TO` | Un'associazione debole, quando nessuna affermazione più forte si adatta. |
| `VALID_VALUE_OF` | La sorgente è un valore ammesso dell'enumerazione o del dominio di destinazione. |
| `DERIVED_FROM` | La sorgente è calcolata o ricavata dalla destinazione. |
| `REPLACES` | La sorgente sostituisce la destinazione. Ritirare un termine ne scrive uno dal suo successore. |
| `PREFERRED_TERM_FOR` | La sorgente è il termine preferito rispetto alla destinazione sconsigliata. |
| `TRANSLATION_OF` | La sorgente è una traduzione della destinazione in un'altra lingua o impostazione locale. |
| `ANTONYM_OF` | La sorgente è l'opposto semantico della destinazione. |

Le relazioni hanno una direzione. L'interfaccia mostra gli archi in uscita (questo termine → un altro) e quelli in entrata (un altro termine → questo termine), etichettando ciascuna direzione con una propria frase chiara.

Gli archi risiedono in una tabella associativa dichiarata come relazione di giunzione. La sua colonna del tipo di relazione è il discriminatore, quindi ciascun tipo sopra è un tipo di relazione Cypher distinto tra due nodi di termine. Nulla di essa è specifico dell'ontologia: è dichiarata come dichiarereste una giunzione sulle vostre tabelle, e la legge lo stesso codice.

## Concetti {: #concepts }

Un Concetto non ha colonne né tabelle proprie. Usatelo per un'idea di business che abbraccia più termini concreti, poi collegatelo ai termini che hanno dati. `revenue`, per esempio, potrebbe essere un Concetto, con archi `PART_OF` da `order amount`, `adjustment amount` e `refund amount`.

Un Concetto che non raggiunge alcuna colonna o tabella tramite il grafo delle relazioni non nomina dati. Non compare nella ricerca dei termini degli agenti né nell'esportazione dei metadati, e **Da rivedere** lo elenca come *nessun dato*. I domini valgono solo per un Concetto. Ogni altro termine prende i suoi domini dalle tabelle a cui è ancorato o dalle colonne che ha.

## PII secondo il significato {: #pii-on-meanings }

La PII è una proprietà di ciò che una colonna significa, quindi Provisa vi lascia marcarla una volta, sul termine, e copre ogni colonna con quel significato, comprese quelle registrate in seguito.

Il tag `pii` va su una colonna o su un termine Proprietà, mai su una sorgente, una tabella, una Classe o un Concetto. Un termine deve essere definito e in servizio (attivo o deprecato) per portarlo. Marcare un termine in bozza viene rifiutato con «Definisci *termine* per segnarlo», e non potete cancellare la definizione di un termine marcato finché non togliete il segno.

Ogni colonna di un termine marcato porta `pii`, e con esso `sensitive`, ovunque si leggano i tag: governance, mascheramento, sensibilità, esportazione e le viste dei tag. Nulla viene copiato sulle colonne. Una colonna spostata sul termine porta il tag subito, e una colonna spostata altrove non lo porta più. Ogni colonna dice da dove viene il suo tag: impostato sulla colonna, con il motivo, oppure tramite un termine, che nomina.

Quando marcate una colonna il cui termine è una Proprietà, l'interfaccia chiede se marcare anche le altre colonne con lo stesso significato.

- **Sì** mette il tag sul termine.
- **Solo questa** marca solo la colonna e chiede perché.

Una colonna che è PII tramite il suo termine non può essere smarcata da sola. Togliete il segno al termine, oppure spostate la colonna sul termine che corrisponde a ciò che contiene.

### Spostamenti che fanno perdere tag {: #moves-that-lose-tags }

Spostare una colonna su un altro termine può toglierle un tag: spostare `customers.email` da `customer email` (marcato `pii`) a un termine non marcato lascerebbe la colonna dell'email senza protezione. Ogni spostamento fatto da una persona viene verificato prima. Questo riguarda rivendicare o spostare una colonna, ritirare un termine, e salvare o eliminare una relazione che ricolloca colonne chiave.

Quando una colonna smetterebbe del tutto di portare `pii`, lo spostamento viene rifiutato finché non decidete. L'interfaccia mostra un riepilogo di ciò che ogni colonna spostata perde e acquista, prima le perdite.

- **Sposta** procede, con ogni tag perso confermato.
- **Sposta e segna** porta i tag persi sul termine verso cui vanno le colonne, nella stessa modifica.
- **Annulla** non sposta nulla.

Una colonna che mantiene il tag grazie al proprio segno non ha bisogno di conferma. Gli spostamenti che il modello fa da sé, come riderivare le colonne di una bozza, non cambiano mai un tag ereditato.

## Da rivedere {: #to-review }

L'ontologia segnala come rilievi ciò che richiede la vostra attenzione. Un termine con un rilievo è **Da rivedere**, e l'elenco dei termini si filtra su questo. Una semplice bozza non è un rilievo; le bozze sono lo stato normale dell'ontologia.

| Rilievo | Che cosa significa | La correzione |
| --- | --- | --- |
| Tabelle non collegate | Una Classe ancora più tabelle che le relazioni di stessa entità non uniscono tutte. | Segnate relazioni di stessa entità tra loro. |
| Classe e attributo | Una Classe ha colonne che non sono sue chiavi, oppure una tabella con chiave non ha Classe perché il suo concetto è una Proprietà. | Spostate le colonne su una Proprietà, oppure ancorate di nuovo la tabella. |
| Tag sul tipo sbagliato | Un termine che non è una Proprietà porta un tag che solo una Proprietà trasmette alle colonne. | Togliete il tag. |
| Ritiro bloccato | Un termine deprecato ha superato il ritiro previsto ma ha ancora colonne o tabelle. | Ritiratelo con un successore. |
| Nessun dato | Un Concetto in servizio non raggiunge alcun dato. | Collegatelo a un termine che ha dati. |

I rilievi segnalano; non cambiano nulla finché non agite.

## Esportazione dei metadati {: #metadata-export }

L'ontologia viene pubblicata nei cataloghi di dati esterni nell'ambito dell'esportazione dei metadati. Si pubblicano solo i termini attivi, valutati sulle colonne e sulle tabelle effettivamente pubblicate. Un termine le cui colonne sono tutte trattenute dall'esportazione, perché le loro tabelle non sono prodotti di dati o perché i filtri tecnici le escludono, non è ancorato per l'esportazione, anche se ha colonne nel piano di controllo.

Un termine deprecato viene pubblicato con il suo stato, il motivo e la data di ritiro prevista, così ogni catalogo può associarlo al proprio indicatore di deprecazione.

Gli archi di relazione si pubblicano solo quando si pubblicano entrambi i termini alle estremità. Gli asset di colonna si esportano in modo indipendente, quindi escludere un termine non nasconde i dati sottostanti.

### Escludere un termine dall'esportazione {: #excluding-a-term-from-export }

Alcune colonne trasportano dati di servizio, come identificativi di batch ETL, versioni di riga e timestamp di ingestione. Un termine derivato da una colonna così può avere una definizione accurata e non essere comunque vocabolario di business. Il controllo **Escludi dall'esportazione metadati** trattiene il termine, e qualsiasi arco di relazione che termina su di esso, dai cataloghi in cui Provisa pubblica. Le colonne stesse si esportano comunque come asset.

Ciò che conta è se il business usa la parola. Un identificativo di batch ETL ha un significato chiaro che appartiene all'ontologia per gli ingegneri, ma non a un catalogo di business accanto a `customer` e `revenue`.

## Lavorare con l'ontologia {: #working-with-the-ontology }

Aprite **Ontologia** nella barra di navigazione. Il pannello di sinistra elenca ogni termine con il suo ruolo e, se non è attivo, il suo stato. Le caselle **Deprecato** e **Bozza** mostrano o nascondono quei termini, e **Da rivedere** restringe l'elenco ai termini con rilievi. Fate clic su un termine per aprirne il pannello.

Il pannello mostra ruolo e stato del termine e, per una Proprietà, il segno PII con il numero di colonne coperte. Un termine deprecato o ritirato mostra chi ne ha cambiato lo stato e perché. Un termine con rilievi si apre con una sezione **Da rivedere**: fate clic su un rilievo per vedere che cosa è stato trovato e l'azione che lo corregge.

**Modifica** apre l'intero termine come un unico modulo. Salva applica tutte le modifiche insieme.

- **Rinominate** il termine per cambiarne la formulazione senza spostare le sue colonne.
- **Aggiungete una definizione** scrivendola, oppure fate clic sul pulsante di bozza IA per generare un punto di partenza dal nome del termine, dalle sue colonne e dalle sue relazioni. La bozza non viene salvata finché non salvate il modulo.
- **Rivendicate colonne** da altri termini per questo, oppure scegliete **Sposta nel termine** accanto a una delle sue colonne. Ogni colonna appartiene a un solo termine. Le colonne chiave seguono la loro Classe e non si possono spostare.
- **Ancorate tabelle** a una Classe, oppure rimuovetele.
- **Segnate PII** su una Proprietà.
- **Aggiungete una relazione** scegliendone il tipo. Cambiate il tipo di un arco esistente sul posto invece di eliminarlo e ricrearlo.
- **Assegnate esperti** per ID utente, come `expert` o `author`.

Accanto a Modifica, il pannello offre le azioni di stato che il termine consente: **Depreca** un termine attivo, **Ritira** qualsiasi termine in servizio e **Ripristina** un termine deprecato o ritirato.

Nel menu ✨, **Definizioni** riempie ogni definizione vuota in un passaggio e non sovrascrive mai il testo di una persona. **Relazioni** propone archi tipizzati sull'intero elenco dei termini; le proposte che nominano termini sconosciuti, due volte lo stesso termine o un tipo non riconosciuto vengono scartate.

Il banner **Bozza** su un termine vi dice se è non definito (date un alias alla colonna o aggiungete una definizione) o non ancorato (collegatelo a un termine che ha dati). Finché lo vedete, agenti e cataloghi non possono raggiungere il termine.

## Vedi anche {: #see-also }

- [Esportazione dei metadati](metadata-export.md) descrive come termini e relazioni vengono pubblicati nei cataloghi di dati esterni.
- [Lineage a livello di colonna](lineage.md) descrive l'explorer del lineage e come una colonna fisica segnala i termini a essa legati.
