# Ontologia {: #ontology }

L'ontologia è un vocabolario vivo sopra il vostro modello di dati. Ogni colonna fisica si risolve in un termine, e le colonne con lo stesso significato condividono un termine, comunque siano scritte. Ogni termine può contenere una definizione, relazioni tipizzate verso altri termini, un elenco di esperti della materia e tag come `pii` che ogni colonna con quel significato porta con sé.

È un grafo di concetti, delle loro definizioni e delle relazioni tra loro. Provisa lo associa alle tabelle e alle colonne che contengono i valori, ed è questo che gli permette di rispondere a domande sui vostri dati. Un termine è una bozza finché uno steward non lo approva, e se raggiunge dati si ricava dal grafo. L'ontologia è separata dalle metriche: un termine dice che cos'è un concetto, una metrica dice come si calcola un numero.

Quel vocabolario condiviso collega il linguaggio del business ai dati fisici. Un agente IA che sa che «customer» indica ogni colonna con un identificativo cliente non deve indovinare quale tra `cust_id`, `customerId` e `CUSTOMER_KEY` sia quella giusta. Si risolvono tutte nello stesso termine, e il termine porta la definizione.

## Classi, proprietà e concetti {: #classes-properties-and-concepts }

Ogni termine ha esattamente un ruolo, ricavato da ciò che ancora. Non lo impostate mai voi.

| Ruolo | Ancorato in | Esempio |
| --- | --- | --- |
| **Classe** | Una o più tabelle o viste. Le sue istanze sono le loro righe. | `customer`, che ancora `customers` e `crm_contacts` |
| **Proprietà** | Colonne che non sono chiavi. | `customer email`, che contiene `customers.email` |
| **Concetto** | Niente direttamente. Raggiunge i dati tramite le sue relazioni. | `revenue`, con archi `PART_OF` da `order amount` e `refund amount` |

Un termine è l'uno o l'altro. Provisa rifiuta di ancorare una tabella su un termine che contiene colonne non chiave, e rifiuta di spostare una colonna non chiave su un termine ancorato in tabelle. Ogni rifiuto nomina il termine e ciò che contiene già.

### Ogni tabella con chiave propone una classe {: #every-keyed-table-proposes-a-class }

Quando si salva una tabella con chiave primaria, Provisa la ancora in un termine di classe in bozza, chiamato come il concetto della tabella: `orders` propone `order`, `order_lines` propone `order line`. Le tabelle dello stesso concetto condividono una classe. Una tabella senza chiave primaria non può essere una classe, e il suo ancoraggio viene rifiutato nominando la tabella.

Un ancoraggio che fate voi stessi, nella UI, nella configurazione o tramite Polly, è curato. La sincronizzazione non lo sposta mai, e ancorare una tabella su un termine curato ne sostituisce la proposta in bozza. Quando un concetto è già una proprietà con colonne non chiave, le sue tabelle non ricevono proposte; l'ontologia le elenca invece in **Da rivedere**.

### Le chiavi appartengono alla classe {: #keys-belong-to-the-class }

Una chiave primaria non ha un termine proprio. Ognuna delle sue colonne, comprese le parti di una chiave composta, mette il proprio riferimento sulla classe della sua tabella.

Una chiave esterna funziona allo stesso modo. Ogni colonna del lato «molti» di una relazione dichiarata mette il proprio riferimento sulla classe della tabella di destinazione, comunque si chiami la colonna. `manager_id` su `employees`, che punta di nuovo a `employees`, finisce su `employee`; il ruolo «manager» sta nel nome della relazione. Una colonna senza relazione dichiarata è una proprietà, anche quando il suo nome coincide con una classe.

Provisa colloca i riferimenti di chiave solo per derivazione, ogni volta che una tabella si sincronizza, una relazione cambia o cambia la classe di una tabella. Non potete spostare una colonna di chiave a mano, e il rifiuto nomina la colonna, la sua tabella e la sua classe. Ancorate una tabella su un altro termine e le sue chiavi seguono subito.

### La stessa entità in più tabelle {: #the-same-entity-in-several-tables }

Le istanze di tabelle diverse sono distinte finché non dite altrimenti. Quando `customers` e `crm_contacts` descrivono entrambe uno stesso cliente, marcate la relazione che le unisce come **stessa entità**. Deve unire due tabelle che la stessa classe ancora, uno a uno, su una chiave di ciascuna: la chiave primaria o un vincolo di unicità dichiarato.

Finché su una tabella esiste una relazione di stessa entità, la sua classe non può essere ritirata né sostituita. Togliete prima la marcatura alla relazione.

Una classe ancorata in più tabelle che le relazioni di stessa entità non uniscono tutte viene segnalata in **Da rivedere** come *tabelle non collegate*.

## Come si ricavano i termini {: #how-terms-are-derived }

Provisa crea automaticamente un termine in bozza da ogni nome di colonna, con una regola di normalizzazione deterministica: uniformazione di maiuscole e minuscole, scomposizione su separatori e camelCase, espansione delle abbreviazioni e rimozione dei token sostitutivi finali.

**L'espansione delle abbreviazioni** associa le abbreviazioni aziendali comuni alla forma completa: `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity` e così via. Sia `id` sia `key` si espandono in `identifier`. La tabella è fissa e prudente. Le abbreviazioni ambigue come `st`, `min` e `no` restano come sono, così che nulla venga indovinato male.

**La rimozione dei token sostitutivi** toglie un token finale `identifier`, `code`, `index` o `reference`. Una colonna chiamata `cust_id` non nomina l'identificativo in sé; nomina un cliente tramite un valore sostitutivo. Togliendo il sostitutivo, `cust_id` e `customerId` finiscono entrambe sul termine `customer`. Si tolgono solo token finali, e mai l'ultimo rimasto, così una colonna `id` da sola si espande in `identifier` e resta lì.

**La deduplicazione** è lo scopo. La regola è deterministica, quindi `cust_id`, `customerId` e `CUSTOMER_KEY` producono tutte `customer`. Ogni colonna riceve un riferimento su quell'unico termine, e la cura ha un solo punto in cui aggiungere la definizione invece di tre.

Le colonne di chiave saltano questa regola. I loro riferimenti vanno a una classe, come descritto sopra.

### Frasi generiche {: #generic-phrases }

Alcune frasi normalizzate sono troppo generiche per essere un concetto a sé. Una colonna `name`, `date` o `identifier` da sola nomina un attributo del concetto della sua tabella, che non esiste fuori da quella tabella. I dipendenti hanno nomi e i prodotti hanno nomi, ma non sono la stessa cosa.

Quando una frase rientra nell'insieme generico e c'è un contesto di tabella disponibile, il termine si qualifica come `<table concept> <phrase>`: `employees.first_name` si normalizza in `employee first name`.

L'insieme generico comprende sostantivi di attributo (`name`, `date`, `status`, `type`, `amount`, `quantity`), frasi da traccia di audit (`created_at`, `modified_by`, `submitted_timestamp`) e poche altre che compaiono in quasi ogni tabella.

### I termini seguono il nome di business {: #terms-follow-the-business-name }

Un termine ricavato segue il **nome di business** della colonna: il suo alias, se il modellatore ne ha impostato uno, altrimenti il suo nome fisico. Quando `usr_nm` ha l'alias `user name`, il termine ricavato è `user name`, dove espandere `usr_nm` avrebbe potuto dare `user number`.

Dare un alias alla colonna è la correzione più forte. Un alias arriva a ogni superficie che legge la colonna (SQL, GraphQL, agenti IA, il catalogo), così il modello si descrive correttamente ovunque. Rinominare il termine corregge una voce di catalogo e lascia la colonna come `usr_nm` per il lettore successivo. Il banner Bozza nella UI lo dice: date prima un alias alla colonna, e rinominate il termine solo quando il nome della colonna è giusto e il vocabolario no.

Cambiare l'alias di una colonna ricava di nuovo il suo termine in bozza, così l'ontologia segue il modello e non chiede mai due volte la stessa correzione. Dopo che un curatore ha aggiunto a un termine una definizione, una relazione, un esperto o un tag, una modifica dell'alias lascia la colonna dov'è, perché quel lavoro è del curatore e resta.

### Nomi di tabella di percorso d'accesso {: #access-path-table-names }

Alcuni nomi di tabella descrivono un percorso d'accesso invece di un concetto: `user_by_name` è un utente raggiunto tramite una ricerca per nome, non un tipo di entità a parte. Quando Provisa ricava il concetto di una tabella, taglia il nome al connettivo, così `user_by_name` diventa `user` e `orders_by_customer` diventa `order`.

Senza il taglio, `user_by_name` proporrebbe una classe `user name` in conflitto con l'attributo reale `users.name`. Il taglio vale solo per i concetti di tabella. In un nome di colonna, `by` fa parte del sostantivo composto, così `pet_by_name` e `pet_name` si normalizzano nello stesso termine, `pet name`.

## Perché approvazione e ancoraggio contano {: #why-approval-and-grounding-matter }

Gli agenti e altra automazione trovano i dati su cui operano attraverso l'ontologia, e la citano. Un agente parte da un termine che una persona ha approvato. Segue un percorso fino a dati di cui può vedere l'affidabilità, ancorati o ancora in attesa di un'approvazione. Arriva a tabelle e colonne che legge con i permessi, le regole di riga e le maschere del suo ruolo. Ogni passo di quel percorso ha un nome che un agente può mettere in una citazione.

Quindi due fatti su un termine decidono che cosa un agente può farne. Le persone impostano il suo **stato**. Provisa calcola il suo **ancoraggio**. La pagina li mostra come due chip separati, e voi filtrate su ciascuno.

## Stato {: #status }

Una persona imposta lo stato di un termine. Ne ha uno alla volta.

| Stato | Significato | Come ci arriva |
| --- | --- | --- |
| **Bozza** | Non ancora approvato. | Ogni termine ricavato parte da qui, come ogni termine che create. |
| **Approvato** | La scelta di uno steward: questo è vocabolario con cui lavorare. | Uno steward lo approva. L'approvazione richiede una definizione. |
| **Deprecato** | In uscita. | Un curatore depreca un termine approvato, oppure un termine curato perde la sua ultima colonna. |
| **Ritirato** | Fuori servizio, conservato come traccia. | Un curatore lo ritira e nomina un successore. |

Una definizione da sola non approva nulla. Approvate un termine quando la sua definizione dice che cosa il business intende con esso.

## Ancoraggio {: #grounding }

L'ancoraggio dice se un termine raggiunge dati. Nessuno lo imposta: Provisa lo ricava dalle relazioni del termine ogni volta che il grafo cambia.

L'ancoraggio di un termine guarda in basso lungo i suoi percorsi verso i dati. Un percorso va dal termine, attraverso le sue relazioni, fino al primo termine che contiene una colonna o una tabella: il termine all'estremo dei dati, che detiene il legame. Contano solo i termini strettamente tra i due estremi. Il termine stesso non conta mai contro il proprio ancoraggio, e nemmeno ciò che sta sopra di lui.

| Ancoraggio | Segno | Significato |
| --- | --- | --- |
| **Ancorato** | icona ⏚ verde | Qualche percorso verso il basso non ha termini in bozza tra il termine e l'estremo dei dati. |
| **Ancorato · percorso in attesa di approvazione** | icona ⏚ attenuata | Ogni percorso verso il basso ha un termine in bozza nel mezzo. Passate il mouse sull'icona: dice «Raggiunge dati, ma solo tramite termini non ancora approvati». |
| **Non ancorato** | nessuno | Nessun percorso raggiunge dati. |

L'icona è il simbolo elettrico della terra. Compare nell'elenco dei termini accanto al numero di colonne, nella pagina del termine, prima del nome del termine nei grafi e nella legenda del grafo. Il testo al passaggio del mouse di un termine ancorato dice fin dove arriva, per esempio «Ancorato: raggiunge dati in 3 colonne»; un lettore di schermo legge le stesse parole.

Alcuni casi seguono direttamente dalla regola:

- Un concetto in bozza che state scrivendo sopra una catena approvata è ancorato. Il suo chip di stato dice ancora Bozza, e i termini sotto non cambiano.
- Un termine ricavato da una colonna è una bozza all'estremo dei dati. Non mette mai in attesa la catena sopra di lui.
- Un termine in bozza a metà dell'unico percorso rende ⏚ attenuato ogni termine sopra. Approvare quell'unica bozza li ancora.
- Basta un secondo percorso pulito. Una bozza accanto non cambia nulla.
- I termini deprecati e ritirati spezzano la catena. Un termine il cui unico percorso passava per uno di essi smette di essere ancorato, e i suoi chip e filtri lo dicono.

## Che cosa vede un agente {: #what-an-agent-sees }

La ricerca di termini MCP, il vocabolario di SPARQL e ogni altra superficie per agenti mostrano a un agente la stessa porzione dell'ontologia:

- un termine approvato su un percorso verso i dati;
- un termine in bozza definito su un percorso verso i dati che contiene almeno un termine approvato, marcato come bozza;
- un termine non definito su un percorso del genere solo come passo anonimo, senza nome né definizione. Ogni passo resta distinto, così la forma del percorso non cambia.

Nient'altro raggiunge un agente. Una catena senza termini approvati resta nascosta, come i termini che non raggiungono dati e i termini deprecati o ritirati. Le persone vedono ogni termine nella pagina Ontologia, con stato e ancoraggio.

Ogni risultato di ricerca porta lo stato e l'ancoraggio del termine, le sue colonne e tabelle, i suoi esperti e i suoi percorsi verso i dati. Ogni passo di un percorso è un termine con nome o un passo anonimo, e l'estremo dei dati elenca ciò a cui è legato. È quel percorso che un agente segue e cita.

### Deprecare un termine {: #deprecating-a-term }

Deprecate un termine approvato quando è in uscita. Fornite un motivo, obbligatorio, e facoltativamente una data di ritiro prevista, oggi o più avanti. In quella data lo scheduler lo ritira. Un termine deprecato spezza la catena che passa per lui dal momento in cui lo deprecate, quindi conviene leggere prima l'anteprima dell'impatto.

Un termine il cui ritiro è scaduto e che contiene ancora colonne o tabelle non può essere ritirato per data, perché solo un successore può prenderle. Aspetta, e **Da rivedere** lo elenca come *ritiro bloccato* finché non lo ritirate con un successore.

Un termine curato che perde la sua ultima colonna viene deprecato automaticamente e conservato. Esce dal servizio, mantiene il contenuto fornito dai suoi editor e ritorna se la stessa colonna ricompare. Un termine non curato senza più colonne viene rimosso.

### Ritirare un termine {: #retiring-a-term }

Ritirare un termine sposta tutto ciò che gli è collegato su un **successore**, poi lo mette fuori servizio.

- Le colonne di una proprietà passano al successore. Deve essere una proprietà o un concetto.
- Le tabelle di una classe passano al successore, e le loro chiavi le seguono. Deve essere una classe o un concetto.
- Un concetto non contiene nulla, quindi il suo successore è facoltativo.

Il ritiro richiede un motivo, e scrive un arco `REPLACES` dal successore al termine ritirato. Il termine ritirato resta come traccia inerte: la sua definizione, i tag, le relazioni, gli esperti e il motivo. Non conduce nulla. Il successore mantiene il proprio stato; un ritiro non lo approva mai.

Poiché le sue colonne ora appartengono al successore, il pannello di un termine ritirato mostra a che cosa **si applicava per ultimo**: le sue colonne e tabelle come il modello le aveva nel commit precedente al ritiro. Il pannello rimanda anche al termine che lo sostituisce.

### Ripristinare un termine {: #restoring-a-term }

**Ripristina** annulla ciò che un curatore ha affermato. Un termine deprecato torna approvato. Un termine ritirato torna come concetto vuoto e in bozza, perché le sue colonne sono andate al successore, e il suo arco `REPLACES` viene rimosso.

### Che cosa rende curato un termine {: #what-makes-a-term-curated }

Un termine nato dalla normalizzazione delle colonne parte vuoto. Diventa curato quando ha uno qualsiasi di questi:

- una definizione salvata
- un arco di relazione
- un esperto della materia assegnato
- un tag, come `pii`
- un ritiro

I termini curati si conservano quando le loro colonne se ne vanno. La sincronizzazione non ricava mai altrove le colonne di un termine curato.

## Risincronizzare dalle tabelle {: #re-syncing-from-tables }

Ogni volta che una tabella viene salvata o ricaricata, Provisa riconcilia le colonne di quella tabella con l'ontologia. Le nuove colonne creano o collegano termini, le colonne sparite lasciano i loro riferimenti, e ogni termine rimasto senza riferimenti viene rimosso o deprecato secondo la regola sopra. Le colonne di chiave vanno alla loro classe, e una tabella con chiave senza classe ne propone una.

Si ricavano di nuovo solo i termini non curati. Se date un alias a una colonna e il suo termine in bozza cambia, la colonna passa al nuovo termine. Se il termine è curato, la colonna resta, perché una modifica dell'alias non prevale sulla scelta di un curatore.

Quando rimuovere un termine lascerebbe un concetto senza percorso verso i dati, Provisa depreca invece quel termine, così la struttura che avete costruito sopravvive finché non la ricollegate.

## Relazioni {: #relationships }

I termini si collegano ad altri termini tramite archi tipizzati.

| Tipo | Significato |
| --- | --- |
| `KIND_OF` | Il termine di origine è un tipo del termine di destinazione. |
| `PART_OF` | Il termine di origine è un componente del termine di destinazione. |
| `SYNONYM_OF` | I due termini sono intercambiabili in questo dominio. |
| `RELATED_TO` | Due argomenti distinti che vanno insieme, quando nessun altro tipo è adatto. Tiene un posto e non ancora. |
| `VALID_VALUE_OF` | L'origine è un valore ammesso dell'enumerazione o del dominio di destinazione. |
| `DERIVED_FROM` | L'origine è calcolata o tratta dalla destinazione. |
| `REPLACES` | L'origine sostituisce la destinazione. Ritirare un termine ne scrive uno dal suo successore. |
| `PREFERRED_TERM_FOR` | L'origine è il termine preferito rispetto alla destinazione sconsigliata. |
| `TRANSLATION_OF` | L'origine è una traduzione della destinazione in una lingua o impostazione locale. |
| `ANTONYM_OF` | L'origine è l'opposto semantico della destinazione. |

Le relazioni sono orientate. La UI mostra sia gli archi in uscita (questo termine → un altro) sia quelli in entrata (un altro termine → questo termine), ed etichetta ogni direzione con la propria frase in linguaggio semplice.

Gli archi stanno in una tabella associativa dichiarata come relazione di giunzione. La sua colonna del tipo di relazione è il discriminatore, così ogni tipo sopra è un tipo di relazione Cypher distinto tra due nodi termine. Nulla di tutto questo è specifico dell'ontologia: è dichiarata come dichiarereste una giunzione sulle vostre tabelle, e lo stesso codice la legge.

## Costruire un glossario che ancora {: #building-a-glossary-that-grounds }

Un termine aiuta un agente solo quando è ancorato, cioè quando un percorso di relazioni porta da lui a una colonna o a una tabella. Gli agenti vedono solo i termini su percorsi del genere, come descrive [Che cosa vede un agente](#what-an-agent-sees). Un termine ancorato vi dice dove stanno i suoi dati. Uno non ancorato nomina un'idea e vi lascia cercare i dati da soli.

I tipi di relazione che scegliete decidono se quel percorso esiste. L'ancoraggio scende da un termine più ampio a uno più ristretto. `revenue` raggiunge dati perché `order amount` ne fa parte e `order amount` contiene una colonna. Non risale mai, quindi `order amount` non guadagna nulla dall'essere parte di `revenue`.

### Verificate ogni tipo prima di sceglierlo {: #test-each-type-before-you-pick-it }

Leggete un arco con X come il termine da cui parte e Y come il termine a cui punta. Poi applicate le verifiche in quest'ordine e tenete la prima che riesce.

| Tipo | Verifica | Esempio | Che cosa ancora |
| --- | --- | --- | --- |
| **Tipo di** | Ogni X è un Y. | Ogni cliente aziendale è un cliente. | Y raggiunge i dati di X. |
| **Parte di** | Un Y ha un X. | Un ordine ha un importo dell'ordine. | Y raggiunge i dati di X. |
| **Valore valido di** | X è uno dei valori ammessi di Y. | Platino è un livello cliente. | Y raggiunge i dati di X. |
| **Sinonimo di** | X e Y significano la stessa cosa. | Cliente e acquirente. | Ciascuno raggiunge i dati dell'altro. |
| **Correlato a** | X e Y sono argomenti distinti che vanno insieme, e nessuno è un tipo, una parte o un valore dell'altro. | La torta e le candeline. | Niente. |

Derivato da, Sostituisce, Termine preferito per, Traduzione di e Contrario di registrano ciascuno un fatto su due termini. Nessuno di essi ancora.

Una proprietà che descrive un concetto ne è parte. L'inizio turno descrive un turno, quindi è **Parte di** copertura turni. Non è mai semplicemente correlato a essa.

Quando aggiungete una relazione o ne cambiate il tipo, il selettore dei tipi elenca prima Tipo di, Parte di e Valore valido di, ciascuno con la sua verifica sotto. Correlato a viene per ultimo, con la sua verifica e il promemoria che non ancora. Il link **Scegliere una relazione che ancori** sotto il selettore apre questa sezione.

### «Correlato a» tiene un posto {: #related-to-holds-a-place }

Correlato a è l'ultima risorsa. Poiché non ancora, un termine i cui unici collegamenti sono Correlato a non raggiunge dati, per quanti ne abbia. Trattate ciascuno come un segnaposto per una coppia che non avete ancora risolto.

Per lavorare sui segnaposto, spuntate **Correlato a** sopra l'elenco dei termini, accanto a **Deprecato** e **Da rivedere**. L'elenco si riduce ai termini con almeno un collegamento Correlato a, a uno dei due estremi. L'icona del libro accanto alla casella apre questa sezione.

Un termine che non raggiunge dati e ha solo collegamenti Correlato a mostra una nota nella sua pagina. La nota dice che alla coppia potrebbe mancare un termine intermedio e nomina le forme che la risolverebbero. È solo un'indicazione. Nulla del termine viene bloccato.

### Risolvete una coppia con un nuovo termine {: #resolve-a-pair-with-one-new-term }

Alla maggior parte delle coppie Correlato a manca un termine intermedio. Un nuovo termine trasforma il segnaposto in due archi che ancorano, e cinque forme ci riescono. Eccole per la torta e le candeline, dalla più stretta alla più ampia:

| Forma | Nuovo termine | Archi |
| --- | --- | --- |
| **Tipo di componente** | decorazione per torta | candeline Tipo di decorazione per torta; decorazione per torta Parte di torta |
| **Specializzazione** | torta di compleanno | candeline Parte di torta di compleanno; torta di compleanno Tipo di torta |
| **Tutto comune** | festa | torta Parte di festa; candeline Parte di festa |
| **Genere comune** | articoli per feste | torta Tipo di articoli per feste; candeline Tipo di articoli per feste |
| **Enumerazione comune** | tipo di articolo per feste | torta Valore valido di tipo di articolo per feste; candeline Valore valido di tipo di articolo per feste |

Seguite l'ancoraggio verso il basso in ogni riga. Nelle prime due forme, torta raggiunge i dati di candeline tramite il nuovo termine. Nelle ultime tre, il nuovo termine è l'estremo più ampio di entrambi gli archi, quindi raggiunge i dati di tutti e due. Né torta né candeline raggiunge quelli dell'altro.

A volte il termine intermedio esiste già, e basta cambiare il tipo dei collegamenti. La demo del rifugio inclusa ha un concetto, `shift coverage`, definito come la fascia oraria e il luogo in cui un membro del personale è in turno. Le sue parti `shift start`, `shift end` e `schedule location` contengono una colonna ciascuna. Collegato a esse con Correlato a, shift coverage non raggiungeva dati. Con ciascuna delle tre collegata come Parte di shift coverage, è ancorato tramite tutte e tre le colonne. È un tutto comune, e il tutto c'era fin dall'inizio.

### Preferite la forma più stretta {: #prefer-the-tightest-shape }

Più il nuovo termine è ampio, meno il suo ancoraggio dice a chi legge. Festa collega la torta e le candeline con la stessa certezza di decorazione per torta. Ma un nodo così ampio sta anche sopra la lista degli invitati, il locale e la musica, quindi il suo ancoraggio non dice più quali dati rispondono a una domanda sulle candeline. Decorazione per torta dà a un agente un percorso breve verso le colonne giuste. Festa gli dà un percorso lungo verso molte.

Partite dal tipo di componente e scendete nell'elenco solo quando una forma più stretta non è adatta. Usate un genere comune o un'enumerazione comune quando la coppia non condivide nulla di più ristretto.

### Quando scegliete «Correlato a» {: #when-you-choose-related-to }

Scegliere **Correlato a** per un collegamento che state aggiungendo apre una finestra di dialogo, per esempio «Collegare candeline e torta con «Correlato a»?». Cambiare in Correlato a il tipo di un collegamento esistente la apre a sua volta. L'interruzione è voluta, perché è il momento in cui correggere il collegamento costa meno.

La finestra di dialogo spiega prima perché Correlato a è l'arco meno utile. Non ancora, quindi il termine continua a non poter raggiungere dati tramite esso. Sotto disegna le cinque forme per i vostri due termini, dalla più stretta alla più ampia. Ogni forma mostra un nome per il nuovo termine, suggerito dal modello di IA della vostra organizzazione, per esempio candeline → decorazione per torta → torta. Se il modello non suggerisce nomi, la finestra di dialogo dice perché, e il nome lo scrivete voi.

Poi scegliete una di tre azioni.

- **Avvia** accanto a una forma apre il modulo del nuovo termine. Il nome suggerito è già compilato, e **Collegamenti aggiunti con esso** elenca i due archi. Rinominate il termine, aggiungete una definizione e i suoi domini, poi fate clic su **Crea**. Crea aggiunge il termine. L'arco che tocca il termine che state modificando entra nel vostro modulo e viene salvato con esso. L'altro arco viene scritto subito.
- **Aggiungi comunque «Correlato a»** aggiunge il collegamento con un clic. Si unisce ai segnaposto che il filtro **Correlato a** elenca.
- **Annulla** non cambia nulla.

### Come sceglie l'assistente delle relazioni {: #how-the-relationship-assistant-chooses }

**Relazioni** nel menu ✨ chiede al modello di IA della vostra organizzazione relazioni tra tutti i termini dei vostri domini. Le istruzioni del modello portano le stesse verifiche. Preferisce Tipo di, Parte di e Valore valido di ovunque uno sia adatto, e collega una proprietà al concetto che descrive con Parte di. Propone Correlato a solo quando due termini vanno chiaramente insieme e nessun altro tipo è adatto.

L'assistente collega termini che esistono già, e non aggiunge mai un termine. Per ogni Correlato a che aggiunge, nomina termini candidati per ogni forma adatta. Quando ha finito, **Collegamenti «Correlato a» da esaminare** elenca ciascuna di quelle coppie con le sue forme, dalla più stretta alla più ampia, e i candidati per ciascuna. Fate clic su uno dei due termini per aprirlo. Se una coppia ha bisogno di un nuovo termine, e come chiamarlo, lo decidete voi.

### Misurate il glossario {: #measure-the-glossary }

Il report **Ontology quality** misura il glossario a partire dal modello, per dominio e per steward. Apritelo nel [visualizzatore di report](admin.md#reports-viewer). Riporta:

- la quota di termini in servizio che sono ancorati, contando a parte quelli ancorati pienamente e quelli in attesa di approvazione;
- la quota di termini approvati e ancorati, cioè quelli che un agente può usare;
- i segnaposto non risolti, cioè i collegamenti Correlato a e i termini i cui unici collegamenti sono Correlato a;
- la precisione dell'ancoraggio, come passi da ogni termine ai dati e come numero di percorsi che passano per un termine nodo ampio;
- i termini ancorati solo tramite bozze, ciascuno con il termine la cui approvazione lo ancorerebbe pienamente;
- i termini non definiti su percorsi verso i dati;
- la deriva, i termini che hanno perso o guadagnato ancoraggio dal periodo precedente.

Ogni riga rimanda al suo termine, e ogni riga di segnaposto ai collegamenti Correlato a di quel termine, da esaminare. Come ogni report, si legge attraverso la pipeline governata con il vostro ruolo, e potete esportarlo.

## Concetti {: #concepts }

Un concetto non ha colonne né tabelle proprie. Usatene uno per un'idea di business che abbraccia più termini concreti, poi collegatelo ai termini che contengono dati. `revenue`, per esempio, potrebbe essere un concetto, con archi `PART_OF` da `order amount`, `adjustment amount` e `refund amount`.

Un concetto che non riesce a raggiungere alcuna colonna o tabella attraverso il grafo delle relazioni non nomina dati. Non compare nella ricerca di termini degli agenti né nell'esportazione dei metadati, e **Da rivedere** lo elenca come *nessun dato*. I domini si applicano solo a un concetto. Ogni altro termine prende i domini dalle tabelle che ancora o dalle colonne che contiene.

## PII sul significato {: #pii-on-meanings }

La PII è una proprietà di ciò che una colonna significa, quindi Provisa vi lascia marcarla una volta, sul termine, e copre ogni colonna con quel significato, comprese quelle registrate in seguito.

Il tag `pii` va su una colonna o su un termine di proprietà, mai su una sorgente, una tabella, una classe o un concetto. Un termine deve essere definito e non ritirato per riceverlo. Marcare un termine in bozza viene rifiutato con «Definite *termine* per marcarlo», e non potete cancellare la definizione di un termine marcato finché non togliete la marcatura.

Ogni colonna di un termine marcato porta `pii`, e `sensitive` con esso, ovunque si leggano i tag: governance, mascheramento, sensibilità, esportazione e viste dei tag. Nulla viene copiato sulle colonne. Una colonna spostata sul termine porta subito il tag, e una spostata altrove non lo porta più. Ogni colonna dice da dove viene il suo tag: impostato sulla colonna, con il suo motivo, oppure tramite un termine, che nomina.

Quando marcate una colonna il cui termine è una proprietà, la UI chiede se marcare le altre colonne che significano la stessa cosa.

- **Sì** mette il tag sul termine.
- **Solo questa** marca solo la colonna, e chiede perché.

Una colonna che è PII tramite il suo termine non può essere smarcata da sola. Smarcate il termine, oppure spostate la colonna sul termine che corrisponde a ciò che contiene.

### Spostamenti che perdono tag {: #moves-that-lose-tags }

Spostare una colonna su un altro termine può toglierle un tag: spostare `customers.email` da `customer email` (con tag `pii`) a un termine senza tag lascerebbe senza protezione la colonna dell'e-mail. Ogni spostamento fatto da una persona viene controllato prima. Ciò comprende rivendicare o spostare una colonna, ritirare un termine, e salvare o eliminare una relazione che riassegna colonne di chiave.

Quando una colonna smetterebbe del tutto di portare `pii`, lo spostamento viene rifiutato finché non decidete. La UI mostra un riepilogo di ciò che ogni colonna spostata perde e guadagna, prima le perdite.

- **Sposta** procede, con ogni tag perso confermato.
- **Sposta e marca** porta i tag persi sul termine su cui vanno le colonne, nella stessa modifica.
- **Annulla** non sposta nulla.

Una colonna che mantiene il tag tramite la propria marcatura non richiede conferma. Gli spostamenti che il modello fa da sé, come ricavare di nuovo le colonne di una bozza, non cambiano mai un tag ereditato.

## Da rivedere {: #to-review }

L'ontologia riporta ciò che richiede la vostra attenzione sotto forma di rilievi. Un termine con un rilievo è **Da rivedere**, e l'elenco dei termini filtra su di esso. Una semplice bozza non è un rilievo; le bozze sono lo stato ordinario dell'ontologia. Un termine che raggiunge dati solo tramite bozze lo è, perché una sola approvazione lo metterebbe davanti agli agenti.

| Rilievo | Che cosa significa | La correzione |
| --- | --- | --- |
| Tabelle non collegate | Una classe ancora più tabelle che le relazioni di stessa entità non uniscono tutte. | Marcate relazioni di stessa entità tra di esse. |
| Classe e attributo | Una classe contiene colonne che non sono sue chiavi, oppure una tabella con chiave non ha classe perché il suo concetto è una proprietà. | Spostate le colonne su una proprietà, oppure riancorate la tabella. |
| Tag sul tipo sbagliato | Un termine che non è una proprietà porta un tag che solo una proprietà trasmette alle colonne. | Togliete il tag. |
| Ritiro bloccato | Un termine deprecato ha superato la data di ritiro prevista ma contiene ancora colonne o tabelle. | Ritiratelo con un successore. |
| Nessun dato | Un concetto in bozza non raggiunge dati. | Collegatelo a un termine che contiene dati. |
| Approvato, non ancorato | Un termine approvato non raggiunge dati, quindi nessun agente lo vede. | Collegatelo a un termine che contiene dati. |
| Ancorato · percorso in attesa di approvazione | Ogni percorso verso i dati passa per una bozza. Il rilievo nomina la bozza la cui approvazione ancora il termine. | Aprite quel termine e approvatelo. |

I rilievi riportano; non cambiano nulla finché non agite.

## Esportazione dei metadati {: #metadata-export }

L'ontologia viene pubblicata in cataloghi di dati esterni come parte dell'esportazione dei metadati. Si pubblicano i termini approvati, ciascuno con il suo ancoraggio: `grounded`, `grounded_pending` o `not_grounded`. Una bozza non lascia mai Provisa. L'ancoraggio per l'esportazione si valuta sulle colonne e sulle tabelle effettivamente pubblicate. Un termine le cui colonne sono tutte trattenute dall'esportazione, perché le loro tabelle non sono prodotti di dati o perché filtri tecnici le escludono, viene trattenuto con esse.

Un termine approvato che un curatore ha poi deprecato si pubblica con il suo stato, il motivo e la data di ritiro prevista, così ogni catalogo può associarlo al proprio marcatore di deprecazione.

Gli archi di relazione si pubblicano solo quando si pubblicano entrambi i termini agli estremi. Gli asset di colonna si esportano in modo indipendente, quindi escludere un termine non nasconde i dati sottostanti.

### Escludere un termine dall'esportazione {: #excluding-a-term-from-export }

Alcune colonne portano infrastruttura, come identificativi di batch ETL, versioni di riga e timestamp di ingestione. Un termine ricavato da una colonna del genere può avere una definizione esatta e tuttavia non essere vocabolario di business. Il controllo **Escludi dall'esportazione dei metadati** trattiene il termine, e ogni arco di relazione che termina su di esso, dai cataloghi in cui Provisa pubblica. Le colonne stesse si esportano comunque come asset.

Ciò che conta è se il business usa la parola. Un identificativo di batch ETL ha un significato chiaro che appartiene all'ontologia per gli ingegneri, ma non a un catalogo di business accanto a `customer` e `revenue`.

## Lavorare con l'ontologia {: #working-with-the-ontology }

Aprite **Ontologia** nella barra di navigazione. Il pannello di sinistra elenca ogni termine con il suo ruolo, il suo stato se non è approvato, e il suo chip di ancoraggio. I filtri **Stato** e **Ancoraggio** restringono l'elenco per ciascuno, la casella **Deprecato** mostra o nasconde i termini deprecati, **Da rivedere** lo restringe ai termini con rilievi, e **Correlato a** lo restringe ai termini con un collegamento Correlato a. Fate clic su un termine per aprirne il pannello.

Il pannello mostra affiancati il ruolo, lo stato e l'ancoraggio del termine, e per una proprietà la sua marcatura PII con il numero di colonne che copre. Un termine ⏚ attenuato nomina le bozze la cui approvazione lo ancorerebbe. Un termine deprecato o ritirato mostra chi ne ha cambiato lo stato e perché. Un termine con rilievi si apre con una sezione **Da rivedere**: fate clic su un rilievo per vedere che cosa è stato trovato e l'azione che lo corregge.

### Riepilogo di stewardship {: #stewardship-summary }

Senza termini aperti, il pannello mostra il riepilogo di stewardship. Elenca i termini che gli agenti possono vedere, i termini ⏚ attenuati che attendono ciascuno un'approvazione, con il termine da approvare, e i termini approvati che non raggiungono dati. **Approva le bozze definite e ancorate** elenca ogni bozza che ha una definizione ed è ancorata; togliete la spunta a quelle con una definizione non giusta, e approvate le altre insieme. Il pulsante degli appunti nell'intestazione riporta il riepilogo.

### Vedere prima l'impatto {: #seeing-the-impact-first }

Approvare, ritirare e legare un termine cambiano ciascuno ciò che raggiunge i dati. Prima di ognuna di queste azioni, la pagina mostra che cosa cambierebbe: quanti termini diventano ancorati, ⏚ attenuati o non ancorati, e verso quante tabelle gli agenti guadagnano o perdono un percorso. Per esempio, «Approvare region ancora 3 termini e dà agli agenti un percorso verso altre 2 tabelle». **Approva** apre l'anteprima con il suo pulsante di conferma, **Ritira** la mostra sotto il successore che scegliete, e ancorare una tabella nel modulo di modifica la mostra sotto la tabella prima di salvare.

**Modifica** apre l'intero termine in un solo modulo. Salvare applica tutte le modifiche insieme.

- **Rinominate** il termine per cambiarne la formulazione senza spostarne le colonne.
- **Aggiungete una definizione** scrivendola, oppure fate clic sul pulsante della bozza IA per generare un punto di partenza dal nome del termine, dalle sue colonne e dalle sue relazioni. La bozza non viene salvata finché non salvate il modulo.
- **Rivendicate colonne** per spostare colonne da altri termini su questo, oppure scegliete **Sposta su termine** accanto a una delle sue colonne. Ogni colonna appartiene a un termine. Le colonne di chiave seguono la loro classe e non possono essere spostate.
- **Ancorate tabelle** su una classe, oppure rimuovetele.
- **Marcate la PII** su una proprietà.
- **Aggiungete una relazione**, scegliendone il tipo. Cambiate sul posto il tipo di un arco esistente invece di eliminarlo e aggiungerlo di nuovo.
- **Assegnate esperti** per ID utente, come `expert` o `author`.

Accanto a Modifica, il pannello offre le azioni di stato che il termine consente: **Approva** una bozza definita, **Depreca** un termine approvato, **Ritira** qualsiasi termine in servizio, e **Ripristina** uno deprecato o ritirato.

Dal menu ✨, **Definizioni** compila ogni definizione vuota in un passaggio e non sovrascrive mai il testo di una persona. **Relazioni** aggiunge archi tipizzati su tutto l'elenco dei termini; le proposte che nominano termini sconosciuti, lo stesso termine due volte o un tipo non riconosciuto vengono scartate. Ogni Correlato a che aggiunge viene elencato per l'esame, come descrive [Come sceglie l'assistente delle relazioni](#how-the-relationship-assistant-chooses).

Il banner **Bozza** su un termine vi dice se gli serve ancora una definizione (date un alias alla colonna o aggiungetene una) o solo la vostra approvazione. Un banner a parte dice quando un termine non raggiunge dati: collegatelo a un termine che ha dati.

## Interrogare l'ontologia {: #querying-the-ontology }

L'ontologia è uno schema che potete interrogare, chiamato `ontology`, accanto allo schema `meta` che descrive la struttura del modello. Le sue tabelle iniziano con un trattino basso, così la tabella propria di una classe non condivide mai il nome con una di esse.

| Tabella | Una riga per |
|---|---|
| `_terms` | Termine, con il suo stato, il suo ancoraggio, la sua definizione e se è una classe, una proprietà o entrambe |
| `_term_refs` | Legame di una proprietà a una colonna, con la tabella e il dominio della colonna |
| `_term_edges` | Relazione tra due termini |
| `_domains` | Dominio a cui appartiene un termine |
| `_experts` | Esperto o autore assegnato a un termine |
| `_axioms` | Enunciato dell'ontologia in RDFS, OWL e SKOS |
| `_closure` | Termine associato a ogni termine di cui è un tipo, a qualsiasi profondità, e a se stesso |

Un legame collega un termine a una colonna nello schema `meta`, così potete unire `ontology._term_refs` a `meta.table_columns` e `meta.registered_tables`. Le tabelle dell'ontologia sono di sola lettura. Cambiano man mano che curate i termini.

Leggete un termine solo quando raggiungete uno dei suoi domini. Un termine senza dominio è visibile a tutti, come un termine aziendale (`*`). Una relazione o un assioma è visibile quando lo sono entrambi i suoi termini.

### L'ontologia in SPARQL {: #the-ontology-in-sparql }

SPARQL restituisce l'ontologia nei vocabolari standard, così uno strumento RDF la legge come legge qualsiasi ontologia OWL:

- una classe è una `owl:Class`, e una relazione di tipo tra classi è `rdfs:subClassOf`;
- una proprietà è una `owl:DatatypeProperty`, oppure una `owl:ObjectProperty` quando ogni colonna che lega è una chiave verso una tabella con classe;
- il `rdfs:domain` di una proprietà è la classe di ogni tabella di cui lega una colonna, e il suo `rdfs:range` è la classe a cui portano le sue chiavi o il tipo XML Schema delle sue colonne;
- una relazione di tipo tra proprietà è `rdfs:subPropertyOf`;
- ogni termine ha il suo nome come `skos:prefLabel` e la sua definizione come `skos:definition`.

Un termine è nominato dal suo IRI di vocabolario, `<your Provisa address>/vocab/term/<name>`, l'IRI che tipizza le righe delle tabelle che ancora. Un termine ritirato non enuncia nulla.

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?class ?parent ?label WHERE {
  ?class a owl:Class ; rdfs:subClassOf ?parent .
  ?parent skos:prefLabel ?label .
}
```

Una query può unire l'ontologia alle righe che leggete. Le istanze di una classe comprendono quelle di ogni classe che ne è un tipo, a qualsiasi profondità:

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX term: <https://your-provisa/vocab/term/>

SELECT ?party ?class WHERE {
  ?party a ?class .
  ?class rdfs:subClassOf* term:Party .
}
```

Una proprietà funziona allo stesso modo: `?x ?p ?v . ?p rdfs:subPropertyOf* term:amount` legge i valori di ogni proprietà che è un tipo di `amount`. Ottenete le istanze delle tabelle che leggete, quindi una classe di cui non potete leggere le tabelle non ne aggiunge nessuna.

Gli stessi enunciati sono righe di `ontology._axioms` in SQL, e le coppie di tipo, chiuse a ogni profondità, sono righe di `ontology._closure`. GraphQL, Cypher e gli altri linguaggi di query non offrono queste due tabelle.

## Vedi anche {: #see-also }

- [Esportazione dei metadati](metadata-export.md) spiega come termini e relazioni vengono pubblicati in cataloghi di dati esterni.
- [Lineage a livello di colonna](lineage.md) spiega l'esploratore del lineage e come una colonna fisica riporta i termini a essa legati.

## Lo schema dell'ontologia {: #the-ontology-schema }

Lo schema di sistema `ontology` rende il vocabolario interrogabile da SQL, GraphQL, Cypher, REST e MCP, le stesse superfici che usate per i vostri dati. Sta accanto a `meta`, che contiene la struttura del modello; `ontology` ne contiene il significato.

Lo schema ha due parti: viste di definizione e tabelle di classe.

### Viste di definizione {: #definition-views }

Cinque viste espongono il contenuto del glossario:

| Vista | Contenuto |
| --- | --- |
| `_terms` | Ogni termine: nome, ruolo (classe, proprietà, concetto), definizione, stato e ancoraggio. |
| `_term_refs` | Ogni legame termine-colonna: il nome del termine, la sorgente, la tabella e la colonna. |
| `_term_edges` | Ogni arco di relazione: termine di origine, tipo di arco e termine di destinazione. |
| `_domains` | Appartenenza ai domini: il nome del termine e ogni dominio a cui appartiene. |
| `_experts` | Assegnazioni di esperti: il nome del termine, l'ID utente e il ruolo (`expert` o `author`). |

Una sesta vista, `_axioms`, contiene ogni tripla RDFS/OWL/SKOS che lo schema può esprimere, una riga per tripla, con le colonne `subject`, `predicate`, `object`, `object_iri` e `object_literal`. `object` nomina l'oggetto quando è un termine; `object_iri` lo contiene quando è un IRI, e `object_literal` quando è un letterale. Una delle tre contiene il valore su ogni riga. Gli strumenti che preferiscono le triple alle righe tabellari leggono da qui.

Un **legame di termine**, cioè un termine legato a una colonna e registrato in `_term_refs`, è diverso da un legame di comando. Un legame di termine dice che cosa significa una colonna; un legame di comando inserisce una funzione nella pipeline delle query.

### Tabelle di classe {: #class-tables }

Ogni termine di classe non ritirato diventa una vista nello schema `ontology` con il nome di quel termine, con gli spazi sostituiti da trattini bassi. Una classe ancorata in una tabella produce una vista; una classe ancorata in più tabelle produce l'unione di quelle tabelle.

In una vista di unione compaiono le righe di ogni tabella di origine. La colonna `_source_table` indica da quale tabella viene ogni riga: contiene il nome qualificato della tabella, `domain.table`. Una colonna di proprietà che una tabella di origine non lega vale `NULL` per le sue righe. Una proprietà legata a più colonne di una stessa tabella diventa una colonna array.

Ogni colonna di una vista di classe corrisponde a una proprietà legata. La vista è governata dalla stessa pipeline di qualsiasi altra lettura.

**Esempio.** Una classe `customer` ancorata in `sales.customers` e `crm.contacts`, con una proprietà `name` legata a `customers.name` e `contacts.full_name`, e una proprietà `segment` legata solo a `customers.segment`:

```sql
-- rows from both tables; segment is NULL for crm.contacts rows
SELECT _source_table, name, segment
FROM ontology.customer
WHERE segment = 'enterprise';
```

### Diritti di lettura {: #read-rights }

Valgono qui le stesse regole di governance delle vostre tabelle.

Un termine compare nelle viste di definizione solo se raggiungete almeno uno dei suoi domini. Un termine senza dominio è visibile a tutti. Una riga di legame compare solo quando la sua colonna vi viene servita. Un arco compare solo quando entrambi i suoi termini vi sono visibili. Una colonna di proprietà in una vista di classe compare solo quando le sue colonne sottostanti vengono servite.

Non serve alcun accesso speciale. Se potete interrogare una tabella, potete interrogare i termini di classe ancorati in essa.

### SPARQL {: #sparql }

L'endpoint SPARQL serve lo schema `ontology` come un'ontologia RDFS/OWL/SKOS standard. Ogni termine di classe diventa una `owl:Class`, ogni arco di tipo diventa `rdfs:subClassOf`, ogni termine di proprietà diventa `owl:DatatypeProperty` o `owl:ObjectProperty` con `rdfs:domain` e `rdfs:range`, gli archi di sottoproprietà diventano `rdfs:subPropertyOf`, i nomi diventano `skos:prefLabel` e le definizioni `skos:definition`. Valgono gli stessi diritti di lettura.

L'IRI di base di tutti i termini deriva dall'impostazione `server.public_url`, sotto il percorso `/vocab/term/`. Un termine di classe chiamato `customer` ha l'IRI `{public_url}/vocab/term/customer`.

Tre query che scriverebbe un ontologo:

**Trovare tutti i sottotipi di Party, a qualsiasi profondità:**

```sparql
PREFIX : <{public_url}/vocab/term/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?c WHERE {
  ?c rdfs:subClassOf* :Party .
}
```

**Trovare i clienti con un valore di segmento, usando l'IRI del termine:**

```sparql
PREFIX : <{public_url}/vocab/term/>

SELECT ?x ?s WHERE {
  ?x a :Customer ;
     :segment ?s .
}
```

**Elencare tutte le proprietà dichiarate su Customer con i loro intervalli:**

```sparql
PREFIX : <{public_url}/vocab/term/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?p ?range WHERE {
  ?p rdfs:domain :Customer ;
     rdfs:range ?range .
}
```

Sostituite `{public_url}` con l'indirizzo pubblico del vostro deployment, come configurato in `server.public_url`.
