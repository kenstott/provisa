# Ontologie {: #ontology }

L'ontologie est un vocabulaire vivant posé sur votre modèle de données. Chaque colonne physique se résout en un terme, et les colonnes qui portent le même sens partagent un seul terme, quelle que soit leur orthographe. Chaque terme peut contenir une définition, des relations typées vers d'autres termes, une liste d'experts métier et des étiquettes comme `pii` que porte chaque colonne ayant ce sens.

C'est un graphe de concepts, de leurs définitions et des relations entre eux. Provisa le relie aux tables et aux colonnes qui contiennent les valeurs, ce qui lui permet de répondre à des questions sur vos données. Un terme est un brouillon jusqu'à ce qu'un intendant l'approuve, et le fait qu'il atteigne ou non des données se déduit du graphe. L'ontologie est distincte des métriques : un terme dit ce qu'est un concept, une métrique dit comment un nombre se calcule.

Ce vocabulaire partagé relie le langage métier aux données physiques. Un agent IA qui sait que « customer » désigne chaque colonne portant un identifiant client n'a pas à deviner laquelle de `cust_id`, `customerId` et `CUSTOMER_KEY` est la bonne. Toutes se résolvent dans le même terme, et le terme porte la définition.

## Classes, propriétés et concepts {: #classes-properties-and-concepts }

Chaque terme a exactement un rôle, déduit de ce qu'il ancre. Vous ne le définissez jamais.

| Rôle | Ancré dans | Exemple |
| --- | --- | --- |
| **Classe** | Une ou plusieurs tables ou vues. Ses instances sont leurs lignes. | `customer`, ancrant `customers` et `crm_contacts` |
| **Propriété** | Des colonnes qui ne sont pas des clés. | `customer email`, contenant `customers.email` |
| **Concept** | Rien directement. Il atteint les données par ses relations. | `revenue`, avec des arêtes `PART_OF` depuis `order amount` et `refund amount` |

Un terme est l'un ou l'autre. Provisa refuse d'ancrer une table sur un terme qui contient des colonnes non clés, et refuse de déplacer une colonne non clé vers un terme ancré dans des tables. Chaque refus nomme le terme et ce qu'il contient déjà.

### Chaque table à clé propose une classe {: #every-keyed-table-proposes-a-class }

Quand une table dotée d'une clé primaire est enregistrée, Provisa l'ancre dans un terme de classe en brouillon nommé d'après le concept de la table : `orders` propose `order`, `order_lines` propose `order line`. Les tables d'un même concept partagent une classe. Une table sans clé primaire ne peut pas être une classe, et son ancrage est refusé en nommant la table.

Un ancrage que vous faites vous-même, dans l'UI, dans la configuration ou via Polly, est curé. La synchronisation ne le déplace jamais, et ancrer une table sur un terme curé remplace sa proposition en brouillon. Quand un concept est déjà une propriété avec des colonnes non clés, ses tables ne reçoivent aucune proposition ; l'ontologie les liste plutôt sous **À examiner**.

### Les clés appartiennent à la classe {: #keys-belong-to-the-class }

Une clé primaire n'a pas de terme propre. Chacune de ses colonnes, y compris les parties d'une clé composite, place sa référence sur la classe de sa table.

Une clé étrangère fonctionne de la même manière. Chaque colonne du côté multiple d'une relation déclarée place sa référence sur la classe de la table cible, quel que soit le nom de la colonne. `manager_id` sur `employees`, qui pointe vers `employees`, aboutit sur `employee` ; le rôle « manager » vit dans le nom de la relation. Une colonne sans relation déclarée est une propriété, même quand son nom correspond à une classe.

Provisa place les références de clés par dérivation seule, chaque fois qu'une table se synchronise, qu'une relation change ou que la classe d'une table change. Vous ne pouvez pas déplacer une colonne de clé à la main, et le refus nomme la colonne, sa table et sa classe. Ancrez une table sur un autre terme et ses clés suivent aussitôt.

### La même entité dans plusieurs tables {: #the-same-entity-in-several-tables }

Les instances de tables différentes sont distinctes jusqu'à ce que vous disiez le contraire. Quand `customers` et `crm_contacts` décrivent tous deux un même client, marquez la relation qui les joint comme **même entité**. Elle doit joindre deux tables que la même classe ancre, en un-à-un, sur une clé de chacune : la clé primaire ou une contrainte d'unicité déclarée.

Tant qu'une relation de même entité existe sur une table, sa classe ne peut être ni retirée ni remplacée. Retirez d'abord la marque de la relation.

Une classe ancrée dans plusieurs tables que les relations de même entité ne joignent pas toutes est signalée sous **À examiner** comme *tables non reliées*.

## Comment les termes sont dérivés {: #how-terms-are-derived }

Provisa crée automatiquement un terme en brouillon à partir de chaque nom de colonne, selon une règle de normalisation déterministe : uniformisation de la casse, découpage aux séparateurs et en camelCase, développement des abréviations et suppression des jetons substituts finaux.

**Le développement des abréviations** fait correspondre les abréviations courantes en entreprise à leur forme complète : `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity`, etc. `id` et `key` se développent tous deux en `identifier`. La table est fixe et prudente. Les abréviations ambiguës comme `st`, `min` et `no` restent telles quelles, pour que rien ne soit deviné de travers.

**La suppression des jetons substituts** retire un jeton final `identifier`, `code`, `index` ou `reference`. Une colonne nommée `cust_id` ne nomme pas l'identifiant lui-même ; elle nomme un client par une valeur de substitution. La suppression du substitut fait aboutir `cust_id` et `customerId` sur le terme `customer`. Seuls les jetons finaux sont supprimés, et jamais le dernier restant, si bien qu'une colonne `id` seule se développe en `identifier` et en reste là.

**La déduplication** est le but. La règle est déterministe, donc `cust_id`, `customerId` et `CUSTOMER_KEY` produisent tous `customer`. Chaque colonne reçoit une référence sur ce terme unique, et la curation a un seul endroit où ajouter la définition au lieu de trois.

Les colonnes de clé échappent à cette règle. Leurs références vont à une classe, comme décrit plus haut.

### Expressions génériques {: #generic-phrases }

Certaines expressions normalisées sont trop génériques pour être un concept à elles seules. Une colonne `name`, `date` ou `identifier` seule nomme un attribut du concept de sa table, qui n'existe pas hors de cette table. Les employés ont des noms, les produits ont des noms, mais ce n'est pas la même chose.

Quand une expression appartient à l'ensemble générique et qu'un contexte de table est disponible, le terme se qualifie en `<table concept> <phrase>` : `employees.first_name` se normalise en `employee first name`.

L'ensemble générique couvre les noms d'attributs (`name`, `date`, `status`, `type`, `amount`, `quantity`), les expressions de piste d'audit (`created_at`, `modified_by`, `submitted_timestamp`) et quelques autres qui apparaissent dans presque toutes les tables.

### Les termes suivent le nom métier {: #terms-follow-the-business-name }

Un terme dérivé suit le **nom métier** de la colonne : son alias quand le modélisateur en a défini un, et son nom physique sinon. Quand `usr_nm` a l'alias `user name`, le terme dérivé est `user name`, là où le développement de `usr_nm` aurait pu donner `user number`.

Donner un alias à une colonne est la correction la plus forte. Un alias se propage à chaque surface qui lit la colonne (SQL, GraphQL, agents IA, le catalogue), si bien que le modèle se décrit correctement partout. Renommer le terme corrige une entrée de catalogue et laisse la colonne affichée `usr_nm` pour le lecteur suivant. Le bandeau Brouillon de l'UI le dit : donnez d'abord un alias à la colonne, et ne renommez le terme que lorsque le nom de la colonne est juste et que le vocabulaire ne l'est pas.

Changer l'alias d'une colonne redérive son terme en brouillon, si bien que l'ontologie suit le modèle et ne demande jamais deux fois la même correction. Dès qu'un curateur a ajouté à un terme une définition, une relation, un expert ou une étiquette, une modification d'alias laisse la colonne où elle est, car ce travail appartient au curateur et il reste.

### Noms de table de chemin d'accès {: #access-path-table-names }

Certains noms de table décrivent un chemin d'accès plutôt qu'un concept : `user_by_name` est un utilisateur atteint par une recherche sur le nom, et non un type d'entité distinct. Quand Provisa dérive le concept d'une table, il coupe le nom au connecteur, si bien que `user_by_name` devient `user` et `orders_by_customer` devient `order`.

Sans cette coupe, `user_by_name` proposerait une classe `user name` qui entrerait en collision avec le véritable attribut `users.name`. La coupe s'applique aux seuls concepts de table. Dans un nom de colonne, `by` fait partie du nom composé, si bien que `pet_by_name` et `pet_name` se normalisent dans le même terme, `pet name`.

## Pourquoi l'approbation et l'ancrage comptent {: #why-approval-and-grounding-matter }

Les agents et autres automatismes trouvent les données sur lesquelles ils travaillent par l'ontologie, et ils la citent. Un agent part d'un terme qu'une personne a approuvé. Il suit un chemin jusqu'à des données dont il voit la fiabilité, ancrées ou encore en attente d'une approbation. Il arrive à des tables et des colonnes qu'il lit sous les droits, les règles de lignes et les masques de son rôle. Chaque étape de ce chemin a un nom qu'un agent peut mettre dans une citation.

Deux faits sur un terme décident donc de ce qu'un agent peut en faire. Les personnes fixent son **statut**. Provisa calcule son **ancrage**. La page les montre sous forme de deux puces distinctes, et vous filtrez sur chacune.

## Statut {: #status }

Une personne fixe le statut d'un terme. Il en a un à la fois.

| Statut | Signification | Comment il y arrive |
| --- | --- | --- |
| **Brouillon** | Pas encore approuvé. | Chaque terme dérivé commence ici, tout comme chaque terme que vous créez. |
| **Approuvé** | Le choix d'un intendant : c'est un vocabulaire avec lequel travailler. | Un intendant l'approuve. L'approbation exige une définition. |
| **Obsolète** | En voie de sortie. | Un curateur marque obsolète un terme approuvé, ou un terme curé perd sa dernière colonne. |
| **Retiré** | Hors service, conservé comme trace. | Un curateur le retire et nomme un successeur. |

Une définition seule n'approuve rien. Approuvez un terme une fois que sa définition dit ce que le métier entend par lui.

## Ancrage {: #grounding }

L'ancrage dit si un terme atteint des données. Personne ne le fixe : Provisa le calcule à partir des relations du terme chaque fois que le graphe change.

L'ancrage d'un terme regarde vers le bas, le long de ses propres chemins vers les données. Un chemin va du terme, par ses relations, jusqu'au premier terme qui contient une colonne ou une table : le terme à l'extrémité des données, qui porte la liaison. Seuls comptent les termes strictement entre les deux extrémités. Le terme lui-même ne compte jamais contre son propre ancrage, pas plus que ce qui se trouve au-dessus de lui.

| Ancrage | Marque | Signification |
| --- | --- | --- |
| **Ancré** | icône ⏚ verte | Un chemin vers le bas n'a aucun terme en brouillon entre le terme et l'extrémité des données. |
| **Ancré · chemin en attente d'approbation** | icône ⏚ atténuée | Chaque chemin vers le bas passe par un terme en brouillon. Survolez l'icône : elle indique « Atteint des données, mais seulement par des termes pas encore approuvés ». |
| **Non ancré** | aucune | Aucun chemin n'atteint de données. |

L'icône est le symbole électrique de la terre. Elle figure dans la liste des termes à côté du nombre de colonnes, sur la page du terme, devant le nom du terme dans les graphes et dans la légende des graphes. Le texte de survol d'un terme ancré dit jusqu'où il atteint, par exemple « Ancré : atteint des données dans 3 colonnes » ; un lecteur d'écran lit les mêmes mots.

Quelques cas découlent directement de la règle :

- Un concept en brouillon que vous rédigez au-dessus d'une chaîne approuvée est ancré. Sa puce de statut indique toujours Brouillon, et les termes en dessous ne changent pas.
- Un terme dérivé d'une colonne est un brouillon à l'extrémité des données. Il ne met jamais la chaîne au-dessus en attente.
- Un terme en brouillon au milieu du seul chemin rend chaque terme au-dessus ⏚ atténué. Approuver ce seul brouillon les ancre.
- Un second chemin propre suffit. Un brouillon à côté ne change rien.
- Les termes obsolètes et retirés rompent la chaîne. Un terme dont le seul chemin passait par l'un d'eux cesse d'être ancré, et ses puces et filtres le disent.

## Ce que voit un agent {: #what-an-agent-sees }

La recherche de termes MCP, le vocabulaire de SPARQL et toutes les autres surfaces destinées aux agents montrent à un agent la même partie de l'ontologie :

- un terme approuvé sur un chemin vers des données ;
- un terme en brouillon défini sur un chemin vers des données qui contient au moins un terme approuvé, marqué brouillon ;
- un terme non défini sur un tel chemin uniquement comme étape anonyme, sans nom ni définition. Chaque étape reste distincte, si bien que la forme du chemin ne change pas.

Rien d'autre n'atteint un agent. Une chaîne sans terme approuvé reste cachée, tout comme les termes qui n'atteignent aucune donnée et les termes obsolètes ou retirés. Les personnes voient chaque terme sur la page Ontologie, avec son statut et son ancrage.

Chaque résultat de recherche porte le statut et l'ancrage du terme, ses colonnes et tables, ses experts et ses chemins vers les données. Chaque étape d'un chemin est un terme nommé ou une étape anonyme, et l'extrémité des données liste ce à quoi elle est liée. C'est ce chemin qu'un agent suit et cite.

### Marquer un terme obsolète {: #deprecating-a-term }

Marquez obsolète un terme approuvé quand il est en voie de sortie. Vous donnez une raison, obligatoire, et éventuellement une date de retrait prévue, aujourd'hui ou plus tard. À cette date, le planificateur le retire. Un terme obsolète rompt la chaîne qui passe par lui dès l'instant où vous le marquez, donc lisez d'abord l'aperçu de l'impact.

Un terme dont le retrait est échu et qui contient encore des colonnes ou des tables ne peut pas être retiré par date, car seul un successeur peut les reprendre. Il attend, et **À examiner** le liste comme *retrait bloqué* jusqu'à ce que vous le retiriez avec un successeur.

Un terme curé qui perd sa dernière colonne est automatiquement marqué obsolète et conservé. Il sort du service, garde le contenu fourni par ses éditeurs, et revient si la même colonne réapparaît. Un terme non curé sans plus aucune colonne est supprimé.

### Retirer un terme {: #retiring-a-term }

Retirer un terme déplace tout ce qui lui est rattaché vers un **successeur**, puis le met hors service.

- Les colonnes d'une propriété passent au successeur. Il doit être une propriété ou un concept.
- Les tables d'une classe passent au successeur, et leurs clés les suivent. Il doit être une classe ou un concept.
- Un concept ne contient rien, donc son successeur est facultatif.

Le retrait exige une raison, et il écrit une arête `REPLACES` du successeur vers le terme retiré. Le terme retiré reste comme trace inerte : sa définition, ses étiquettes, ses relations, ses experts et la raison. Il ne conduit rien. Le successeur garde son propre statut ; un retrait ne l'approuve jamais.

Comme ses colonnes appartiennent désormais au successeur, le panneau d'un terme retiré montre ce à quoi il **s'appliquait en dernier** : ses colonnes et ses tables telles que le modèle les contenait dans le commit précédant le retrait. Son panneau renvoie aussi au terme qui le remplace.

### Restaurer un terme {: #restoring-a-term }

**Restaurer** annule ce qu'un curateur a affirmé. Un terme obsolète est de nouveau approuvé. Un terme retiré revient comme concept vide et en brouillon, puisque ses colonnes sont allées à son successeur, et son arête `REPLACES` est supprimée.

### Ce qui rend un terme curé {: #what-makes-a-term-curated }

Un terme né de la normalisation des colonnes démarre vide. Il devient curé dès qu'il a l'un de ces éléments :

- une définition enregistrée
- une arête de relation
- un expert métier assigné
- une étiquette, comme `pii`
- un retrait

Les termes curés sont conservés quand leurs colonnes partent. La synchronisation ne redérive jamais ailleurs les colonnes d'un terme curé.

## Resynchroniser depuis les tables {: #re-syncing-from-tables }

Chaque fois qu'une table est enregistrée ou rechargée, Provisa réconcilie les colonnes de cette table avec l'ontologie. Les nouvelles colonnes créent ou relient des termes, les colonnes disparues abandonnent leurs références, et tout terme laissé sans référence est supprimé ou marqué obsolète selon la règle ci-dessus. Les colonnes de clé vont à leur classe, et une table à clé sans classe en propose une.

Seuls les termes non curés sont redérivés. Si vous donnez un alias à une colonne et que son terme en brouillon change, la colonne passe au nouveau terme. Si le terme est curé, la colonne reste, car une modification d'alias ne l'emporte pas sur le choix d'un curateur.

Quand la suppression d'un terme laisserait un concept sans chemin vers les données, Provisa marque plutôt ce terme obsolète, si bien que la structure que vous avez construite survit jusqu'à ce que vous la recâbliez.

## Relations {: #relationships }

Les termes sont reliés à d'autres termes par des arêtes typées.

| Type | Signification |
| --- | --- |
| `KIND_OF` | Le terme source est un type du terme cible. |
| `PART_OF` | Le terme source est un composant du terme cible. |
| `SYNONYM_OF` | Les deux termes sont interchangeables dans ce domaine. |
| `RELATED_TO` | Deux sujets distincts qui vont ensemble, quand aucun autre type ne convient. Il tient une place et n'ancre pas. |
| `VALID_VALUE_OF` | La source est une valeur autorisée de l'énumération ou du domaine cible. |
| `DERIVED_FROM` | La source est calculée ou tirée de la cible. |
| `REPLACES` | La source remplace la cible. Retirer un terme en écrit une depuis son successeur. |
| `PREFERRED_TERM_FOR` | La source est le terme privilégié par rapport à la cible déconseillée. |
| `TRANSLATION_OF` | La source est une traduction de la cible dans une langue ou une locale. |
| `ANTONYM_OF` | La source est l'opposé sémantique de la cible. |

Les relations sont orientées. L'UI montre à la fois les arêtes sortantes (ce terme → un autre) et les arêtes entrantes (un autre terme → ce terme), en étiquetant chaque sens de sa propre formule en langage courant.

Les arêtes résident dans une table associative déclarée comme relation de jonction. Sa colonne de type de relation est le discriminant, si bien que chaque type ci-dessus est un type de relation Cypher distinct entre deux nœuds de terme. Rien de tout cela n'est propre à l'ontologie : elle est déclarée comme vous déclareriez une jonction sur vos propres tables, et le même code la lit.

## Construire un glossaire qui ancre {: #building-a-glossary-that-grounds }

Un terme n'aide un agent que s'il est ancré, c'est-à-dire si un chemin de relations mène de lui à une colonne ou à une table. Les agents ne voient que les termes situés sur de tels chemins, comme le décrit [Ce que voit un agent](#what-an-agent-sees). Un terme ancré vous dit où vivent ses données. Un terme non ancré nomme une idée et vous laisse trouver les données vous-même.

Les types de relations que vous choisissez décident si ce chemin existe. L'ancrage descend d'un terme plus large vers un terme plus étroit. `revenue` atteint des données parce que `order amount` en fait partie et que `order amount` contient une colonne. Il ne remonte jamais, donc `order amount` ne gagne rien à faire partie de `revenue`.

### Testez chaque type avant de le choisir {: #test-each-type-before-you-pick-it }

Lisez une arête avec X pour le terme dont elle part et Y pour le terme vers lequel elle pointe. Appliquez ensuite les tests dans cet ordre et retenez le premier qui réussit.

| Type | Test | Exemple | Ce qu'il ancre |
| --- | --- | --- | --- |
| **Type de** | Tout X est un Y. | Tout client grand compte est un client. | Y atteint les données de X. |
| **Partie de** | Un Y a un X. | Une commande a un montant de commande. | Y atteint les données de X. |
| **Valeur valide de** | X est l'une des valeurs autorisées de Y. | Platine est un niveau client. | Y atteint les données de X. |
| **Synonyme de** | X et Y signifient la même chose. | Client et acheteur. | Chacun atteint les données de l'autre. |
| **Lié à** | X et Y sont des sujets distincts qui vont ensemble, et aucun n'est un type, une partie ou une valeur de l'autre. | Le gâteau et les bougies. | Rien. |

Dérivé de, Remplace, Terme privilégié pour, Traduction de et Antonyme de consignent chacun un fait sur deux termes. Aucun d'eux n'ancre.

Une propriété qui décrit un concept en fait partie. Le début de service décrit un service, il est donc **Partie de** la couverture de service. Il n'y est jamais simplement lié.

Quand vous ajoutez une relation ou changez son type, le sélecteur de type liste d'abord Type de, Partie de et Valeur valide de, chacun avec son test en dessous. Lié à vient en dernier, avec son test et le rappel qu'il n'ancre pas. Le lien **Choisir une relation qui ancre** sous le sélecteur ouvre cette section.

### « Lié à » tient une place {: #related-to-holds-a-place }

Lié à est le dernier recours. Comme il n'ancre pas, un terme dont les seuls liens sont Lié à n'atteint aucune donnée, quel que soit leur nombre. Traitez chacun comme un espace réservé pour une paire que vous n'avez pas encore résolue.

Pour traiter les espaces réservés, cochez **Lié à** au-dessus de la liste des termes, à côté de **Obsolète** et **À examiner**. La liste se réduit aux termes ayant au moins un lien Lié à, à l'une ou l'autre extrémité. L'icône de livre à côté de la case ouvre cette section.

Un terme qui n'atteint aucune donnée et n'a que des liens Lié à affiche une note sur sa page. La note dit qu'il manque peut-être un terme intermédiaire à la paire et nomme les formes qui la résoudraient. C'est une simple indication. Rien n'est bloqué sur le terme.

### Résoudre une paire avec un nouveau terme {: #resolve-a-pair-with-one-new-term }

À la plupart des paires Lié à, il manque un terme intermédiaire. Un nouveau terme transforme l'espace réservé en deux arêtes qui ancrent, et cinq formes y parviennent. Les voici pour le gâteau et les bougies, la plus étroite d'abord :

| Forme | Nouveau terme | Arêtes |
| --- | --- | --- |
| **Type de composant** | décoration de gâteau | bougies Type de décoration de gâteau ; décoration de gâteau Partie de gâteau |
| **Spécialisation** | gâteau d'anniversaire | bougies Partie de gâteau d'anniversaire ; gâteau d'anniversaire Type de gâteau |
| **Tout commun** | fête | gâteau Partie de fête ; bougies Partie de fête |
| **Genre commun** | articles de fête | gâteau Type de articles de fête ; bougies Type de articles de fête |
| **Énumération commune** | type d'article de fête | gâteau Valeur valide de type d'article de fête ; bougies Valeur valide de type d'article de fête |

Suivez l'ancrage vers le bas dans chaque ligne. Dans les deux premières formes, gâteau atteint les données de bougies par le nouveau terme. Dans les trois dernières, le nouveau terme est l'extrémité la plus large des deux arêtes, il atteint donc les données des deux. Ni gâteau ni bougies n'atteint celles de l'autre.

Parfois, le terme intermédiaire existe déjà et il suffit de changer le type des liens. La démo de refuge livrée comporte un concept, `shift coverage`, défini comme la plage horaire et le lieu où un membre du personnel est planifié. Ses parties `shift start`, `shift end` et `schedule location` contiennent chacune une colonne. Reliée à elles par Lié à, shift coverage n'atteignait aucune donnée. Depuis que chacune des trois est reliée comme Partie de shift coverage, celle-ci est ancrée par les trois colonnes. C'est un tout commun, et le tout était là depuis le début.

### Préférez la forme la plus étroite {: #prefer-the-tightest-shape }

Plus le nouveau terme est large, moins son ancrage renseigne le lecteur. Fête relie le gâteau et les bougies aussi sûrement que décoration de gâteau. Mais un pivot aussi large se trouve aussi au-dessus de la liste des invités, du lieu et de la musique, si bien que son ancrage ne dit plus quelles données répondent à une question sur les bougies. Décoration de gâteau donne à un agent un chemin court vers les bonnes colonnes. Fête lui donne un long chemin vers beaucoup d'entre elles.

Commencez par le type de composant et ne descendez dans la liste que lorsqu'une forme plus étroite ne convient pas. Utilisez un genre commun ou une énumération commune quand la paire ne partage rien de plus étroit.

### Quand vous choisissez « Lié à » {: #when-you-choose-related-to }

Choisir **Lié à** pour un lien que vous ajoutez ouvre une boîte de dialogue, par exemple « Relier bougies et gâteau par « Lié à » ? ». Changer le type d'un lien existant en Lié à l'ouvre aussi. L'interruption est voulue, car c'est le moment où corriger le lien coûte le moins.

La boîte de dialogue explique d'abord pourquoi Lié à est l'arête la moins utile. Elle n'ancre pas, donc le terme ne peut toujours pas atteindre de données par elle. En dessous, elle dessine les cinq formes pour vos deux termes, la plus étroite d'abord. Chaque forme affiche un nom pour le nouveau terme, suggéré par le modèle d'IA de votre organisation, par exemple bougies → décoration de gâteau → gâteau. Si le modèle ne suggère aucun nom, la boîte de dialogue dit pourquoi, et vous saisissez le nom vous-même.

Vous choisissez ensuite l'une de trois actions.

- **Commencer** à côté d'une forme ouvre le formulaire de nouveau terme. Le nom suggéré est prérempli, et **Liens ajoutés avec lui** liste les deux arêtes. Renommez le terme, ajoutez une définition et ses domaines, puis cliquez sur **Créer**. Créer ajoute le terme. L'arête qui touche le terme que vous modifiez rejoint votre formulaire et est enregistrée avec lui. L'autre arête est écrite aussitôt.
- **Ajouter « Lié à » quand même** ajoute le lien en un clic. Il rejoint les espaces réservés que liste le filtre **Lié à**.
- **Annuler** ne change rien.

### Comment l'assistant de relations choisit {: #how-the-relationship-assistant-chooses }

**Relations** dans le menu ✨ demande au modèle d'IA de votre organisation des relations entre tous les termes de vos domaines. Les instructions du modèle portent les mêmes tests. Il préfère Type de, Partie de et Valeur valide de partout où l'un convient, et relie une propriété au concept qu'elle décrit par Partie de. Il ne propose Lié à que lorsque deux termes vont clairement ensemble et qu'aucun autre type ne convient.

L'assistant relie des termes qui existent déjà, et il n'ajoute jamais de terme. Pour chaque Lié à qu'il ajoute, il nomme des termes candidats pour chaque forme qui convient. Quand il a terminé, **Liens « Lié à » à examiner** liste chacune de ces paires avec ses formes, la plus étroite d'abord, et les candidats pour chacune. Cliquez sur l'un ou l'autre terme pour l'ouvrir. C'est à vous de décider si une paire a besoin d'un nouveau terme, et comment l'appeler.

### Mesurer le glossaire {: #measure-the-glossary }

Le rapport **Ontology quality** mesure le glossaire à partir du modèle, par domaine et par intendant. Ouvrez-le dans le [visualiseur de rapports](admin.md#reports-viewer). Il indique :

- la part des termes en service qui sont ancrés, les termes entièrement ancrés et ceux en attente d'approbation étant comptés à part ;
- la part des termes approuvés et ancrés, c'est-à-dire ceux qu'un agent peut utiliser ;
- les espaces réservés non résolus, c'est-à-dire les liens Lié à et les termes dont les seuls liens sont Lié à ;
- la précision de l'ancrage, en nombre d'étapes de chaque terme jusqu'aux données et en nombre de chemins qui passent par un terme pivot large ;
- les termes ancrés uniquement par des brouillons, chacun avec le terme dont l'approbation l'ancrerait entièrement ;
- les termes non définis sur des chemins vers des données ;
- la dérive, c'est-à-dire les termes qui ont perdu ou gagné leur ancrage depuis la période précédente.

Chaque ligne renvoie à son terme, et chaque ligne d'espace réservé aux liens Lié à de ce terme, pour examen. Comme tout rapport, il se lit par le pipeline gouverné sous votre rôle, et vous pouvez l'exporter.

## Concepts {: #concepts }

Un concept n'a ni colonnes ni tables propres. Utilisez-en un pour une idée métier qui couvre plusieurs termes concrets, puis reliez-le aux termes qui contiennent des données. `revenue`, par exemple, pourrait être un concept, avec des arêtes `PART_OF` depuis `order amount`, `adjustment amount` et `refund amount`.

Un concept qui ne peut atteindre aucune colonne ni aucune table par le graphe de relations ne désigne aucune donnée. Il n'apparaît ni dans la recherche de termes des agents ni dans l'export de métadonnées, et **À examiner** le liste comme *aucune donnée*. Les domaines ne s'appliquent qu'à un concept. Tout autre terme tient ses domaines des tables qu'il ancre ou des colonnes qu'il contient.

## Les PII sur le sens {: #pii-on-meanings }

Les PII sont une propriété de ce que signifie une colonne, donc Provisa vous laisse les marquer une fois, sur le terme, et couvre ainsi chaque colonne ayant ce sens, y compris celles enregistrées plus tard.

L'étiquette `pii` se pose sur une colonne ou sur un terme de propriété, jamais sur une source, une table, une classe ou un concept. Un terme doit être défini et non retiré pour la recevoir. Marquer un terme en brouillon est refusé avec « Définissez *terme* pour le marquer », et vous ne pouvez pas effacer la définition d'un terme étiqueté tant que vous ne l'avez pas démarqué.

Chaque colonne d'un terme étiqueté porte `pii`, et `sensitive` avec, partout où les étiquettes sont lues : gouvernance, masquage, sensibilité, export et vues d'étiquettes. Rien n'est copié sur les colonnes. Une colonne déplacée sur le terme porte l'étiquette aussitôt, et une colonne déplacée hors de lui ne la porte plus. Chaque colonne dit d'où vient son étiquette : posée sur la colonne, avec sa raison, ou héritée d'un terme, qu'elle nomme.

Quand vous marquez une colonne dont le terme est une propriété, l'UI demande s'il faut marquer les autres colonnes qui signifient la même chose.

- **Oui** pose l'étiquette sur le terme.
- **Seulement celle-ci** marque la colonne seule, et demande pourquoi.

Une colonne PII par son terme ne peut pas être démarquée seule. Démarquez le terme, ou déplacez la colonne vers le terme qui correspond à ce qu'elle contient.

### Déplacements qui font perdre des étiquettes {: #moves-that-lose-tags }

Déplacer une colonne vers un autre terme peut lui retirer une étiquette : déplacer `customers.email` de `customer email` (étiqueté `pii`) vers un terme non étiqueté laisserait la colonne d'e-mail sans protection. Chaque déplacement fait par une personne est vérifié au préalable. Cela couvre la revendication ou le déplacement d'une colonne, le retrait d'un terme, et l'enregistrement ou la suppression d'une relation qui réaffecte des colonnes de clé.

Quand une colonne cesserait complètement de porter `pii`, le déplacement est refusé jusqu'à ce que vous décidiez. L'UI montre un seul récapitulatif de ce que chaque colonne déplacée perd et gagne, les pertes d'abord.

- **Déplacer** procède, chaque étiquette perdue étant confirmée.
- **Déplacer et marquer** reporte les étiquettes perdues sur le terme vers lequel vont les colonnes, dans la même modification.
- **Annuler** ne déplace rien.

Une colonne qui garde l'étiquette par sa propre marque n'a besoin d'aucune confirmation. Les déplacements que le modèle fait lui-même, comme la redérivation des colonnes d'un brouillon, ne changent jamais une étiquette héritée.

## À examiner {: #to-review }

L'ontologie signale ce qui demande votre attention sous forme de constats. Un terme ayant un constat est **À examiner**, et la liste des termes filtre dessus. Un simple brouillon n'est pas un constat ; les brouillons sont l'état ordinaire de l'ontologie. Un terme qui n'atteint des données que par des brouillons en est un, car une seule approbation le mettrait devant les agents.

| Constat | Ce qu'il signifie | La correction |
| --- | --- | --- |
| Tables non reliées | Une classe ancre plusieurs tables que les relations de même entité ne joignent pas toutes. | Marquez des relations de même entité entre elles. |
| Classe et attribut | Une classe contient des colonnes qui ne sont pas ses clés, ou une table à clé n'a pas de classe parce que son concept est une propriété. | Déplacez les colonnes vers une propriété, ou réancrez la table. |
| Étiquette sur le mauvais type | Un terme qui n'est pas une propriété porte une étiquette que seule une propriété transmet aux colonnes. | Retirez l'étiquette. |
| Retrait bloqué | Un terme obsolète a dépassé sa date de retrait prévue mais contient encore des colonnes ou des tables. | Retirez-le avec un successeur. |
| Aucune donnée | Un concept en brouillon n'atteint aucune donnée. | Reliez-le à un terme qui contient des données. |
| Approuvé, non ancré | Un terme approuvé n'atteint aucune donnée, donc aucun agent ne le voit. | Reliez-le à un terme qui contient des données. |
| Ancré · chemin en attente d'approbation | Chaque chemin vers les données passe par un brouillon. Le constat nomme le brouillon dont l'approbation ancre le terme. | Ouvrez ce terme et approuvez-le. |

Les constats signalent ; ils ne changent rien tant que vous n'agissez pas.

## Export de métadonnées {: #metadata-export }

L'ontologie est publiée vers des catalogues de données externes dans le cadre de l'export de métadonnées. Les termes approuvés sont publiés, chacun avec son ancrage : `grounded`, `grounded_pending` ou `not_grounded`. Un brouillon ne quitte jamais Provisa. L'ancrage pour l'export est jugé sur les colonnes et les tables effectivement publiées. Un terme dont toutes les colonnes sont retenues de l'export, parce que leurs tables ne sont pas des produits de données ou parce que des filtres techniques les excluent, est retenu avec elles.

Un terme approuvé qu'un curateur a depuis marqué obsolète est publié avec son statut, sa raison et sa date de retrait prévue, si bien que chaque catalogue peut le faire correspondre à son propre marqueur d'obsolescence.

Les arêtes de relation ne sont publiées que si les deux termes d'extrémité le sont. Les actifs de colonne s'exportent indépendamment, donc exclure un terme ne cache pas les données sous-jacentes.

### Exclure un terme de l'export {: #excluding-a-term-from-export }

Certaines colonnes portent de la plomberie, comme des identifiants de lot ETL, des versions de ligne et des horodatages d'ingestion. Un terme dérivé d'une telle colonne peut avoir une définition exacte sans être pour autant du vocabulaire métier. Le contrôle **Exclure de l'export de métadonnées** retient le terme, ainsi que toute arête de relation qui aboutit sur lui, des catalogues vers lesquels Provisa publie. Les colonnes elles-mêmes s'exportent toujours comme actifs.

Ce qui compte, c'est si le métier emploie le mot. Un identifiant de lot ETL a un sens clair qui a sa place dans l'ontologie pour les ingénieurs, mais pas dans un catalogue métier à côté de `customer` et `revenue`.

## Travailler avec l'ontologie {: #working-with-the-ontology }

Ouvrez **Ontologie** dans la barre de navigation. Le panneau de gauche liste chaque terme avec son rôle, son statut sauf s'il est approuvé, et sa puce d'ancrage. Les filtres **Statut** et **Ancrage** restreignent la liste selon chacun, la case **Obsolète** affiche ou masque les termes obsolètes, **À examiner** la restreint aux termes ayant des constats, et **Lié à** la restreint aux termes ayant un lien Lié à. Cliquez sur un terme pour ouvrir son panneau.

Le panneau montre côte à côte le rôle, le statut et l'ancrage du terme, et pour une propriété sa marque PII avec le nombre de colonnes qu'elle couvre. Un terme ⏚ atténué nomme les brouillons dont l'approbation l'ancrerait. Un terme obsolète ou retiré montre qui a changé son statut et pourquoi. Un terme ayant des constats s'ouvre sur une section **À examiner** : cliquez sur un constat pour voir ce qui a été trouvé et l'action qui le corrige.

### Récapitulatif d'intendance {: #stewardship-summary }

Quand aucun terme n'est ouvert, le panneau montre le récapitulatif d'intendance. Il liste les termes que les agents peuvent voir, les termes ⏚ atténués qui attendent chacun une approbation, avec le terme à approuver, et les termes approuvés qui n'atteignent aucune donnée. **Approuver les brouillons définis et ancrés** liste chaque brouillon qui a une définition et est ancré ; décochez ceux dont la définition n'est pas juste, et approuvez les autres ensemble. Le bouton presse-papiers de l'en-tête fait revenir le récapitulatif.

### Voir l'impact d'abord {: #seeing-the-impact-first }

Approuver, retirer et lier un terme changent chacun ce qui atteint les données. Avant chacune de ces actions, la page montre ce que l'action changerait : combien de termes deviennent ancrés, ⏚ atténués ou non ancrés, et vers combien de tables les agents gagnent ou perdent un chemin. Par exemple, « Approuver region ancre 3 termes et donne aux agents un chemin vers 2 tables de plus ». **Approuver** ouvre l'aperçu avec son bouton de confirmation, **Retirer** le montre sous le successeur que vous choisissez, et ancrer une table dans le formulaire de modification le montre sous la table avant que vous enregistriez.

**Modifier** ouvre le terme entier dans un seul formulaire. Enregistrer applique toutes les modifications ensemble.

- **Renommez** le terme pour changer sa formulation sans déplacer ses colonnes.
- **Ajoutez une définition** en la saisissant, ou cliquez sur le bouton de brouillon IA pour générer un point de départ à partir du nom du terme, de ses colonnes et de ses relations. Le brouillon n'est enregistré que lorsque vous enregistrez le formulaire.
- **Revendiquez des colonnes** pour déplacer des colonnes d'autres termes vers celui-ci, ou choisissez **Déplacer vers un terme** à côté de l'une de ses propres colonnes. Chaque colonne appartient à un terme. Les colonnes de clé suivent leur classe et ne peuvent pas être déplacées.
- **Ancrez des tables** sur une classe, ou retirez-les.
- **Marquez les PII** sur une propriété.
- **Ajoutez une relation**, en choisissant son type. Changez le type d'une arête existante sur place au lieu de la supprimer et de la rajouter.
- **Assignez des experts** par identifiant utilisateur, comme `expert` ou `author`.

À côté de Modifier, le panneau propose les actions de statut que le terme permet : **Approuver** un brouillon défini, **Marquer obsolète** un terme approuvé, **Retirer** tout terme en service, et **Restaurer** un terme obsolète ou retiré.

Depuis le menu ✨, **Définitions** remplit chaque définition vide en une passe et n'écrase jamais le texte d'une personne. **Relations** ajoute des arêtes typées sur toute la liste des termes ; les propositions qui nomment des termes inconnus, deux fois le même terme ou un type non reconnu sont écartées. Chaque Lié à qu'il ajoute est listé pour examen, comme le décrit [Comment l'assistant de relations choisit](#how-the-relationship-assistant-chooses).

Le bandeau **Brouillon** sur un terme vous dit s'il lui faut encore une définition (donnez un alias à la colonne ou ajoutez-en une) ou seulement votre approbation. Un bandeau distinct indique quand un terme n'atteint aucune donnée : reliez-le à un terme qui a des données.

## Interroger l'ontologie {: #querying-the-ontology }

L'ontologie est un schéma que vous pouvez interroger, nommé `ontology`, à côté du schéma `meta` qui décrit la structure du modèle. Ses tables commencent par un tiret bas, si bien que la table propre d'une classe ne partage jamais son nom avec l'une d'elles.

| Table | Une ligne par |
|---|---|
| `_terms` | Terme, avec son statut, son ancrage, sa définition et s'il est une classe, une propriété ou les deux |
| `_term_refs` | Liaison d'une propriété à une colonne, avec la table et le domaine de la colonne |
| `_term_edges` | Relation entre deux termes |
| `_domains` | Domaine auquel appartient un terme |
| `_experts` | Expert ou auteur assigné à un terme |
| `_axioms` | Énoncé de l'ontologie en RDFS, OWL et SKOS |
| `_closure` | Terme associé à chaque terme dont il est un type, à toute profondeur, et à lui-même |

Une liaison relie un terme à une colonne du schéma `meta`, si bien que vous pouvez joindre `ontology._term_refs` à `meta.table_columns` et `meta.registered_tables`. Les tables de l'ontologie sont en lecture seule. Elles changent à mesure que vous curez les termes.

Vous ne lisez un terme que si vous atteignez l'un de ses domaines. Un terme sans domaine est visible de tous, de même qu'un terme d'entreprise (`*`). Une relation ou un axiome est visible quand ses deux termes le sont.

### L'ontologie en SPARQL {: #the-ontology-in-sparql }

SPARQL restitue l'ontologie dans les vocabulaires standard, si bien qu'un outil RDF la lit comme n'importe quelle ontologie OWL :

- une classe est une `owl:Class`, et une relation de type entre classes est `rdfs:subClassOf` ;
- une propriété est une `owl:DatatypeProperty`, ou une `owl:ObjectProperty` quand chaque colonne qu'elle lie est une clé vers une table dotée d'une classe ;
- le `rdfs:domain` d'une propriété est la classe de chaque table dont elle lie une colonne, et son `rdfs:range` est la classe vers laquelle mènent ses clés ou le type XML Schema de ses colonnes ;
- une relation de type entre propriétés est `rdfs:subPropertyOf` ;
- chaque terme a son nom comme `skos:prefLabel` et sa définition comme `skos:definition`.

Un terme est nommé par son IRI de vocabulaire, `<your Provisa address>/vocab/term/<name>`, l'IRI qui type les lignes des tables qu'il ancre. Un terme retiré n'énonce rien.

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?class ?parent ?label WHERE {
  ?class a owl:Class ; rdfs:subClassOf ?parent .
  ?parent skos:prefLabel ?label .
}
```

Une requête peut joindre l'ontologie aux lignes que vous lisez. Les instances d'une classe incluent celles de chaque classe qui en est un type, à toute profondeur :

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX term: <https://your-provisa/vocab/term/>

SELECT ?party ?class WHERE {
  ?party a ?class .
  ?class rdfs:subClassOf* term:Party .
}
```

Une propriété fonctionne de la même façon : `?x ?p ?v . ?p rdfs:subPropertyOf* term:amount` lit les valeurs de chaque propriété qui est un type de `amount`. Vous obtenez les instances des tables que vous lisez, si bien qu'une classe dont vous ne pouvez pas lire les tables n'en ajoute aucune.

Les mêmes énoncés sont des lignes de `ontology._axioms` en SQL, et les paires de type, fermées sur toute profondeur, sont des lignes de `ontology._closure`. GraphQL, Cypher et les autres langages de requête ne proposent pas ces deux tables.

## Voir aussi {: #see-also }

- [Export de métadonnées](metadata-export.md) explique comment les termes et les relations sont publiés vers des catalogues de données externes.
- [Lignage au niveau des colonnes](lineage.md) présente l'explorateur de lignage et la façon dont une colonne physique signale les termes qui lui sont liés.

## Le schéma de l'ontologie {: #the-ontology-schema }

Le schéma système `ontology` rend le vocabulaire interrogeable depuis SQL, GraphQL, Cypher, REST et MCP, les mêmes surfaces que vous utilisez pour vos propres données. Il se trouve à côté de `meta`, qui contient la structure du modèle ; `ontology` en contient le sens.

Deux parties composent le schéma : les vues de définition et les tables de classe.

### Vues de définition {: #definition-views }

Cinq vues exposent le contenu du glossaire :

| Vue | Contenu |
| --- | --- |
| `_terms` | Chaque terme : son nom, son rôle (classe, propriété, concept), sa définition, son statut et son ancrage. |
| `_term_refs` | Chaque liaison terme-colonne : le nom du terme, la source, la table et la colonne. |
| `_term_edges` | Chaque arête de relation : terme source, type d'arête et terme cible. |
| `_domains` | Appartenance aux domaines : le nom du terme et chaque domaine auquel il appartient. |
| `_experts` | Affectations d'experts : le nom du terme, l'identifiant utilisateur et le rôle (`expert` ou `author`). |

Une sixième vue, `_axioms`, contient chaque triplet RDFS/OWL/SKOS que le schéma peut exprimer, un par ligne, avec les colonnes `subject`, `predicate`, `object`, `object_iri` et `object_literal`. `object` nomme l'objet quand c'est un terme ; `object_iri` le contient quand c'est une IRI, et `object_literal` quand c'est un littéral. L'une des trois contient la valeur sur chaque ligne. Les outils qui préfèrent les triplets aux lignes tabulaires lisent ici.

Une **liaison de terme**, c'est-à-dire un terme lié à une colonne et consigné dans `_term_refs`, diffère d'une liaison de commande. Une liaison de terme dit ce que signifie une colonne ; une liaison de commande branche une fonction dans le pipeline de requêtes.

### Tables de classe {: #class-tables }

Chaque terme de classe non retiré devient une vue du schéma `ontology` nommée d'après ce terme, les espaces étant remplacés par des tirets bas. Une classe ancrée dans une table produit une vue ; une classe ancrée dans plusieurs tables produit l'union de ces tables.

Dans une vue d'union, les lignes de chaque table source apparaissent. La colonne `_source_table` indique de quelle table vient chaque ligne : elle contient le nom qualifié de la table, `domain.table`. Une colonne de propriété qu'une table source ne lie pas vaut `NULL` pour ses lignes. Une propriété liée à plusieurs colonnes d'une même table devient une colonne de tableau.

Chaque colonne d'une vue de classe correspond à une propriété liée. La vue est gouvernée par le même pipeline que toute autre lecture.

**Exemple.** Une classe `customer` ancrée dans `sales.customers` et `crm.contacts`, avec une propriété `name` liée à `customers.name` et `contacts.full_name`, et une propriété `segment` liée seulement à `customers.segment` :

```sql
-- rows from both tables; segment is NULL for crm.contacts rows
SELECT _source_table, name, segment
FROM ontology.customer
WHERE segment = 'enterprise';
```

### Droits de lecture {: #read-rights }

Les mêmes règles de gouvernance que pour vos propres tables s'appliquent ici.

Un terme n'apparaît dans les vues de définition que si vous atteignez au moins l'un de ses domaines. Un terme sans domaine est visible de tous. Une ligne de liaison n'apparaît que si sa colonne vous est servie. Une arête n'apparaît que si ses deux termes vous sont visibles. Une colonne de propriété dans une vue de classe n'apparaît que si ses colonnes sous-jacentes sont servies.

Aucun accès particulier n'est nécessaire. Si vous pouvez interroger une table, vous pouvez interroger les termes de classe qui y sont ancrés.

### SPARQL {: #sparql }

Le point de terminaison SPARQL sert le schéma `ontology` comme une ontologie RDFS/OWL/SKOS standard. Chaque terme de classe devient une `owl:Class`, chaque arête de type devient `rdfs:subClassOf`, chaque terme de propriété devient `owl:DatatypeProperty` ou `owl:ObjectProperty` avec `rdfs:domain` et `rdfs:range`, les arêtes de sous-propriété deviennent `rdfs:subPropertyOf`, les noms deviennent `skos:prefLabel` et les définitions `skos:definition`. Les mêmes droits de lecture s'appliquent.

L'IRI de base de tous les termes est dérivée du paramètre `server.public_url`, sous le chemin `/vocab/term/`. Un terme de classe nommé `customer` a l'IRI `{public_url}/vocab/term/customer`.

Trois requêtes qu'écrirait un ontologue :

**Trouver tous les sous-types de Party, à toute profondeur :**

```sparql
PREFIX : <{public_url}/vocab/term/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?c WHERE {
  ?c rdfs:subClassOf* :Party .
}
```

**Trouver les clients ayant une valeur de segment, par l'IRI du terme :**

```sparql
PREFIX : <{public_url}/vocab/term/>

SELECT ?x ?s WHERE {
  ?x a :Customer ;
     :segment ?s .
}
```

**Lister toutes les propriétés déclarées sur Customer avec leurs plages :**

```sparql
PREFIX : <{public_url}/vocab/term/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?p ?range WHERE {
  ?p rdfs:domain :Customer ;
     rdfs:range ?range .
}
```

Remplacez `{public_url}` par l'adresse publique de votre déploiement, telle que configurée dans `server.public_url`.
