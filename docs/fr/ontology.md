# Ontologie {: #ontology }

L'ontologie est un vocabulaire vivant posé sur votre modèle de données. Chaque colonne physique correspond à un terme, et les colonnes de même signification partagent un seul terme, quelle que soit leur graphie. Chaque terme peut porter une définition, des relations typées vers d'autres termes, une liste d'experts métier et des étiquettes comme `pii` que porte chaque colonne de cette signification.

C'est un graphe de concepts, de leurs définitions et des relations entre eux. Provisa le relie aux tables et aux colonnes qui contiennent les valeurs, et c'est ce qui lui permet de répondre à des questions sur vos données. Un terme est un brouillon tant qu'il n'a pas de définition et n'atteint pas les données. L'ontologie est distincte des métriques : un terme dit ce qu'est un concept, une métrique dit comment un nombre est calculé.

Ce vocabulaire partagé relie le langage métier aux données physiques. Un agent IA qui sait que « customer » désigne chaque colonne portant un identifiant client n'a pas à deviner laquelle de `cust_id`, `customerId` et `CUSTOMER_KEY` est la bonne. Elles correspondent toutes au même terme, et le terme porte la définition.

## Classes, propriétés et concepts {: #classes-properties-and-concepts }

Chaque terme a exactement un rôle, déduit de ce dans quoi il est ancré. Vous ne le fixez jamais.

| Rôle | Ancré dans | Exemple |
| --- | --- | --- |
| **Classe** | Une ou plusieurs tables ou vues. Ses instances sont leurs lignes. | `customer`, ancré dans `customers` et `crm_contacts` |
| **Propriété** | Des colonnes qui ne sont pas des clés. | `customer email`, qui porte `customers.email` |
| **Concept** | Rien directement. Il atteint les données par ses relations. | `revenue`, avec des arêtes `PART_OF` depuis `order amount` et `refund amount` |

Un terme est l'un ou l'autre. Provisa refuse d'ancrer une table dans un terme qui porte des colonnes non clés, et refuse de déplacer une colonne non clé vers un terme ancré dans des tables. Chaque refus nomme le terme et ce qu'il porte déjà.

### Chaque table à clé propose une Classe {: #every-keyed-table-proposes-a-class }

Quand une table dotée d'une clé primaire est enregistrée, Provisa l'ancre dans un terme Classe en brouillon, nommé d'après le concept de la table : `orders` propose `order`, `order_lines` propose `order line`. Les tables d'un même concept partagent une Classe. Une table sans clé primaire ne peut pas être une Classe, et son ancrage est refusé en nommant la table.

Un ancrage que vous faites vous-même, dans l'interface, dans la configuration ou via Polly, est curé. La synchronisation ne le déplace jamais, et ancrer une table dans un terme curé remplace sa proposition en brouillon. Quand un concept est déjà une Propriété avec des colonnes non clés, ses tables ne reçoivent pas de proposition ; l'ontologie les signale plutôt sous **À examiner**.

### Les clés appartiennent à la Classe {: #keys-belong-to-the-class }

Une clé primaire n'a pas de terme propre. Chacune de ses colonnes, y compris les parties d'une clé composite, place sa référence sur la Classe de sa table.

Une clé étrangère fonctionne de la même façon. Chaque colonne du côté « plusieurs » d'une relation déclarée place sa référence sur la Classe de la table cible, quel que soit son nom. `manager_id` sur `employees`, qui pointe vers `employees`, aboutit sur `employee` ; le rôle « manager » vit dans le nom de la relation. Une colonne sans relation déclarée est une Propriété, même quand son nom correspond à une Classe.

Provisa place les références de clé par dérivation uniquement, chaque fois qu'une table se synchronise, qu'une relation change ou que la Classe d'une table change. Vous ne pouvez pas déplacer une colonne clé à la main ; le refus nomme la colonne, sa table et sa Classe. Ancrez une table dans un autre terme, et ses clés suivent aussitôt.

### La même entité dans plusieurs tables {: #the-same-entity-in-several-tables }

Les instances de tables différentes sont distinctes tant que vous ne dites pas le contraire. Quand `customers` et `crm_contacts` décrivent tous deux le même client, marquez la relation qui les joint comme **même entité**. Elle doit joindre deux tables qu'ancre la même Classe, de un à un, sur une clé de chaque côté : la clé primaire ou une contrainte d'unicité déclarée.

Tant qu'une relation de même entité existe sur une table, sa Classe ne peut être ni retirée ni remplacée. Retirez d'abord la marque de la relation.

Une Classe ancrée dans plusieurs tables que les relations de même entité ne joignent pas toutes est signalée sous **À examiner** comme *tables non liées*.

## Comment les termes sont dérivés {: #how-terms-are-derived }

Provisa crée automatiquement un terme en brouillon à partir de chaque nom de colonne, selon une règle de normalisation déterministe : uniformisation de la casse, découpage aux séparateurs et en camelCase, développement des abréviations et suppression des jetons proxy finaux.

Le **développement des abréviations** remplace les abréviations d'entreprise courantes par leur forme complète : `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity`, etc. `id` comme `key` se développent en `identifier`. La table est fixe et prudente. Les abréviations ambiguës comme `st`, `min` et `no` restent telles quelles, pour ne rien deviner de travers.

La **suppression des jetons proxy** retire un jeton final `identifier`, `code`, `index` ou `reference`. Une colonne nommée `cust_id` ne désigne pas l'identifiant lui-même ; elle désigne un client par une valeur de substitution. Retirer le proxy fait aboutir `cust_id` et `customerId` sur le terme `customer`. Seuls les jetons finaux sont retirés, et jamais le dernier restant, si bien qu'une colonne `id` seule devient `identifier` et en reste là.

La **déduplication** est le but. La règle est déterministe, donc `cust_id`, `customerId` et `CUSTOMER_KEY` donnent tous `customer`. Chaque colonne reçoit une référence sur ce terme unique, et la curation a un seul endroit où ajouter la définition au lieu de trois.

Les colonnes clés échappent à cette règle. Leurs références vont à une Classe, comme décrit plus haut.

### Expressions génériques {: #generic-phrases }

Certaines expressions normalisées sont trop génériques pour former un concept à elles seules. Une colonne `name`, `date` ou `identifier` seule désigne un attribut du concept de sa table, qui n'existe pas en dehors de cette table. Les employés ont des noms, les produits aussi, mais ce n'est pas la même chose.

Quand une expression appartient à l'ensemble générique et qu'un contexte de table est disponible, le terme est qualifié en `<concept de la table> <expression>` : `employees.first_name` se normalise en `employee first name`.

L'ensemble générique couvre les noms d'attribut (`name`, `date`, `status`, `type`, `amount`, `quantity`), les expressions d'audit (`created_at`, `modified_by`, `submitted_timestamp`) et quelques autres qui figurent sur presque toutes les tables.

### Les termes suivent le nom métier {: #terms-follow-the-business-name }

Un terme dérivé suit le **nom métier** de la colonne : son alias quand le modélisateur en a défini un, son nom physique sinon. Quand `usr_nm` a pour alias `user name`, le terme dérivé est `user name`, là où développer `usr_nm` aurait pu donner `user number`.

Donner un alias à une colonne est la correction la plus forte. Un alias se propage à toutes les surfaces qui lisent la colonne (SQL, GraphQL, agents IA, catalogue), si bien que le modèle se décrit correctement partout. Renommer le terme corrige une entrée de catalogue et laisse la colonne nommée `usr_nm` pour le lecteur suivant. Le bandeau Brouillon de l'interface le dit : donnez d'abord un alias à la colonne, et ne renommez le terme que si le nom de la colonne est juste et le vocabulaire non.

Changer l'alias d'une colonne redérive son terme en brouillon, si bien que l'ontologie suit le modèle et ne demande jamais deux fois la même correction. Dès qu'un curateur a ajouté à un terme une définition, une relation, un expert ou une étiquette, une modification d'alias laisse la colonne où elle est, car ce travail est celui du curateur et il reste.

### Noms de table de chemin d'accès {: #access-path-table-names }

Certains noms de table décrivent un chemin d'accès plutôt qu'un concept : `user_by_name` est un utilisateur atteint par une recherche sur le nom, et non un type d'entité distinct. Quand Provisa dérive le concept d'une table, il coupe le nom au connecteur, si bien que `user_by_name` devient `user` et `orders_by_customer` devient `order`.

Sans la coupe, `user_by_name` proposerait une Classe `user name` qui entrerait en collision avec l'attribut réel `users.name`. La coupe s'applique aux concepts de table seulement. Dans un nom de colonne, `by` fait partie du nom composé, donc `pet_by_name` et `pet_name` se normalisent vers le même terme, `pet name`.

## Statut {: #status }

Un terme a un seul statut à la fois.

| Statut | Signification | Comment il y arrive |
| --- | --- | --- |
| **Brouillon** | Indisponible pour les agents, la recherche et les catalogues tant qu'il n'a pas de définition et n'atteint pas les données. | Tout terme dérivé commence ici. |
| **Actif** | Admis : proposé aux agents, à la recherche et aux catalogues. | Défini, ancré et en service. |
| **Obsolète** | En voie de sortie. | Un curateur marque obsolète un terme actif, ou un terme curé perd sa dernière colonne. |
| **Retiré** | Hors service, conservé comme trace. | Un curateur le retire et nomme un successeur. |

### La règle d'admission {: #the-admission-rule }

Un terme est actif quand il remplit trois conditions.

1. **En service.** Il n'est pas retiré, et il n'est pas devenu obsolète en perdant sa dernière colonne. Un terme qu'un curateur a marqué obsolète reste en service jusqu'à son retrait prévu.
2. **Défini.** Il porte une définition. Un terme dérivé d'un nom de colonne n'est qu'un jeton. Sans définition, c'est un brouillon qui attend un curateur, jamais un vocabulaire sur lequel un agent peut fonder une question.
3. **Ancré.** Il est relié, par des termes en service, à un terme qui porte une colonne ou une table. L'ontologie est un point d'entrée dans les données, donc chaque chaîne doit aboutir aux données.

La connectivité se propage dans le graphe. Un Concept atteint les données par n'importe quel voisin en service qui les atteint. Les termes hors service ne conduisent pas, donc un terme retiré ne maintient pas ses dépendants en vie.

### Marquer un terme obsolète {: #deprecating-a-term }

Marquez obsolète un terme actif quand il est en voie de sortie mais encore utilisé. Vous donnez un motif, obligatoire, et éventuellement une date de retrait prévue, aujourd'hui ou plus tard. Le terme reste en service, et continue de compter pour la règle d'admission, jusqu'à cette date. Le planificateur le retire alors.

Un terme arrivé à échéance qui porte encore des colonnes ou des tables ne peut pas être retiré par la date, car seul un successeur peut les reprendre. Il attend, et **À examiner** le signale comme *retrait bloqué* jusqu'à ce que vous le retiriez avec un successeur.

Un terme curé qui perd sa dernière colonne devient obsolète automatiquement et est conservé. Il sort de service, garde le contenu fourni par ses rédacteurs et revient si la même colonne réapparaît. Un terme non curé qui n'a plus de colonne est supprimé.

### Retirer un terme {: #retiring-a-term }

Retirer un terme déplace tout ce qu'il porte vers un **successeur**, puis le met hors service.

- Les colonnes d'une Propriété passent au successeur. Il doit être une Propriété ou un Concept.
- Les tables d'une Classe passent au successeur, et leurs clés les suivent. Il doit être une Classe ou un Concept.
- Un Concept ne porte rien, donc son successeur est facultatif.

Le retrait demande un motif et écrit une arête `REPLACES` du successeur vers le terme retiré. Le terme retiré reste une trace inerte : sa définition, ses étiquettes, ses relations, ses experts et le motif. Il ne conduit rien.

Comme ses colonnes appartiennent désormais au successeur, le panneau d'un terme retiré montre sa **dernière application** : ses colonnes et tables telles que le modèle les tenait dans le commit précédant le retrait. Le panneau renvoie aussi vers le terme qui le remplace.

### Restaurer un terme {: #restoring-a-term }

**Restaurer** annule ce qu'un curateur a déclaré. Un terme obsolète cesse de l'être. Un terme retiré revient comme Concept vide, en brouillon, puisque ses colonnes sont allées à son successeur, et son arête `REPLACES` est supprimée.

### Ce qui rend un terme curé {: #what-makes-a-term-curated }

Un terme né de la normalisation des colonnes commence vide. Il devient curé dès qu'il a l'un de ces éléments :

- une définition enregistrée
- une arête de relation
- un expert métier assigné
- une étiquette, comme `pii`
- un retrait

Les termes curés sont conservés quand leurs colonnes partent. La synchronisation ne redérive jamais les colonnes d'un terme curé vers un autre.

## Resynchroniser depuis les tables {: #re-syncing-from-tables }

Chaque fois qu'une table est enregistrée ou rechargée, Provisa rapproche ses colonnes de l'ontologie. Les nouvelles colonnes créent ou relient des termes, les colonnes disparues perdent leurs références, et tout terme laissé sans référence est supprimé ou marqué obsolète selon la règle ci-dessus. Les colonnes clés vont à leur Classe, et une table à clé sans Classe en propose une.

Seuls les termes non curés sont redérivés. Si vous donnez un alias à une colonne et que son terme en brouillon change, la colonne passe au nouveau terme. Si le terme est curé, la colonne reste, car une modification d'alias ne l'emporte pas sur le choix d'un curateur.

Quand supprimer un terme laisserait un Concept sans chemin vers les données, Provisa marque plutôt ce terme obsolète, pour que la structure que vous avez construite subsiste jusqu'à ce que vous la recâbliez.

## Relations {: #relationships }

Les termes sont reliés à d'autres termes par des arêtes typées.

| Type | Signification |
| --- | --- |
| `KIND_OF` | Le terme source est une sorte du terme cible. |
| `PART_OF` | Le terme source est un composant du terme cible. |
| `SYNONYM_OF` | Les deux termes sont interchangeables dans ce domaine. |
| `RELATED_TO` | Une association lâche, quand aucune affirmation plus forte ne convient. |
| `VALID_VALUE_OF` | La source est une valeur admise de l'énumération ou du domaine cible. |
| `DERIVED_FROM` | La source est calculée ou tirée de la cible. |
| `REPLACES` | La source remplace la cible. Retirer un terme en écrit une depuis son successeur. |
| `PREFERRED_TERM_FOR` | La source est le terme préféré au terme cible déconseillé. |
| `TRANSLATION_OF` | La source est une traduction de la cible dans une autre langue ou locale. |
| `ANTONYM_OF` | La source est le contraire sémantique de la cible. |

Les relations sont orientées. L'interface montre les arêtes sortantes (ce terme → un autre) et entrantes (un autre terme → ce terme), en étiquetant chaque sens avec sa propre formulation claire.

Les arêtes résident dans une table d'association déclarée comme relation de jonction. Sa colonne de type de relation est le discriminant, si bien que chaque type ci-dessus est un type de relation Cypher distinct entre deux nœuds de terme. Rien n'y est propre à l'ontologie : elle est déclarée comme vous déclareriez une jonction sur vos propres tables, et le même code la lit.

## Concepts {: #concepts }

Un Concept n'a ni colonnes ni tables propres. Utilisez-en un pour une idée métier qui couvre plusieurs termes concrets, puis reliez-le aux termes qui portent des données. `revenue`, par exemple, pourrait être un Concept, avec des arêtes `PART_OF` depuis `order amount`, `adjustment amount` et `refund amount`.

Un Concept qui n'atteint aucune colonne ni table par le graphe des relations ne désigne aucune donnée. Il n'apparaît ni dans la recherche de termes des agents ni dans l'export de métadonnées, et **À examiner** le signale comme *aucune donnée*. Les domaines ne s'appliquent qu'à un Concept. Tout autre terme tire ses domaines des tables dans lesquelles il est ancré ou des colonnes qu'il porte.

## PII selon la signification {: #pii-on-meanings }

La PII est une propriété de ce que signifie une colonne, donc Provisa vous laisse la marquer une fois, sur le terme, et couvre chaque colonne de cette signification, y compris celles enregistrées plus tard.

L'étiquette `pii` se pose sur une colonne ou sur un terme Propriété, jamais sur une source, une table, une Classe ou un Concept. Un terme doit être défini et en service (actif ou obsolète) pour la porter. Marquer un terme en brouillon est refusé avec « Définissez *terme* pour le marquer », et vous ne pouvez pas effacer la définition d'un terme marqué tant que vous ne l'avez pas démarqué.

Chaque colonne d'un terme marqué porte `pii`, et `sensitive` avec, partout où les étiquettes sont lues : gouvernance, masquage, sensibilité, export et vues d'étiquettes. Rien n'est copié sur les colonnes. Une colonne déplacée vers le terme porte l'étiquette aussitôt, et une colonne qui en sort ne la porte plus. Chaque colonne indique d'où vient son étiquette : posée sur la colonne, avec son motif, ou via un terme, qu'elle nomme.

Quand vous marquez une colonne dont le terme est une Propriété, l'interface demande s'il faut marquer les autres colonnes de même signification.

- **Oui** pose l'étiquette sur le terme.
- **Celle-ci seulement** marque la colonne seule et demande pourquoi.

Une colonne qui est PII via son terme ne peut pas être démarquée seule. Démarquez le terme, ou déplacez la colonne vers le terme qui correspond à ce qu'elle contient.

### Déplacements qui font perdre des étiquettes {: #moves-that-lose-tags }

Déplacer une colonne vers un autre terme peut lui ôter une étiquette : déplacer `customers.email` de `customer email` (marqué `pii`) vers un terme non marqué laisserait la colonne d'e-mail sans protection. Chaque déplacement qu'une personne effectue est vérifié d'abord. Cela couvre revendiquer ou déplacer une colonne, retirer un terme, et enregistrer ou supprimer une relation qui reloge des colonnes clés.

Quand une colonne cesserait complètement de porter `pii`, le déplacement est refusé jusqu'à ce que vous décidiez. L'interface montre un récapitulatif de ce que chaque colonne déplacée perd et gagne, les pertes en premier.

- **Déplacer** va de l'avant, chaque étiquette perdue étant confirmée.
- **Déplacer et marquer** reporte les étiquettes perdues sur le terme vers lequel vont les colonnes, dans la même modification.
- **Annuler** ne déplace rien.

Une colonne qui garde l'étiquette grâce à sa propre marque n'a besoin d'aucune confirmation. Les déplacements que le modèle fait lui-même, comme redériver les colonnes d'un brouillon, ne changent jamais une étiquette héritée.

## À examiner {: #to-review }

L'ontologie signale sous forme de constats ce qui demande votre attention. Un terme qui a un constat est **À examiner**, et la liste des termes se filtre là-dessus. Un simple brouillon n'est pas un constat ; les brouillons sont l'état ordinaire de l'ontologie.

| Constat | Ce qu'il signifie | La correction |
| --- | --- | --- |
| Tables non liées | Une Classe ancre plusieurs tables que les relations de même entité ne joignent pas toutes. | Marquez des relations de même entité entre elles. |
| Classe et attribut | Une Classe porte des colonnes qui ne sont pas ses clés, ou une table à clé n'a pas de Classe parce que son concept est une Propriété. | Déplacez les colonnes vers une Propriété, ou réancrez la table. |
| Étiquette du mauvais type | Un terme qui n'est pas une Propriété porte une étiquette que seule une Propriété transmet aux colonnes. | Retirez l'étiquette. |
| Retrait bloqué | Un terme obsolète a dépassé son retrait prévu mais porte encore des colonnes ou des tables. | Retirez-le avec un successeur. |
| Aucune donnée | Un Concept en service n'atteint aucune donnée. | Reliez-le à un terme qui porte des données. |

Les constats signalent ; ils ne changent rien tant que vous n'agissez pas.

## Export de métadonnées {: #metadata-export }

L'ontologie est publiée vers des catalogues de données externes dans le cadre de l'export de métadonnées. Seuls les termes actifs sont publiés, jugés sur les colonnes et les tables effectivement publiées. Un terme dont toutes les colonnes sont retenues hors de l'export, parce que leurs tables ne sont pas des produits de données ou que des filtres techniques les excluent, n'est pas ancré pour l'export, même s'il porte des colonnes dans le plan de contrôle.

Un terme obsolète est publié avec son statut, son motif et sa date de retrait prévue, pour que chaque catalogue le relie à son propre marqueur d'obsolescence.

Les arêtes de relation ne sont publiées que lorsque les deux termes aux extrémités le sont. Les actifs de colonne s'exportent indépendamment, donc exclure un terme ne masque pas les données sous-jacentes.

### Exclure un terme de l'export {: #excluding-a-term-from-export }

Certaines colonnes portent de la plomberie, comme des identifiants de lot ETL, des versions de ligne et des horodatages d'ingestion. Un terme dérivé d'une telle colonne peut avoir une définition exacte sans être pour autant du vocabulaire métier. Le contrôle **Exclure de l'export de métadonnées** retient le terme, et toutes les arêtes de relation qui y aboutissent, hors des catalogues vers lesquels Provisa publie. Les colonnes elles-mêmes s'exportent toujours comme actifs.

Ce qui compte, c'est que le métier emploie le mot. Un identifiant de lot ETL a un sens clair qui a sa place dans l'ontologie pour les ingénieurs, mais pas dans un catalogue métier à côté de `customer` et `revenue`.

## Travailler avec l'ontologie {: #working-with-the-ontology }

Ouvrez **Ontologie** dans la barre de navigation. Le panneau de gauche liste chaque terme avec son rôle et, s'il n'est pas actif, son statut. Les cases **Obsolète** et **Brouillon** affichent ou masquent ces termes, et **À examiner** restreint la liste aux termes qui ont des constats. Cliquez sur un terme pour ouvrir son panneau.

Le panneau montre le rôle et le statut du terme et, pour une Propriété, sa marque PII avec le nombre de colonnes couvertes. Un terme obsolète ou retiré indique qui a changé son statut et pourquoi. Un terme qui a des constats s'ouvre sur une section **À examiner** : cliquez sur un constat pour voir ce qui a été trouvé et l'action qui le corrige.

**Modifier** ouvre le terme entier dans un seul formulaire. Enregistrer applique toutes les modifications ensemble.

- **Renommez** le terme pour changer sa formulation sans déplacer ses colonnes.
- **Ajoutez une définition** en la saisissant, ou cliquez sur le bouton de brouillon IA pour générer un point de départ à partir du nom du terme, de ses colonnes et de ses relations. Le brouillon n'est enregistré que lorsque vous enregistrez le formulaire.
- **Revendiquez des colonnes** d'autres termes pour celui-ci, ou choisissez **Déplacer vers un terme** à côté de l'une de ses colonnes. Chaque colonne appartient à un seul terme. Les colonnes clés suivent leur Classe et ne se déplacent pas.
- **Ancrez des tables** dans une Classe, ou retirez-les.
- **Marquez PII** sur une Propriété.
- **Ajoutez une relation** en choisissant son type. Changez le type d'une arête existante sur place au lieu de la supprimer et de la recréer.
- **Assignez des experts** par identifiant d'utilisateur, comme `expert` ou `author`.

À côté de Modifier, le panneau propose les actions de statut que le terme autorise : **Marquer obsolète** un terme actif, **Retirer** tout terme en service et **Restaurer** un terme obsolète ou retiré.

Dans le menu ✨, **Définitions** remplit chaque définition vide en une passe et n'écrase jamais le texte d'une personne. **Relations** propose des arêtes typées sur toute la liste des termes ; les propositions qui nomment des termes inconnus, deux fois le même terme ou un type non reconnu sont écartées.

Le bandeau **Brouillon** d'un terme vous indique s'il est non défini (donnez un alias à la colonne ou ajoutez une définition) ou non ancré (reliez-le à un terme qui porte des données). Tant que vous le voyez, ni les agents ni les catalogues ne peuvent atteindre le terme.

## Voir aussi {: #see-also }

- [Export de métadonnées](metadata-export.md) décrit comment les termes et les relations sont publiés vers des catalogues de données externes.
- [Lignage au niveau des colonnes](lineage.md) décrit l'explorateur de lignage et la façon dont une colonne physique signale les termes qui lui sont liés.
