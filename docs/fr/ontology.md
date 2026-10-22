# Ontologie {: #ontology }

L'ontologie est un vocabulaire vivant posé sur votre modèle de données. Chaque colonne physique du modèle correspond à un terme, et les colonnes qui portent le même concept partagent un seul terme, quelle que soit leur graphie. Chaque terme peut avoir une définition, un ensemble de relations typées vers d'autres termes et une liste d'experts métier qui en possèdent le sens.

C'est un graphe de concepts, de leurs définitions et des relations entre eux. Provisa le fait correspondre aux structures de base de données qui contiennent les valeurs, ce qui lui permet de répondre à des questions sur vos données. Un concept reste un brouillon tant qu'il n'atteint pas les données, directement ou par d'autres concepts. L'ontologie est distincte des métriques : un terme dit ce qu'est un concept, une métrique dit comment un chiffre est calculé.

Ce vocabulaire commun relie le langage métier aux données physiques. Un agent d'IA qui sait que « customer » désigne toute colonne portant un identifiant client n'a pas à deviner laquelle de `cust_id`, `customerId` et `CUSTOMER_KEY` est la bonne. Toutes correspondent au même terme, et c'est le terme qui porte la définition.

## Comment les termes sont dérivés {: #how-terms-are-derived }

Provisa dérive automatiquement un terme de chaque nom de colonne, à l'aide d'une règle de
normalisation déterministe : mise en casse uniforme, découpage sur les séparateurs et le
camelCase, expansion des abréviations et retrait des jetons de substitution finaux.

L'**expansion des abréviations** associe les abréviations d'entreprise courantes à leur forme complète : `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity`, etc. `id` et `key` deviennent tous deux `identifier`. La table est fixe et prudente : les abréviations ambiguës comme `st`, `min` et `no` restent telles quelles, pour ne rien deviner de travers.

**Le retrait des jetons de substitution** supprime un jeton final `identifier`, `code`, `index` ou
`reference`. Une colonne nommée `cust_id` ne nomme pas l'identifiant lui-même ; elle nomme un
client à travers une valeur de substitution. Retirer la substitution fait atterrir `cust_id` et
`customerId` sur le même terme `customer`. Seuls les jetons finaux sont retirés, et jamais le
dernier jeton restant : une colonne `id` seule s'étend en `identifier` et y reste.

La **déduplication** est tout l'objet. La règle de normalisation est déterministe : `cust_id`, `customerId` et `CUSTOMER_KEY` donnent tous `customer`. Chaque colonne reçoit une référence vers ce terme unique, et la curation n'a qu'un endroit où ajouter la définition au lieu de trois.

### Expressions génériques {: #generic-phrases }

Certaines expressions normalisées sont trop génériques pour former un concept à elles seules. Une colonne `name`, `date` ou `identifier` toute seule désigne un attribut du concept de sa table, qui n'existe pas en dehors d'elle. Les employés ont un nom, les produits ont un nom, mais ce n'est pas la même chose.

Lorsqu'une expression appartient à l'ensemble générique et qu'un contexte de table est disponible,
le terme est qualifié en `<table concept> <phrase>` : `employees.first_name` se normalise
en `employee first name`, et `orders.id` se normalise en `order`, parce que le retrait de la
substitution fait ensuite retomber l'expression qualifiée sur le concept qu'elle identifie. Ce
dernier cas est important : la clé primaire de `orders` et chaque clé étrangère `order_id` des
autres tables atterrissent toutes sur `order`, sans aucune curation supplémentaire.

L'ensemble générique couvre les noms d'attributs (`name`, `date`, `status`, `type`, `amount`,
`quantity`), les expressions de piste d'audit (`created_at`, `modified_by`,
`submitted_timestamp`) et une poignée d'autres qui apparaissent sur presque chaque table.

### Le nom métier, pas le nom physique {: #the-business-name-not-the-physical-name }

Un terme dérivé suit le **nom métier** de la colonne : son alias si le modélisateur en a défini un, sinon son nom physique. Quand `usr_nm` a pour alias `user name`, le terme dérivé est `user name`, là où une expansion de `usr_nm` aurait pu donner `user number`.

Donner un alias à la colonne est la correction la plus forte. Un alias se propage à toutes les surfaces qui lisent la colonne (SQL, GraphQL, agents d'IA, le catalogue), si bien que le modèle se décrit correctement partout. Renommer le terme corrige une entrée du catalogue et laisse la colonne s'appeler `usr_nm` pour le lecteur suivant. Le bandeau de terme proposé dans l'interface le dit : donnez d'abord un alias à la colonne, et ne renommez le terme que si le nom de colonne est juste et le vocabulaire ne l'est pas.

Changer l'alias d'une colonne re-dérive son terme proposé ; l'ontologie suit ainsi le modèle et ne demande jamais deux fois la même correction. Dès qu'un curateur a ajouté à un terme une définition, une relation ou un expert, une modification d'alias laisse la référence en place, car ce travail est celui du curateur et il reste.

### Noms de table décrivant un chemin d'accès {: #access-path-table-names }

Certains noms de table décrivent un chemin d'accès plutôt qu'un concept : `user_by_name` est un utilisateur atteint par une recherche sur le nom, pas un type d'entité distinct. Quand Provisa dérive le concept de la table pour qualifier les expressions génériques, il coupe le nom au mot de liaison : `user_by_name` devient `user` et `orders_by_customer` devient `order`.

Sans la coupe, la clé de substitution de `user_by_name` serait normalisée en `user name` et entrerait en collision avec l'attribut réel `users.name`, ce qui donnerait un terme contenant à la fois une chose et l'un de ses propres champs. La coupe ne s'applique qu'aux concepts de table. Dans un nom de colonne, `by` fait partie du nom composé : `pet_by_name` et `pet_name` se normalisent donc en un même terme, `pet name`.

## Ce qui fait qu'un terme est curé {: #what-makes-a-term-curated }

Un terme issu de la normalisation des colonnes démarre vide, comme une proposition qui n'est pas encore du vocabulaire. Il devient curé dès que l'une des conditions suivantes est vraie :

- Une définition a été enregistrée.
- Une arête de relation a été ajoutée.
- Un expert métier a été assigné.
- Un curateur l'a retiré manuellement du service.

La curation compte pour le cycle de vie du terme. Quand la dernière colonne physique d'un terme curé quitte le modèle, le terme est déprécié et conservé : il sort du service, garde le contenu fourni par ses éditeurs et revient automatiquement si la même colonne réapparaît. Un terme non curé sans colonne restante est tout simplement supprimé.

## Resynchronisation depuis les tables {: #re-syncing-from-tables }

Chaque fois qu'une table est enregistrée ou rechargée, `sync_table_refs` réconcilie les colonnes de
cette table avec les références existantes. Les nouvelles colonnes créent ou lient des termes ; les
colonnes disparues abandonnent leurs références ; et la règle « supprimer ou déprécier » règle le
sort de tout terme qui perd sa dernière référence.

Seuls les termes non curés sont re-dérivés. Si vous avez donné un alias à une colonne et que le terme proposé change, la référence passe au nouveau terme. Si le terme est curé, le lien reste, car une modification d'alias ne remplace pas le choix de terme du curateur.

Un terme abstrait dont l'unique chemin vers des données physiques passait par un terme qui s'en va
est déprécié plutôt que supprimé, ce qui préserve la structure conceptuelle jusqu'à ce qu'elle soit
recâblée.

## Relations {: #relationships }

Les termes se relient à d'autres termes par des arêtes typées. Les types de relation pris en charge
sont :

| Type | Signification |
| --- | --- |
| `KIND_OF` | Le terme source est une sorte de terme cible. |
| `PART_OF` | Le terme source est un composant du terme cible. |
| `SYNONYM_OF` | Les deux termes sont interchangeables dans ce domaine. |
| `RELATED_TO` | Une association lâche — aucune affirmation plus forte ne convient. |
| `VALID_VALUE_OF` | La source est une valeur admise de l'énumération ou du domaine cible. |
| `DERIVED_FROM` | La source est calculée à partir de la cible ou en provient. |
| `REPLACES` | La source remplace la cible dépréciée. |
| `PREFERRED_TERM_FOR` | La source est le terme à préférer à la cible déconseillée. |
| `TRANSLATION_OF` | La source est une traduction, en une langue ou une locale, de la cible. |
| `ANTONYM_OF` | La source est l'opposé sémantique de la cible. |

Les relations sont orientées. L'interface montre à la fois les arêtes sortantes (ce terme → un
autre) et les arêtes entrantes (un autre terme → ce terme), en étiquetant chaque direction par sa
propre formulation en langage courant.

Les arêtes vivent dans `glossary_term_edges`, une table associative déclarée comme relation de jonction : sa colonne `rel_type` est le discriminant, de sorte que chacun des types ci-dessus est un type de relation Cypher distinct entre deux nœuds `GlossaryTerm`, et non une propriété portée par un nœud réifié. La table est provisionnée avec le reste du schéma de métadonnées et n'apparaît pas comme un nœud dans les clients graphe : elle est l'arête. Rien en elle n'est spécifique à l'ontologie : elle se déclare exactement comme vous déclareriez une jonction sur vos propres tables, et elle est lue par le même code.

## Termes abstraits {: #abstract-terms }

Un terme abstrait n'a aucune référence de colonne physique en propre. Employez-en un pour un concept métier qui couvre plusieurs termes concrets : un chapeau que vous câblez ensuite vers les termes précis qui, eux, portent des colonnes. `revenue`, par exemple, pourrait être abstrait, avec des arêtes `PART_OF` venant de `order amount`, `adjustment amount` et `refund amount` pointant vers lui.

Un terme abstrait qui ne peut atteindre aucune colonne physique par le graphe des relations est une proposition en suspens. Il n'apparaît ni dans la recherche de termes des agents ni dans l'export de métadonnées, car un terme qui ne désigne aucune donnée ne peut répondre à rien.

## La règle d'admission pour les surfaces consommatrices {: #the-admission-rule-for-consuming-surfaces }

Un terme qu'une surface consommatrice peut proposer doit satisfaire trois conditions :

1. **En service** — ni retiré (un curateur l'a sorti du service) ni déprécié (il a perdu sa
   dernière colonne et n'était conservé que parce que le supprimer laisserait quelque chose en
   suspens).
2. **Défini** — il porte une définition. Un terme dérivé d'un nom de colonne est un jeton, pas un
   sens. Sans définition, c'est une proposition en attente d'un curateur, jamais du vocabulaire sur
   lequel un agent peut fonder une question.
3. **Ancré** — relié, par des termes en service, à au moins un terme portant une référence de
   colonne physique. L'ontologie est un point d'entrée vers les données : chaque chaîne doit donc
   se terminer sur une colonne.

La connectivité se propage dans le graphe : un terme abstrait atteint les données par tout voisin en service qui les atteint. Les termes hors service ne transmettent pas ; un terme retiré ne maintient donc pas ses dépendants en vie.

## Export de métadonnées {: #metadata-export }

L'ontologie est publiée dans des catalogues de données externes dans le cadre de l'export de métadonnées. La même règle d'admission s'applique, avec une restriction : l'enracinement d'un terme ne se juge que sur les colonnes réellement publiées. Un terme dont toutes les colonnes sont retenues hors de l'export, parce que leurs tables ne sont pas marquées comme produits de données ou que des filtres techniques les excluent, n'est pas enraciné pour l'export, même s'il a des références dans le plan de contrôle.

Les arêtes de relation ne se publient que lorsque les deux termes qu'elles relient se publient.

Les actifs de colonne s'exportent indépendamment. L'exclusion d'un terme ne masque pas les données
sous-jacentes.

### Exclure un terme de l'export {: #excluding-a-term-from-export }

Certaines colonnes portent de la tuyauterie plutôt que des données métier : identifiants de lot
ETL, versions de ligne, horodatages d'ingestion. Un terme dérivé d'une telle colonne peut avoir une
définition parfaitement exacte qui n'est simplement pas du vocabulaire métier. Le
contrôle **Exclure de l'export de métadonnées** retient le terme, ainsi que toute arête de relation
qui s'y termine, hors des catalogues vers lesquels Provisa publie, tandis que les colonnes
elles-mêmes continuent de s'exporter comme actifs.

Ce qui compte, c'est si le métier emploie le mot ; la qualité de la définition n'en décide pas. Un identifiant de lot ETL a un sens clair qui a sa place dans l'ontologie pour les ingénieurs, mais pas dans un catalogue métier à côté de `customer` et `revenue`.

## Travailler avec l'ontologie {: #working-with-the-ontology }

Ouvrez **Ontologie** dans la barre de navigation de l'interface. Le panneau de gauche liste tous les termes ; cliquez
sur l'un d'eux pour ouvrir sa vue détaillée. De là :

- **Renommez** le terme pour changer sa formulation sans déplacer ses colonnes.
- **Ajoutez une définition** en la saisissant ou en cliquant sur le bouton de brouillon par IA pour
  générer un point de départ à partir du nom du terme, de ses colonnes physiques et de ses
  relations. Le brouillon n'est pas enregistré tant que vous ne l'avez pas confirmé.
- **Déplacez une référence** pour fusionner deux termes : choisissez le terme cible dans la liste
  déroulante à côté de n'importe quelle référence physique. Si le terme source perd sa dernière
  référence, son sort est réglé automatiquement par la règle « supprimer ou déprécier ».
- **Ajoutez une relation** entre ce terme et un autre, en choisissant le type dans l'ensemble
  fermé. Retypez une arête existante sur place plutôt que de la supprimer et de la recréer.
- **Assignez des experts** par identifiant d'utilisateur, avec un genre `expert` ou `author`.
- **Retirez** un terme pour le sortir du service. Il conserve ses colonnes et reste modifiable ici,
  mais la recherche de termes par les agents comme l'export de métadonnées le sautent. Restaurez-le
  plus tard si le concept revient.
- **Générez les définitions en masse** (**Définitions** dans le menu ✨) pour remplir toutes les définitions vides en une seule
  passe. Seules les définitions vides sont écrites ; le texte humain n'est jamais écrasé.
- **Générez les relations en masse** (**Relations** dans le menu ✨) pour proposer des arêtes typées sur l'ensemble de la liste des
  termes. Les propositions mal formées — noms de termes inconnus, arêtes vers soi-même, types non
  reconnus — sont écartées automatiquement.

La bannière **Proposé** sur un terme sans définition vous indique si le terme est indéfini (aliasez
la colonne ou ajoutez une définition) ou non ancré (reliez-le à un terme qui possède des colonnes).
Lorsque vous la voyez, le terme n'est pas encore accessible aux agents ni aux catalogues.

## Voir aussi {: #see-also }

- [Export de métadonnées](metadata-export.md) — comment les termes et les relations se publient
  vers des catalogues de données externes, et quels termes la règle d'admission à l'export admet.
- [Traçabilité au niveau des colonnes](lineage.md) — l'explorateur de traçabilité et la façon dont
  `columnDependents` rapporte les liaisons de l'ontologie comme dépendants d'une colonne physique.
