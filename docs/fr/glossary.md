# Glossaire métier

Le glossaire métier est un vocabulaire vivant posé sur votre modèle de données. Chaque colonne
physique de la couche sémantique se résout vers un terme — un terme partagé unique dès que
plusieurs colonnes portent le même concept, aussi différemment qu'elles l'orthographient. Chaque
terme peut porter une définition, un ensemble de relations typées vers d'autres termes et une
liste d'experts métier qui possèdent le sens.

Ce vocabulaire partagé est le pont entre le langage métier et les données physiques. Un agent d'IA
qui sait que « customer » nomme toutes les colonnes portant un identifiant de client n'a pas à
deviner lequel de `cust_id`, `customerId` et `CUSTOMER_KEY` est le bon — ils se résolvent tous vers
le même terme, et le terme porte la définition.

## Comment les termes sont dérivés

Provisa dérive automatiquement un terme de chaque nom de colonne, à l'aide d'une règle de
normalisation déterministe (REQ-1387) : mise en casse uniforme, découpage sur les séparateurs et le
camelCase, expansion des abréviations et retrait des jetons de substitution finaux.

**L'expansion des abréviations** fait correspondre les raccourcis d'entreprise courants à leur
forme complète : `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity`, et ainsi de suite.
`id` comme `key` s'étendent en `identifier`. La table est figée et prudente — les raccourcis
ambigus comme `st`, `min` et `no` restent tels quels plutôt que de deviner de travers.

**Le retrait des jetons de substitution** supprime un jeton final `identifier`, `code`, `index` ou
`reference`. Une colonne nommée `cust_id` ne nomme pas l'identifiant lui-même ; elle nomme un
client à travers une valeur de substitution. Retirer la substitution fait atterrir `cust_id` et
`customerId` sur le même terme `customer`. Seuls les jetons finaux sont retirés, et jamais le
dernier jeton restant : une colonne `id` seule s'étend en `identifier` et y reste.

**La déduplication** est tout l'enjeu. La règle de normalisation étant déterministe, `cust_id`,
`customerId` et `CUSTOMER_KEY` produisent tous `customer`. Chaque colonne obtient une référence sur
l'unique terme résultant plutôt que trois termes séparés. La curation n'a alors qu'un seul endroit
où ajouter la définition, pas trois.

### Expressions génériques

Certaines expressions normalisées sont trop génériques pour être un concept à elles seules. Une
colonne `name`, `date` ou `identifier` seule nomme un attribut du concept de sa table, pas un
concept indépendant de cette table. Les employés ont des noms ; les produits ont des noms ; ce
n'est pas la même chose.

Lorsqu'une expression appartient à l'ensemble générique et qu'un contexte de table est disponible,
le terme est qualifié en `<concept de la table> <expression>` : `employees.first_name` se normalise
en `employee first name`, et `orders.id` se normalise en `order`, parce que le retrait de la
substitution fait ensuite retomber l'expression qualifiée sur le concept qu'elle identifie. Ce
dernier cas est important : la clé primaire de `orders` et chaque clé étrangère `order_id` des
autres tables atterrissent toutes sur `order`, sans aucune curation supplémentaire.

L'ensemble générique couvre les noms d'attributs (`name`, `date`, `status`, `type`, `amount`,
`quantity`), les expressions de piste d'audit (`created_at`, `modified_by`,
`submitted_timestamp`) et une poignée d'autres qui apparaissent sur presque chaque table.

### Le nom métier, pas le nom physique

Un terme dérivé suit le **nom métier** de la colonne — son alias lorsque le modélisateur en a posé
un, son nom physique sinon (REQ-1581). Lorsque `usr_nm` est aliasé en `user name`, le terme dérivé
est `user name`, et non `user number` ni une expansion quelconque de `usr_nm`.

Aliaser une colonne est la correction la plus forte. Un alias voyage vers toutes les surfaces qui
lisent la colonne — SQL, GraphQL, agents d'IA, catalogue — de sorte que le modèle se décrit
correctement partout. Renommer un terme corrige une entrée de catalogue et laisse la colonne lire
`usr_nm` pour le lecteur suivant. La bannière de terme proposé dans l'interface le dit sans
détour : aliasez d'abord la colonne ; ne renommez le terme que lorsque le nom de la colonne est
juste et que le vocabulaire ne l'est pas.

Ré-aliaser une colonne redérive son terme proposé, de sorte que le glossaire suit le modèle plutôt
que de réclamer deux fois la même correction. Une fois qu'un curateur a ajouté une définition, une
relation ou un expert à un terme, une modification d'alias ne déplace pas la référence — ce travail
appartient au curateur, et il reste.

### Noms de table décrivant un chemin d'accès

Certains noms de table décrivent un chemin d'accès plutôt qu'un concept : `user_by_name` est un
utilisateur atteint par une recherche sur le nom, pas un genre d'entité distinct. Lorsque Provisa
dérive le concept de table pour qualifier une expression générique, il coupe le nom au connecteur
(REQ-1582). `user_by_name` devient `user` ; `orders_by_customer` devient `order`.

Sans cette coupe, la clé de substitution de `user_by_name` se normaliserait en `user name` et
entrerait en collision avec le véritable attribut `users.name` — un seul terme portant une chose et
l'un de ses propres champs. La coupe ne s'applique qu'aux concepts de table. Dans un nom de
colonne, `by` fait partie du nom composé : `pet_by_name` et `pet_name` se normalisent vers le même
terme, `pet name`.

## Ce qui fait qu'un terme est curé

Un terme né de la normalisation d'une colonne démarre vierge — une proposition, pas encore du
vocabulaire. Il devient curé dès que l'une des conditions suivantes est vraie :

- Une définition a été enregistrée.
- Une arête de relation a été ajoutée.
- Un expert métier a été assigné.
- Un curateur l'a retiré manuellement du service.

La curation compte pour le cycle de vie du terme. Lorsque la dernière colonne physique d'un terme
curé est retirée du modèle, le terme est déprécié plutôt que supprimé : il sort du service,
conserve le contenu fourni par ses éditeurs, et est ravivé automatiquement si la même colonne
réapparaît. Un terme non curé sans plus aucune colonne est simplement supprimé.

## Resynchronisation depuis les tables

Chaque fois qu'une table est enregistrée ou rechargée, `sync_table_refs` réconcilie les colonnes de
cette table avec les références existantes. Les nouvelles colonnes créent ou lient des termes ; les
colonnes disparues abandonnent leurs références ; et la règle « supprimer ou déprécier » règle le
sort de tout terme qui perd sa dernière référence.

La redérivation n'a lieu que pour les termes non curés. Si vous avez aliasé une colonne et que le
terme proposé diffère désormais, la référence se déplace vers le nouveau terme. Si le terme est
curé, le lien reste — la modification d'alias n'a pas primé sur le choix de terme du curateur.

Un terme abstrait dont l'unique chemin vers des données physiques passait par un terme qui s'en va
est déprécié plutôt que supprimé, ce qui préserve la structure conceptuelle jusqu'à ce qu'elle soit
recâblée.

## Relations

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

Les arêtes vivent dans `glossary_term_edges`, une table associative déclarée comme relation de jonction
(REQ-1586) : sa colonne `rel_type` est le discriminant, de sorte que chacun des types ci-dessus est un
type de relation Cypher distinct entre deux nœuds `GlossaryTerm`, et non une propriété portée par un nœud
réifié. La table est provisionnée avec le reste du schéma de métadonnées et n'apparaît pas comme un nœud
dans les clients graphe — elle est l'arête. Rien en elle n'est spécifique au glossaire : elle se déclare
exactement comme vous déclareriez une jonction sur vos propres tables, et elle est lue par le même code.
[tool-verified: `provisa/cypher/label_map.py:378-397`, `provisa/api/startup_seed.py:508-550`]

## Termes abstraits

Un terme abstrait n'a aucune référence de colonne physique en propre. Employez-en un pour un
concept métier qui couvre plusieurs termes concrets — un chapeau que vous câblez ensuite vers les
termes précis qui, eux, portent des colonnes. `revenue`, par exemple, pourrait être abstrait, avec
des arêtes `PART_OF` venant de `order amount`, `adjustment amount` et `refund amount` pointant vers
lui.

Un terme abstrait qui ne peut atteindre aucune colonne physique à travers le graphe de relations
est une proposition en suspens. Il n'apparaît ni dans la recherche de termes par les agents ni dans
l'export de métadonnées — un terme qui ne nomme aucune donnée ne peut répondre à rien.

## La règle d'admission pour les surfaces consommatrices

Un terme qu'une surface consommatrice peut proposer doit satisfaire trois conditions (REQ-1387) :

1. **En service** — ni retiré (un curateur l'a sorti du service) ni déprécié (il a perdu sa
   dernière colonne et n'était conservé que parce que le supprimer laisserait quelque chose en
   suspens).
2. **Défini** — il porte une définition. Un terme dérivé d'un nom de colonne est un jeton, pas un
   sens. Sans définition, c'est une proposition en attente d'un curateur, jamais du vocabulaire sur
   lequel un agent peut fonder une question.
3. **Ancré** — relié, par des termes en service, à au moins un terme portant une référence de
   colonne physique. Le glossaire est un point d'entrée vers les données : chaque chaîne doit donc
   se terminer sur une colonne.

La connectivité se propage à travers le graphe : un terme abstrait atteint les données par
n'importe quel voisin en service qui les atteint. Les termes hors service ne conduisent pas — un
terme retiré ne maintient pas ses dépendants en vie.

## Export de métadonnées

Le glossaire se publie vers des catalogues de données externes dans le cadre de l'export de
métadonnées. La même règle d'admission s'applique, avec un resserrement : l'ancrage d'un terme
n'est jugé qu'au regard des colonnes qui se publient réellement. Un terme dont toutes les colonnes
sont retenues hors de l'export — parce que leurs tables ne sont pas marquées comme produits de
données, ou parce que des filtres techniques les excluent — n'est pas ancré du point de vue de
l'export, même s'il porte des références dans le plan de contrôle.

Les arêtes de relation ne se publient que lorsque les deux termes qu'elles relient se publient.

Les actifs de colonne s'exportent indépendamment. L'exclusion d'un terme ne masque pas les données
sous-jacentes.

### Exclure un terme de l'export

Certaines colonnes portent de la tuyauterie plutôt que des données métier : identifiants de lot
ETL, versions de ligne, horodatages d'ingestion. Un terme dérivé d'une telle colonne peut avoir une
définition parfaitement exacte qui n'est simplement pas du vocabulaire métier (REQ-1583). Le
contrôle **Exclure de l'export de métadonnées** retient le terme, ainsi que toute arête de relation
qui s'y termine, hors des catalogues vers lesquels Provisa publie, tandis que les colonnes
elles-mêmes continuent de s'exporter comme actifs.

Le critère est de savoir si le métier prononce ce mot, pas si la définition est bonne. Un
identifiant de lot ETL a un sens clair qui a sa place dans le glossaire pour les ingénieurs ; il
n'a pas sa place dans un catalogue métier à côté de `customer` et `revenue`.

## Travailler avec le glossaire

Ouvrez **Admin → Glossaire** dans l'interface. Le panneau de gauche liste tous les termes ; cliquez
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
- **Générez les définitions en masse** pour remplir toutes les définitions vides en une seule
  passe. Seules les définitions vides sont écrites ; le texte humain n'est jamais écrasé.
- **Générez les relations en masse** pour proposer des arêtes typées sur l'ensemble de la liste des
  termes. Les propositions mal formées — noms de termes inconnus, arêtes vers soi-même, types non
  reconnus — sont écartées automatiquement.

La bannière **Proposé** sur un terme sans définition vous indique si le terme est indéfini (aliasez
la colonne ou ajoutez une définition) ou non ancré (reliez-le à un terme qui possède des colonnes).
Lorsque vous la voyez, le terme n'est pas encore accessible aux agents ni aux catalogues.

## Voir aussi

- [Export de métadonnées](metadata-export.md) — comment les termes et les relations se publient
  vers des catalogues de données externes, et quels termes la règle d'admission à l'export admet.
- [Traçabilité au niveau des colonnes](lineage.md) — l'explorateur de traçabilité et la façon dont
  `columnDependents` rapporte les liaisons du glossaire comme dépendants d'une colonne physique.
