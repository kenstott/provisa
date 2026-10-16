# Régions

Provisa vous donne deux primitives pour placer les données : une **région**, et le **rattachement à une région**, c'est-à-dire créer une région et y affecter une table. Aucune n'est à elle seule une politique. Ce que vous en faites relève de trois groupes :

- **Performance.** La latence vers les lecteurs, la fraîcheur de leur copie, et l'adéquation du moteur : où se trouve son centre de données, quel type de stockage il lit, combien de CPU et de mémoire il a.
- **Coût.** Où le travail s'exécute et sur quoi : matériel en propre ou cloud, un gros moteur ou un petit, et ce que coûte le déplacement des données entre lieux (egress).
- **Souveraineté.** Qui contrôle les données et où elles peuvent aller : les règles de résidence de la loi (où les données personnelles peuvent être traitées et où elles peuvent être stockées), et les lignes de contrôle propres à une organisation, comme une unité métier ou une filiale qui garde ses données et son infrastructure sous son propre contrôle.

La mécanique est la même pour les trois. Seules changent les règles que vous écrivez par-dessus.

## Une région est un moteur et ses magasins en un lieu {: #a-region-is-an-engine-and-its-stores-in-a-place }

Une région est un lieu où Provisa travaille et conserve ce que le travail laisse derrière lui. Chaque région a son propre moteur et ses propres magasins. (REQ-1922) [tool-verified: `provisa/core/regions.py` `OrgRegion`, `STORE_ROLES`]

| Magasin | Ce qu'il contient |
| --- | --- |
| `engine` | Le moteur de fédération qui exécute les instructions dans cette région |
| `replicas` | Les réplicas des tables rattachées ici, et des tables sans région que cette région copie |
| `views` | Les copies de vues matérialisées de cette région |
| `cache` | Les réponses en cache |
| `state` | L'état de construction et d'actualisation |
| `record` | L'enregistrement de chaque requête servie ici |

Un seul modèle gouverne chaque région. Sources, tables, relations, rôles, règles de lignes et de colonnes, vues et réglages de fraîcheur sont déclarés une fois, dans un magasin de modèle, et chaque région sert ce même modèle, si bien que les définitions ne peuvent pas diverger d'une région à l'autre. Un changement atteint chaque région par le même tampon, et chaque région converge ensuite vers lui. (REQ-1914, REQ-1922)

La plateforme déclare les régions physiques où tournent ses nœuds, avec l'adresse à laquelle répond chaque région. Le modèle de chaque organisation choisit celles de ces régions qu'elle utilise et nomme son propre magasin pour chaque rôle dans chaque région. Un nœud démarre dans une région avec `provisa run --region <id>` et sert cette région pour chaque organisation qui la choisit. Quand des régions sont déclarées, un nœud démarré sans `--region` refuse de démarrer et liste les régions qu'il pourrait servir. (REQ-1922) [tool-verified: `provisa/cli.py` `--region`; `provisa/core/regions.py` `validate_regions`]

```yaml
platform:
  regions:
    - { id: eu, address: "https://eu.provisa.example" }
    - { id: us, address: "https://us.provisa.example" }

stores:
  - { id: eu_engine, url: "${secret:eu-trino-url}", kind: trino }
  - { id: us_engine, url: "${secret:us-trino-url}", kind: trino }
  - { id: eu_data,   url: "${secret:eu-pg-url}" }
  - { id: us_data,   url: "${secret:us-pg-url}" }
  - { id: eu_cache,  url: "${secret:eu-redis-url}" }
  - { id: us_cache,  url: "${secret:us-redis-url}" }

regions:
  - { id: eu, engine: eu_engine, replicas: eu_data, views: eu_data, cache: eu_cache, state: eu_data, record: eu_data }
  - { id: us, engine: us_engine, replicas: us_data, views: us_data, cache: us_cache, state: us_data, record: us_data }
```
[inferred: the URLs and secret names are placeholders; the shape follows `PlatformRegion`, `StoreConfig` and `OrgRegion` in `provisa/core/regions.py`]

Au chargement, la configuration est refusée, motif à l'appui, si les régions ne tiennent pas ensemble. Une région doit nommer un magasin déclaré pour chaque rôle. Ses `replicas` et `views` doivent être le même magasin, et son magasin de moteur doit nommer un `kind` de moteur. Avec plus d'une région de plateforme, un fichier de moteur embarqué est refusé, car les autres régions ne peuvent pas l'atteindre. [tool-verified: `require_one_materialize_store`, `require_engine_kind`, `require_reachable_engine`]

Plusieurs régions peuvent partager une instance de base de données, un Redis ou un bucket de stockage objet. Tout ce que garde une région porte la région dans son nom, si bien que leurs magasins n'entrent pas en collision. (REQ-1922)

Un déploiement qui ne déclare aucune région a une région implicite et n'affiche la région nulle part. Il n'y a ni champ de région ni option `--region`, et une configuration qui nomme une région est refusée. (REQ-1922)

## Rattachement à une région : une table nomme sa région d'origine {: #region-homing-a-table-names-its-home }

Une table peut nommer l'une des régions de son organisation comme région d'origine. Celle-ci est la seule région qui construit et actualise son réplica, et la seule qui peut la garder chaude. Toutes les autres régions lisent la table depuis le réplica de la région d'origine, sur place, à l'adresse que donne le modèle. Les règles de lignes et de colonnes du lecteur entrent dans cette instruction, si bien que le filtrage a lieu dans la région d'origine et que seul passe ce que le lecteur a le droit de voir. (REQ-1921) [tool-verified: `provisa/core/regions.py` module docstring]

```yaml
tables:
  - source_id: eu-shop
    schema_name: public
    table_name: customers
    region: eu
```
Quelques conséquences :

- Aucune autre région ne lit la source de la table ; elles ne lisent que le réplica de la région d'origine. Une source que vous répliquez pour la protéger de la charge reste protégée.
- Si le réplica n'a pas été construit, ou si la région d'origine est injoignable, la requête est refusée en nommant la région d'origine. La source n'est pas lue en direct à la place.
- Une table rattachée qui n'est pas répliquée, avec `replicate` Never ou un réglage qui n'a pas encore appelé de copie, est lue en direct depuis sa source par chaque région, comme toute table non répliquée.
- Le compteur chaud d'une table ne reflète que le trafic de sa région d'origine. (REQ-239)
- Une vue matérialisée peut aussi nommer une région. Elle n'est alors construite que là, et toutes les autres régions lisent cette copie, exactement comme pour une table rattachée.

Une région qui lit le réplica d'une autre a besoin d'un moteur capable de le lire : `duckdb`, `motherduck`, `pg`, `trino` ou `trino-byo`. Le magasin `replicas` de la région d'origine doit être un serveur PostgreSQL. La configuration nomme l'un et l'autre quand ils ne conviennent pas. [tool-verified: `require_readable_elsewhere`; `REGION_READERS` in `provisa/core/engine_kinds.py`]

Une source peut aussi nommer une région, mais cette région ne fait que préremplir le formulaire quand vous enregistrez une nouvelle table de la source. Elle ne décide rien de l'endroit où vivent les données, et la changer plus tard ne déplace aucune table. (REQ-1921)

## Les tables sans région sont copiées là où elles sont lues {: #unhomed-tables-are-copied-where-they-are-read }

Une table qui ne nomme aucune région peut être copiée dans chaque région. Quand son réglage `replicate` appelle une copie, la région à laquelle le lecteur s'est connecté en construit une dans son propre magasin. Une vue matérialisée qui ne nomme aucune région est construite dans chaque région, chaque copie dans le stockage de vues de cette région. (REQ-1921)

Ajouter une région signifie donc plus de copies et plus de travail d'actualisation. Jamais plus de gouvernance. Un seul modèle gouverne chaque copie, et les mêmes règles de lignes et de colonnes s'appliquent dans chaque région. La construction d'une vue dans une région est gouvernée comme l'administrateur de l'organisation agissant dans cette région, si bien que la copie de chaque région contient ce qui peut y être conservé. (REQ-1921, REQ-1922)

## Trois raisons d'utiliser des régions {: #three-reasons-to-use-regions }

### Performance

Les régions utilisées pour la seule performance ne déclarent aucune règle entre régions. Les tables, les rôles et les règles sont ceux que vous écririez pour une seule région, et le rattachement décide où le travail a lieu :

- **Latence.** Les lecteurs se connectent à la région proche d'eux, dont le moteur, le cache et les copies leur sont locaux.
- **Fraîcheur.** Une table rattachée là où elle change y est actualisée, près de sa source.
- **Adéquation du moteur.** Chaque région exécute son propre moteur, si bien que vous pouvez adapter un moteur à un lieu : le centre de données où il se trouve, le stockage qu'il lit, et le CPU et la mémoire qu'il a.

**Exemple : deux régions identiques.** Déclarez `east` et `west` dans deux centres de données, chacune avec le même type de moteur et les mêmes types de magasin, et ne rattachez aucune table. Chaque région sert les lecteurs les plus proches d'elle : elle construit ses propres copies de ce qu'ils lisent et met leurs réponses en cache. Les deux peuvent se trouver derrière un équilibreur de charge et se partager la charge. Il n'y a qu'un calendrier d'actualisation, celui du modèle, et les deux régions commencent à actualiser leurs copies selon lui, mais leurs moteurs peuvent ne pas terminer dans le même délai. À un instant donné, rien ne garantit donc que les deux soient dans le même état, et un lecteur passé d'une région à l'autre peut voir des données d'une fraîcheur différente. Provisa ne déplace pas un client vers l'autre région quand l'une s'arrête ; c'est l'équilibreur de charge qui décide où va un client. [inferred: no failover or cross-region redirect exists in `provisa/`; clients connect to a region by its address, and each region builds and refreshes only its own copies, on the model's one schedule (REQ-1922)]

### Coût {: #cost }

Les régions de coût ne déclarent pas non plus de règles entre régions. Elles placent le travail là où il coûte le moins :

- **Matériel en propre et cloud.** Gardez une région sur site, sur du matériel que vous possédez déjà, pour les tables et les lecteurs qui s'y trouvent, et exécutez une autre région dans le cloud pour le reste.
- **Taille du moteur.** Exécutez un gros moteur dans une région pour les travaux lourds occasionnels, et un petit dans une autre pour les lectures quotidiennes. Les lecteurs choisissent la région par son adresse.
- **Egress.** Les lecteurs des autres régions ne reçoivent que les lignes filtrées du réplica d'une table rattachée, et une grosse table rattachée à une région n'est pas copiée dans chaque région.

### Souveraineté {: #sovereignty }

Les régions utilisées pour la souveraineté déclarent, elles, des règles entre régions, que la raison soit la loi ou l'organisation : une filiale qui rattache ses tables à sa propre région les fait traiter et stocker sur sa propre infrastructure, et décide par ces règles ce que lit le reste du groupe. Ce sont des règles de lignes et de colonnes ordinaires, écrites sur des attributs qui disent où se trouve le lecteur, ou le nœud qui fait le travail :

- `current_setting('provisa.region')` est la région du nœud qui répond à la requête, quand la plateforme déclare des régions. Un filtre de lignes écrit dessus garde les lignes d'une région dans cette région. (REQ-1922) [tool-verified: `provisa/core/request_context.py` `session_vars_for`]
- L'attribut `region` d'un membre lui fait détenir les pseudo-rôles `region` et `region__<value>`, si bien qu'une entrée de masque ou d'octroi peut nommer `analyst+region__eu`. Le travail d'arrière-plan propre à un nœud porte sa région de la même façon. (REQ-2057) Voir [Rôles : octroyés, pseudo et méta](security.md#roles-granted-pseudo-and-meta).

Avec le rattachement, ces règles décident de ce qui passe une frontière. Les données personnelles d'une table rattachée sont stockées et traitées dans sa région d'origine, et un lecteur ailleurs ne reçoit que ce que ses règles autorisent, filtré à l'origine. Une vue sans région construite dans chaque région contient ce qu'un administrateur de cette région peut voir, si bien que la copie d'une région peut contenir des lignes que la copie d'une autre région omet. (REQ-1921)

## Qui peut rattacher une table {: #who-may-home-a-table }

Définir, changer ou retirer la région d'une table, d'une source ou d'une vue matérialisée déplace des données à travers une frontière. Il faut pour cela le droit `data_residency`, en plus d'être propriétaire de l'objet par son domaine. L'octroi nomme les valeurs qu'il couvre : les régions de l'organisation et « aucune région ». Déplacer un objet entre deux régions exige un octroi qui couvre les deux. Un enregistrement hors de l'octroi est refusé, en nommant la valeur non couverte. (REQ-1921)

Le droit n'existe que lorsque la plateforme déclare des régions. La propriété du domaine décide toujours qui peut modifier tout le reste de l'objet. Une région ne confère aucun droit propre ; une organisation qui veut que les tables d'une région soient administrées par les personnes de cette région leur donne des domaines à elles. (REQ-1921)

Une table ou une vue peut aussi passer en **brouillon** : hors service, sans copie dans aucune région. Une table enregistrée depuis l'administration démarre en brouillon. Un déplacement entre régions peut passer par le brouillon, si bien qu'entre-temps aucune donnée n'est conservée ni lue nulle part : le côté de la première région met l'objet en brouillon, et le détenteur de la seconde région le revendique en définissant sa région. (REQ-1921)

Voir aussi la [démo à deux régions](regions-demo.md).
