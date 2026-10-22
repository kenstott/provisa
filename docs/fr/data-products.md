# Produits de données {: #data-products }

Un produit de données est un ensemble nommé et possédé de tables publiées ensemble pour la consommation. C'est l'unité que le catalogue expose aux consommateurs : un ensemble sélectionné de tables qu'un domaine a déclaré prêt. Les champs suivent le vocabulaire ODPS (Open Data Product Standard) là où Provisa possède déjà la source de vérité.

## Règle de propriété du domaine {: #domain-ownership-rule }

Chaque produit de données appartient à exactement un domaine (`domain_id` est un champ requis). Une table ne peut rejoindre un produit de données que si les deux partagent le même `domain_id`. L'interface restreint le sélecteur de tables au domaine du produit ; le backend rejette une affectation `product_id` dont le domaine ne correspond pas à celui du produit au moment de l'enregistrement.

Un produit qui a besoin de données d'un autre domaine doit d'abord faire entrer ces données sous forme de vue de domaine, puis inclure la vue comme membre.

## Ports de sortie {: #output-ports }

Les tables et commandes affectées à un produit de données constituent ses **ports de sortie** : la surface interrogeable que voient les consommateurs. Affecter une table définit `Table.product_id` ; l'effacer supprime l'appartenance. Une table appartient à au plus un produit. Les commandes du même domaine peuvent également être affectées comme membres.

## Sections du panneau de détail {: #detail-panel-sections }

Ouvrir un produit de données dans l'interface d'administration affiche les panneaux suivants :

| Panneau | Ce qu'il affiche |
| --- | --- |
| Ports de sortie | Tables membres et leurs colonnes ; commandes membres ; exemples de requêtes (GraphQL, SQL, Cypher, gRPC, JSON:API, REST) |
| Termes associés | Termes du glossaire liés aux tables membres du produit |
| Tables associées | Tables accessibles depuis les tables membres via des relations approuvées mais ne faisant pas encore partie du produit |
| Relations | Relations approuvées entre les tables membres de ce produit |
| Traçabilité | Graphe de traçabilité des colonnes montrant les tables membres comme point final publié plus chaque table en amont. Requiert la capacité `view_governance` |
| Ports d'entrée | Entrées à un saut → transformation → sorties dérivées de la traçabilité. Requiert `view_governance` |
| Qualité des données | Tables de vérification dont les contrats analysent les ports de sortie de ce produit ; une ligne par vérification et par exécution. Comprend une fenêtre modale de règles et un affichage des étiquettes DCP |

## Export de métadonnées {: #metadata-export }

Seules les tables affectées à un produit sont publiées vers les catalogues externes par défaut. `build_snapshot` applique un filtre `data_products_only` : les tables non affectées sont retenues, ainsi que leurs arêtes de relation, arêtes de traçabilité et étiquettes de gouvernance. Les sources et les domaines sont toujours publiés, quel que soit ce filtre.

Un produit sans membre exporté ne publie pas. Une fiche vide laisserait croire qu'un produit existe sans rien derrière.

Seuls les catalogues disposant d'un concept natif de produit de données le publient comme entité à part entière ; les autres publient les tables membres (déjà filtrées ci-dessus) sans regroupement en produit :

| Catalogue | Publié comme |
| --- | --- |
| Snowflake Horizon | SHARE + fiche d'organisation (produit de données natif) ; `publish=false` la maintient en DRAFT, `publish=true` la publie en direct. Le dictionnaire de données de la fiche couvre chaque membre et chaque colonne ; les cinq premiers membres sont mis en avant et les colonnes masquées sont masquées dans l'aperçu |
| BigQuery Analytics Hub | Fiche Analytics Hub (native) |
| OpenMetadata | Entité `DataProduct` (native) |
| DataHub | Entité URN `dataProduct` native avec ses propres aspects de propriétés/propriété |
| Collibra | Actif d'un type de communauté `Data Product`, lié aux tables membres |
| Apache Atlas | Typedef personnalisé `provisa_data_product` au mieux — Atlas n'a pas de type natif de produit de données |
| Atlan | Estimation de typedef personnalisé `DataProduct` au mieux — Atlan n'a pas de type stable documenté pour cela |
| OpenLineage | Pas une fiche — les tables membres portent une facette personnalisée `provisa_data_product` nommant le produit |

## Champs {: #fields }

| Champ | Requis | Notes |
| --- | --- | --- |
| `id` | Oui | Identifiant stable lisible par machine, p. ex. `customer_360` |
| `domain_id` | Oui | Domaine propriétaire ; la règle d'appartenance est appliquée contre celui-ci |
| `name` | Oui | Nom d'affichage |
| `owner_role` | Non | Rôle responsable de ce produit ; distinct du steward de domaine |
| `team_role` | Non | Rôle dont les titulaires forment l'équipe de travail au quotidien ; résolu en individus |
| `purpose` | Non | Ce que ce produit publie et pourquoi |
| `limitations` | Non | Contraintes, mises en garde ou exclusions connues |
| `usage` | Non | Comment consommer ce produit |
| `version` | Non | p. ex. `1.2.0` |
| `status` | Non | p. ex. `proposed`, `active`, `deprecated`, `retired` |
| `sla` | Non | Engagements de niveau de service ; texte libre uniquement — un produit s'étend sur plusieurs tables membres et un SLA structuré ne peut nommer sans ambiguïté quel membre il décrit |
| `support` | Non | Indications de support en texte libre |
| `support_contact` | Non | E-mail ou URL ; requis par les manifestes de fiche d'organisation de Snowflake Horizon Catalog |
| `publish` | Non | `true` pour publier immédiatement les fiches Horizon Catalog ; les nouvelles fiches sont en DRAFT par défaut |
| `custom_properties` | Non | Métadonnées clé-valeur arbitraires non couvertes par les champs standards |
