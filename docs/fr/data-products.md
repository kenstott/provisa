# Produits de données (REQ-1634)

Un produit de données est un ensemble nommé et possédé de tables publiées ensemble pour la consommation. C'est l'unité que le catalogue expose aux consommateurs — non pas des tables individuelles, mais une surface sélectionnée qu'un domaine déclare explicitement prête. Les champs suivent le vocabulaire ODPS (Open Data Product Standard) là où Provisa possède déjà la source de vérité. [tool-verified: `provisa/core/models.py:318-342`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json`]

## Règle de propriété du domaine

Chaque produit de données appartient à exactement un domaine (`domain_id` est un champ requis). Une table ne peut rejoindre un produit de données que si les deux partagent le même `domain_id`. L'interface restreint le sélecteur de tables au domaine du produit ; le backend rejette une affectation `product_id` dont le domaine ne correspond pas à celui du produit au moment de l'enregistrement. [tool-verified: `provisa/core/models.py:320`, `docs/arch/requirements.yaml:54573-54574`]

Un produit qui a besoin de données d'un autre domaine doit d'abord faire entrer ces données sous forme de vue de domaine, puis inclure la vue comme membre.

## Ports de sortie

Les tables et commandes affectées à un produit de données constituent ses **ports de sortie** — la surface interrogeable que voient les consommateurs. Affecter une table définit `Table.product_id` ; l'effacer supprime l'appartenance. Une table appartient à au plus un produit. Les commandes du même domaine peuvent également être affectées comme membres. [tool-verified: `provisa/core/models.py:941`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json:tablesLabel,commandsLabel`]

## Sections du panneau de détail

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

[tool-verified: `provisa-ui/src/i18n/locales/en/dataProductsTab.json:detail`]

## Export de métadonnées {: #metadata-export }

Seules les tables affectées à un produit sont publiées vers les catalogues externes par défaut. `build_snapshot` applique un filtre `data_products_only` : les tables non affectées sont retenues, ainsi que leurs arêtes de relation, arêtes de traçabilité et étiquettes de gouvernance. Les sources et les domaines sont toujours publiés, quel que soit ce filtre. [tool-verified: `provisa/api/metadata_export/builder.py:594,609,641`]

Un produit sans membre exporté ne publie pas — une fiche vide laisserait croire qu'un produit existe sans rien derrière. [tool-verified: `provisa/api/metadata_export/model.py:106-113`]

Seuls les catalogues disposant d'un concept natif de produit de données le publient comme entité de premier ordre ; les autres publient les tables membres (déjà filtrées ci-dessus) sans regroupement en produit :

| Catalogue | Publié comme |
| --- | --- |
| Snowflake Horizon | SHARE + fiche d'organisation (produit de données natif) ; `publish=false` la maintient en DRAFT, `publish=true` la publie en direct |
| BigQuery Analytics Hub | Fiche Analytics Hub (native) |
| OpenMetadata | Entité `DataProduct` (native) |
| DataHub | Entité URN `dataProduct` native avec ses propres aspects de propriétés/propriété |
| Collibra | Actif d'un type de communauté `Data Product`, lié aux tables membres |
| Apache Atlas | Typedef personnalisé `provisa_data_product` au mieux — Atlas n'a pas de type natif de produit de données |
| Atlan | Estimation de typedef personnalisé `DataProduct` au mieux — Atlan n'a pas de type stable documenté pour cela |
| OpenLineage | Pas une fiche — les tables membres portent une facette personnalisée `provisa_data_product` nommant le produit |

[tool-verified: `provisa/api/metadata_export/snowflake_horizon.py:389-418`, `provisa/api/metadata_export/bigquery_dataplex.py:112-136`, `provisa/api/metadata_export/openmetadata.py:326-344`, `provisa/api/metadata_export/datahub.py:133-136,443-483`, `provisa/api/metadata_export/collibra.py:129-133,371-388`, `provisa/api/metadata_export/atlas.py:134-147`, `provisa/api/metadata_export/atlan.py:60`, `provisa/api/metadata_export/openlineage.py:243,348`]

## Champs

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
| `support_contact` | Non | E-mail ou URL ; requis par les manifestes de fiche d'organisation de Snowflake Horizon Catalog (REQ-1635) |
| `publish` | Non | `true` pour publier immédiatement les fiches Horizon Catalog ; les nouvelles fiches sont en DRAFT par défaut (REQ-1635) |
| `custom_properties` | Non | Métadonnées clé-valeur arbitraires non couvertes par les champs standards |

[tool-verified: `provisa/core/models.py:318-342`, `provisa/api/admin/types.py:104-118,538-551`]
