# La logique métier dans Provisa

## Le problème {: #the-problem }

Toute organisation a des standards pour la logique métier, et la plupart des équipes les suivent. Pourtant, au fil des années, à mesure que des systèmes sont construits et remplacés et que les équipes changent, la logique se disperse entre tâches ETL, tableaux de bord, API et procédures stockées. Chaque pièce a du sens en soi. Souvent personne n'a planifié l'ensemble, et plus souvent encore personne n'a planifié l'ensemble dans la durée. Le résultat est connu : le « chiffre d'affaires net » est calculé de trois façons à trois endroits, et personne ne peut dire laquelle est juste.

Le secteur apporte des réponses à certaines parties de ce problème. Une couche de métriques, souvent vendue comme couche sémantique, définit le « chiffre d'affaires net » une fois pour que tous les outils le lisent. L'[Open Data Product Standard](https://github.com/bitol-io/open-data-product-standard) (ODPS) décrit les produits de données et l'[Open Data Contract Standard](https://github.com/bitol-io/open-data-contract-standard) (ODCS) décrit les contrats de données. Provisa [exporte les deux](metadata-export.md#data-products-as-odps-and-odcs). Toutes sont de bonnes idées et toutes gagnent du terrain, mais chacune couvre une partie. La vraie question est de savoir comment elles s'insèrent dans des données contrôlées et gouvernées, en un seul modèle cohérent et composable. Une logique qui dérive ou modifie des données a besoin de la même gouvernance que les données elles-mêmes : qui peut l'utiliser, ce qu'elle lit et écrit, et où vont ses résultats.

## Ce qu'est la logique métier {: #definition }

La logique métier, ce sont les règles qui créent, modifient, suppriment ou transforment des données.

La terminologie métier est autre chose. Les noms et descriptions des domaines, des tables et des colonnes, et les termes du [glossaire](glossary.md), disent ce que signifient les données. La logique métier dit ce qu'on en fait.

## Trois rôles {: #roles }

Chaque règle joue l'un de trois rôles, et chaque rôle peut s'écrire de trois façons :

| Rôle | SQL | Appel externe | Code |
|---|---|---|---|
| **Jeu de données** | vue, commande d'instruction | API ou procédure qui renvoie des lignes | résolveur de code `@table`, `@stream`; commande `python` ou `script` |
| **Valeur** | expression de vue, métrique | — | commande `python` ou `script` |
| **Changement** | `INSERT`, `UPDATE`, `DELETE`, commande d'instruction | écriture d'API, mutation, procédure | `@command`; commande `python` ou `script` |

- **Appel externe.** L'API GraphQL, REST ou gRPC d'un autre système, ou la procédure stockée d'une base de données, enregistrée comme commande.
- **Des tables en arguments.** Une commande peut prendre des tables en arguments, si bien qu'un jeu de données peut être construit à partir d'autres jeux de données.
- **Métrique.** Un agrégat nommé, défini une fois, comme `net_revenue` défini par `SUM(orders.amount) - SUM(orders.refunds)`. Chaque requête le ventile selon les dimensions qu'elle demande, si bien que l'entrepôt, le tableau de bord et l'API lisent le même chiffre.

## Effets de bord cachés {: #side-effects }

Un effet de bord caché est un effet qu'on ne voit pas dans la définition de la règle.

Chaque règle est nommée, typée, enregistrée, auditée et sécurisée. Une commande peut aussi exiger une approbation avant de s'exécuter.

| | Effets de bord possibles | Limités par |
|---|---|---|
| **SQL** | Aucun | — |
| **Appel externe** | Les propres workflows, notifications et cascades de l'autre système | Liste d'autorisation de sortie (HTTP, gRPC) ; identifiants conservés comme secrets (procédures, opérations de source) |
| **Résolveur de code** | Appels à des services et réseaux externes ; résultats qui varient ; coût | Code figé, empreinte d'hôte figée, bac à sable, mesure |
| **Code sur l'hôte** | Lit tout ce que le serveur peut lire (mémoire, identifiants, fichiers, réseau) ; peut affamer le serveur ou le faire tomber | `commands.allow_host_code`, désactivé par défaut |

Un appel externe est typé par le schéma propre du système, ou par les arguments et colonnes de retour que vous déclarez en enregistrant la commande.

Le code d'un résolveur de code est figé par version de paquet, commit Git ou hash d'archive, si bien qu'il est promu avec le modèle.

Le code sur l'hôte est une commande `python` ou `script`. Il s'exécute dans le serveur avec tout son accès, si bien qu'un code malveillant, bogué ou gourmand peut lire tout ce que le serveur peut lire, l'affamer ou le faire tomber. C'est pratique, et sûr quand vous faites confiance au code. Le modèle ne contient qu'une référence (`module:attr` ou une ligne de commande), si bien que le code ne voyage pas avec la promotion. Il ne s'exécute que là où `commands.allow_host_code` est activé.

Ces limites contiennent les effets de bord mais ne peuvent pas les supprimer. Ce que fait un système externe ou votre propre code dépend en fin de compte de lui.

## Ce que le modèle vous apporte {: #what-you-get }

- Chaque règle est enregistrée dans un domaine, avec des droits.
- Chaque règle est gouvernée de la même façon, quelle que soit sa forme d'écriture.
- Chaque règle est visible dans le [lignage](lineage.md).

## À lire ensuite {: #read-next }

- [Vues](configuration.md#views-governed-computed-datasets)
- [Métriques](modeling.md#metrics)
- [Commandes](commands.md)
- [Resolvers de code](code-sources.md)
- [Contrôles de qualité des données](sources.md#data-quality-checkers)
