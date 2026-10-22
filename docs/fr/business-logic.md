# La logique métier dans Provisa

## Le problème {: #the-problem }

La logique métier finit partout : dans les jobs ETL, les tableaux de bord et les API, détenue par des équipes différentes et cataloguée par personne. Le « chiffre d'affaires net » est calculé d'une façon dans le job de l'entrepôt, d'une autre dans le tableau de bord financier et d'une troisième dans l'API de facturation, et personne ne peut dire laquelle est la bonne.

Ce n'est la faute de personne. Il n'existait aucun modèle commun et simple où placer les règles.

## Ce qu'est la logique métier {: #definition }

La logique métier, ce sont les règles qui créent, modifient, suppriment ou transforment des données.

La terminologie métier est autre chose. Les noms et descriptions des domaines, des tables et des colonnes, et les termes du [glossaire](glossary.md), disent ce que signifient les données. La logique métier dit ce qu'on en fait.

## Trois rôles {: #roles }

Chaque règle joue l'un de trois rôles, et chaque rôle peut s'écrire de trois façons :

| Rôle | SQL | Appel externe | Resolver de code |
|---|---|---|---|
| **Jeu de données** | vue, commande d'instruction | API ou procédure qui renvoie des lignes | `@table`, `@stream` |
| **Valeur** | expression de vue, métrique | — | — |
| **Changement** | `INSERT`, `UPDATE`, `DELETE`, commande d'instruction | écriture d'API, mutation, procédure | `@command` |

- **Appel externe.** L'API GraphQL, REST ou gRPC d'un autre système, ou la procédure stockée d'une base de données, enregistrée comme commande.
- **Métrique.** Un agrégat nommé, défini une fois, comme `net_revenue` défini par `SUM(orders.amount) - SUM(orders.refunds)`. Chaque requête le ventile selon les dimensions qu'elle demande, si bien que l'entrepôt, le tableau de bord et l'API lisent le même chiffre.

## Effets de bord cachés {: #side-effects }

Un effet de bord caché est un effet qu'on ne voit pas dans la définition de la règle.

Chaque règle est nommée, typée, enregistrée, auditée et sécurisée.

| | Effets de bord possibles | Limités par |
|---|---|---|
| **SQL** | Aucun | — |
| **Appel externe** | Les propres workflows, notifications et cascades de l'autre système | Approbation |
| **Resolver de code** | Appels à des services et réseaux externes ; résultats qui varient ; coût | Sandbox, mesure |

Un appel externe est typé par le schéma propre du système, ou par les arguments et colonnes de retour que vous déclarez en enregistrant la commande.

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
