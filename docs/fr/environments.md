# Environnements {: #environments }

Un environnement est une copie nommée du modèle gouverné d'une organisation. Physiquement, la copie est un schéma PostgreSQL à part : un vrai schéma, sans colonne discriminante ni préfixe. Toute requête de dépôt existante est donc correcte dans un environnement sans être réécrite, et un prédicat oublié ne peut pas rendre les lignes d'un environnement lisibles depuis un autre.

Chaque organisation démarre avec un environnement nommé `prod`. Il ne peut être ni supprimé ni
renommé. Une requête qui ne nomme aucun environnement est servie par `prod` ; une requête nommant
un environnement inexistant est refusée.

Les environnements sont disponibles pour les organisations sur un forfait payant.

Pour remplir un environnement hors production avec des données sûres à utiliser, voir [Construire des environnements de test](test-data.md).

## Noms d'environnement {: #environment-names }

Un nom doit correspondre à `[a-z][a-z0-9_]{1,31}` : de deux à trente-deux lettres minuscules, chiffres et tirets bas, en commençant par une lettre. `prod` et les noms commençant par `pg_` sont refusés. La longueur maximale pour une organisation dépend de son propre id. PostgreSQL tronque en silence un identifiant de plus de 63 octets, et la limite protège contre le plus long nom de schéma qu'un environnement dérive.

## Ce qu'emporte une copie {: #what-a-copy-carries }

Chaque table du schéma de l'organisation appartient à exactement une classe. La classification est une liste d'autorisation : une table ajoutée plus tard ne voyage pas tant que personne n'a nommé sa classe ici, si bien qu'une table oubliée se voit sous forme de test rouge.

| Classe | Tables | Ce qui se passe à la copie |
| --- | --- | --- |
| CARRIED | domains, naming_rules, registered_tables, table_columns, relationships, metrics, roles, rls_rules, tags, tag_param_values, tag_assignments, glossary terms, materialized_views, calendars, api_endpoints, tracked_functions, tracked_webhooks, table_meta_links | Copiées intégralement |
| IDENTITY_ONLY | sources, api_sources, kafka_sources | Les champs d'identité et de gouvernance voyagent ; les valeurs de connexion restent sur place (voir Liaisons) |
| SEEDED_AT_CREATION | roles, user_role_assignments | Copiées uniquement à la première création d'un environnement ; les fusions ultérieures n'y touchent pas |
| PARTIAL | org_settings | Copiées clé par clé : les paramètres de gouvernance voyagent, les clés nommant une cible externe ou un runtime propre à l'environnement restent sur place |
| NEVER_SENSITIVE | org_secrets, user_directory | Jamais copiées |
| NEVER_RUNTIME | mv_refresh_log, relationship_candidates, admin_audit_log, et d'autres | Jamais copiées |

`SEEDED_AT_CREATION` existe pour résoudre un problème précis. Un nouvel environnement a besoin de
rôles et d'affectations, faute de quoi il ouvre sans que personne ne puisse agir. Mais une fusion
ultérieure qui emporterait la ligne `developer` de `prod` écraserait la version restreinte dont une
branche restreinte pourrait avoir besoin, faisant du chemin de revue la voie d'escalade. Les rôles
et les affectations voyagent donc une seule fois, à la création, et deviennent ensuite la réponse
propre à chaque environnement.

## Liaisons {: #bindings }

Les liaisons sont les colonnes qui disent où pointe réellement une source : `host`, `port`, `database`, `username` et les autres. Elles ne voyagent jamais dans une copie. Un environnement qui n'a pas été lié est marqué `unbound` au lieu d'être laissé vide, car un hôte vide n'est pas la même chose qu'aucun hôte, et le constructeur de connexion le lirait comme `localhost:5432`.

Un environnement créé autrement qu'en Unbound copie la connexion de chaque source depuis son parent,
exactement telle que le parent l'a écrite : une référence `${secret:...}` ou `${env:...}` est copiée comme cette
référence, jamais résolue. Après la création, la copie est la ligne propre de l'environnement, modifiable sur la
page Sources, et rien n'est résolu via le parent lorsque le runtime de l'environnement est construit.
Le champ `binding` de chaque source indique d'où vient sa connexion : `copied`, `own` (donnée dans cet
environnement) ou `unbound`. Modifier une connexion copiée la rend `own`. **Recopier depuis le parent**
(`POST /admin/orgs/{org}/environments/{name}/sources/recopy`) recopie les connexions du parent.
Une fusion ou un déploiement ne transporte aucune connexion, donc une source qu'il introduit arrive `unbound`.

## La projection git {: #the-git-projection }

Chaque écriture dans le modèle commite le résultat sur la branche git de l'environnement. Déployer un arbre demande un appel explicite : une pull request fusionnée sur l'hébergeur git ne se déploie pas d'elle-même.

Chaque entité a un fichier. Le chemin est l'adresse `provisa://` de l'entité sans le schéma ni l'organisation : `provisa://acme/sales/tables/Order` devient `sales/tables/Order.yaml`. Les sources vont dans `sources/`, les commands dans `commands/` et les métriques dans `metrics/`. Les lignes enfants qui dépendent d'un parent (colonnes, relations, règles RLS) sont écrites dans le fichier du parent, sans fichier propre.

Les commandes et leurs affectations d'étiquettes survivent à l'aller-retour. Une étiquette posée
sur une commande est routée vers le fichier propre à la commande (`commands/<name>.yaml`) ; une
étiquette qui n'appartient à aucun fichier disparaît de la projection et serait supprimée au
prochain déploiement de cet arbre.

Aucune clé de substitution n'arrive dans un fichier. `registered_tables.id` est un entier auto-incrémenté : le même modèle reçoit donc des entiers différents dans deux environnements, et un export naïf différerait de lui-même. Chaque clé de substitution est omise, et chaque référence à l'une d'elles est écrite comme le chemin de la cible.

La sérialisation est déterministe. Les clés sont émises par ordre alphabétique, les collections
filles triées par leur adresse, et le style YAML est figé. Deux environnements portant le même
modèle produisent des arbres identiques octet pour octet.

## Fusion {: #merge }

Fusionner le modèle d'un environnement dans un autre met à jour par identité : chaque objet que possède la source est créé ou mis à jour dans la cible. Les objets que la source n'a plus ne sont supprimés que si l'appelant demande explicitement des suppressions. Une fusion est une seule transaction ; si elle échoue en cours de route, la cible reste telle qu'elle était.

Avant d'appliquer, appelez l'endpoint d'aperçu (`GET /{name}/merge-preview`) ou passez
`dry_run: true`. L'aperçu emprunte exactement le même chemin de code que la fusion ; c'est un
endpoint `GET`, de sorte qu'un script de CI qui se trompe d'option ne puisse pas appliquer par
accident la fusion qu'il voulait inspecter.

Une fusion laisse les liaisons, les rôles et les secrets de la cible exactement tels qu'ils
étaient. Un environnement de développement ne perd pas ses propres connexions de bases de données
en prenant un modèle plus récent de prod. Prod n'acquiert pas les habilitations de dev.

### Ce que nomme le rapport {: #what-the-report-names }

Le rapport de fusion liste, par chemin, ce qui a été ajouté, modifié, supprimé et laissé tel quel. Il nomme aussi les **conflits** : les objets modifiés des deux côtés depuis leur dernier commit commun. Un conflit est signalé, et la source l'emporte, puisque c'est ce que signifie une fusion dans une cible. Provisa n'offre ni résolution de conflits, ni marqueurs de fusion, ni choix objet par objet. La liste des conflits vaut comme signal : deux personnes modifiaient le même objet sans le savoir.

Quand les deux côtés ont changé un objet vers la même valeur, ils sont d'accord, et il n'y a pas de conflit. Quand les deux environnements n'ont aucun ancêtre commun, la base vaut `None` dans le rapport, et une liste de conflits vide signifie que rien n'a été comparé, pas que rien n'est entré en collision.

La fusion arrive sous la forme d'un seul commit écrasé sur la branche de la cible. Le message de commit est obligatoire et ne peut pas être vide, car c'est la seule trace du travail que ce commit regroupe. Les commits de la source restent où ils sont et peuvent ensuite toujours être déployés par SHA.

## Pull

Un pull prend ce que le distant détient pour un environnement et en fait le modèle. Il n'avance pas
la branche locale en fast-forward directement ; il applique l'arbre récupéré par le chemin de
déploiement ordinaire, de sorte que la validation et l'audit qui régissent un déploiement manuel
régissent aussi un pull.

Comme une fusion, un pull signale ce qu'il a écrasé : les objets que l'arbre entrant a modifiés et que l'environnement local avait aussi modifiés depuis le dernier commit commun des deux lignes. Une modification locale jamais commitée rend l'environnement dérivé (voir Historique plus bas), et un pull la nomme comme un changement ordinaire dans le rapport.

Un pull est refusé quand les deux lignes ont **divergé**, c'est-à-dire que chacune a des commits que l'autre n'a pas. Le refus liste les objets touchés des deux côtés, pour que la personne qui doit maintenant décider quel travail survit sache quels objets examiner.

## Historique {: #history }

Chaque déploiement fait avancer le curseur de l'environnement sur sa propre ligne de commits. Annuler recule d'un commit, et rétablir avance de nouveau vers le point de départ de l'annulation. Aucun des deux ne supprime de commit : reculer ajoute une position et laisse l'historique intact.

Une branche est amorcée à la pointe de l'environnement dont elle est issue : une annulation
s'arrête donc à ce point d'amorçage et ne s'aventure pas sur les commits de l'environnement parent.

Les indicateurs `can_undo` et `can_redo` accompagnent la réponse de la liste des environnements. Tous deux valent `false` quand la projection ne contient pas le commit que nomme le plan de contrôle, un état que la conception admet et appelle **dérivé**. Un nœud dont le stockage de dépôt n'a jamais reçu un commit donné liste toujours ses environnements ; seules les réponses d'historique changent.

## Autorisation {: #authorization }

Les environnements sont régis par deux droits. Aucun des deux n'appartient à un analyste par défaut
.

| Droit | Qui le détient (à l'amorçage) | Ce qu'il régit |
| --- | --- | --- |
| `environment_management` | org_admin, developer | Créer et supprimer des environnements |
| `environment_switch` | org_admin, developer | Être servi par un environnement autre que prod |

`prod` ne demande aucun droit. C'est lui qui sert une requête ne nommant aucun environnement, et le refuser reviendrait à refuser toutes les requêtes.

La vérification a lieu là où l'environnement est choisi, avant d'atteindre la moindre route. Un membre sans `environment_switch` est refusé sur toutes les surfaces à la fois (HTTP, GraphQL, SQL et les protocoles filaires), car l'environnement est lié dans le middleware et non dans chaque gestionnaire.

Un analyste ne portant aucun droit d'environnement peut interroger `prod` et ne voit pas le
sélecteur d'environnement. Un prestataire à qui l'on accorde le rôle d'analyste ne voit aucune
surface d'environnements et ne peut ni créer ni rejoindre un environnement autre que la production.

### Autorité du propriétaire d'un environnement {: #environment-owner-authority }

Créer un environnement est le seul moyen pour un membre en lecture seule d'obtenir des droits de modification du modèle. Dans un environnement qu'il a créé, le créateur détient les capacités du rôle `developer`, moins les droits sur les données (`write`, `full_results`, `usage`) : des droits pour construire le modèle, sans droits sur les données.

L'octroi est dérivé d'`environments.created_by` au moment de l'autorisation, jamais écrit dans une
table d'octrois. Supprimer l'environnement le retire du même geste.

L'appartenance aux domaines limite toujours ce que le propriétaire peut modifier. Créer une branche
change ce qu'un membre peut faire ; cela ne change jamais à quels domaines il peut le faire
.

## Environnements protégés {: #protected-environments }

Un environnement peut être protégé. Une fusion ou un déploiement vers un environnement protégé
n'est pas appliqué au moment de la demande ; il est proposé, et quelqu'un d'autre que le demandeur
doit l'approuver.

`prod` devient protégé automatiquement dès que l'organisation compte plus d'un membre. Une organisation d'un seul membre ne peut pas satisfaire « quelqu'un d'autre que le demandeur », donc la règle ne s'y applique pas ; elle rendrait toute fusion dans `prod` impossible. Un org_admin peut marquer n'importe quel environnement comme protégé.

Une demande de fusion est une ligne enregistrée, qu'aucune boîte de confirmation ne remplace. L'approbateur est par définition quelqu'un d'autre que le demandeur et n'est pas présent au moment de la demande. Une confirmation éphémère forcerait l'approbation dans la session du demandeur, exactement ce que la règle interdit.

La ligne de la demande porte le rapport de fusion aux côtés du message du demandeur. L'obsolescence
est dérivée au moment de la lecture, jamais stockée : replanifier à la lecture et comparer au
rapport stocké est la seule version qui ne puisse pas se tromper. Une demande obsolète doit être
redemandée. Le demandeur ne peut pas approuver sa propre demande.

États du cycle de vie d'une demande : `requested` → `approved`/`rejected` → `applied`. `stale` est
dérivé.

Les déploiements depuis une référence du dépôt passent par la même porte, et la demande fige le SHA au moment de la proposition. Si la référence bouge entre la proposition et la décision, l'approbateur lit le rapport du commit figé, pas celui du nouveau.

! ! ! note
    L'interface des demandes de fusion se trouve sous l'onglet **Demandes de fusion** du panneau
    Environnements. La colonne **Rapport** montre ce qui changerait, en nombre ; la ligne se
    déplie pour montrer le détail objet par objet.

## Les commandes CLI `env` {: #the-env-cli-commands }

`provisa env deploy` envoie le modèle d'une référence dans un environnement. Il se termine par 0 quand le déploiement a été appliqué ou était un essai à blanc, et par 2 quand l'environnement est protégé et que le déploiement n'a été que proposé. Un pipeline qui prendrait une approbation en attente pour un déploiement livré se tromperait, et le code de sortie fait la différence.

```
provisa env deploy --org acme --env prod --ref main --token <token> --api <url>
```

`provisa env fetch` amène les branches distantes de l'organisation dans le dépôt local. Un
déploiement peut alors nommer `origin/<branch>`.

```
provisa env fetch --org acme --api <url> --token <token>
```

Les deux commandes acceptent `--api` (l'URL de l'API Provisa) et `--token` (un jeton bearer).
Définissez `PROVISA_API_URL` et `PROVISA_API_TOKEN` dans l'environnement pour éviter de les passer
à chaque appel.

Le pipeline de CI type pour un flux adossé à un dépôt :

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
provisa env deploy --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## Voir aussi {: #see-also }

- [Déploiement](deployment.md) — comment monter le plan de contrôle auquel les environnements se connectent
- [Commandes](commands.md) — fonctions et webhooks suivis qui apparaissent dans l'arbre de chaque environnement
- [Construire des environnements de test](test-data.md) — profiler la production, déclarer des données factices, générer des données synthétiques pour un environnement hors production
- [Méthodes de données factices](fake-methods.md) — chaque type de donnée factice et chaque méthode qu'une colonne peut déclarer
