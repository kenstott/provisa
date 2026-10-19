# Environnements {: #environments }

Un environnement est une copie nommée du modèle gouverné d'une organisation. La copie est
physiquement un schéma PostgreSQL distinct — pas une colonne discriminante, pas un préfixe, un
véritable schéma — de sorte que chaque requête existante du dépôt est correcte à l'intérieur d'un
environnement sans rien réécrire, et que les lignes d'un environnement ne peuvent pas atteindre la
lecture d'un autre par la faute d'un prédicat oublié.

Chaque organisation démarre avec un environnement nommé `prod`. Il ne peut être ni supprimé ni
renommé. Une requête qui ne nomme aucun environnement est servie par `prod` ; une requête nommant
un environnement inexistant est refusée.

Les environnements sont disponibles pour les organisations sur un forfait payant.

Pour remplir un environnement hors production avec des données sûres à utiliser, voir [Construire des environnements de test](test-data.md).

## Noms d'environnement {: #environment-names }

Un nom doit correspondre à `[a-z][a-z0-9_]{1,31}` — de deux à trente-deux caractères parmi les
lettres minuscules, les chiffres et les tirets bas, commençant par une lettre. `prod` et les noms
commençant par `pg_` sont refusés. La longueur maximale pour une organisation donnée dépend de son
propre identifiant : PostgreSQL tronque silencieusement un identifiant dépassant 63 octets, et le
plus long nom de schéma qu'un environnement dérive est ce contre quoi le plafond protège.

## Ce qu'emporte une copie {: #what-a-copy-carries }

Chaque table du schéma de l'organisation appartient à exactement une classe. La
classification est une liste d'autorisation, pas une liste d'exclusion : une table ajoutée
ultérieurement ne voyage pas tant que quelqu'un n'a pas nommé sa classe ici, si bien que le mode de
défaillance d'une table oubliée est un test rouge.

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

Les liaisons sont les colonnes qui disent où une source pointe réellement — `host`, `port`,
`database`, `username` et les autres. Elles ne voyagent jamais dans aucune copie. Un environnement
qui n'a pas été lié est marqué `unbound` plutôt que laissé vide : un hôte vide n'est pas un hôte
absent, et le constructeur de connexion le lirait comme `localhost:5432`.

Un environnement créé autrement qu'en Unbound copie la connexion de chaque source depuis son parent,
exactement telle que le parent l'a écrite : une référence `${secret:...}` ou `${env:...}` est copiée comme cette
référence, jamais résolue. Après la création, la copie est la ligne propre de l'environnement, modifiable sur la
page Sources, et rien n'est résolu via le parent lorsque le runtime de l'environnement est construit.
Le champ `binding` de chaque source indique d'où vient sa connexion : `copied`, `own` (donnée dans cet
environnement) ou `unbound`. Modifier une connexion copiée la rend `own`. **Recopier depuis le parent**
(`POST /admin/orgs/{org}/environments/{name}/sources/recopy`) recopie les connexions du parent.
Une fusion ou un déploiement ne transporte aucune connexion, donc une source qu'il introduit arrive `unbound`.

## La projection git {: #the-git-projection }

Chaque écriture dans le modèle commite le résultat dans la branche git de l'environnement. Déployer un arbre
exige un appel explicite — une pull request fusionnée sur l'hôte git ne se déploie pas d'elle-même.

Chaque entité obtient un fichier. Le chemin est l'adresse `provisa://` de l'entité privé de son schéma d'URI et de son
organisation : `provisa://acme/sales/tables/Order` devient `sales/tables/Order.yaml`. Les sources
atterrissent dans `sources/`, les commandes dans `commands/`, les métriques dans `metrics/`. Les
lignes filles qui cascadent depuis un parent — colonnes, relations, règles RLS — sont écrites à
l'intérieur du fichier du parent, non comme des fichiers à part.

Les commandes et leurs affectations d'étiquettes survivent à l'aller-retour. Une étiquette posée
sur une commande est routée vers le fichier propre à la commande (`commands/<name>.yaml`) ; une
étiquette qui n'appartient à aucun fichier disparaît de la projection et serait supprimée au
prochain déploiement de cet arbre.

Aucune clé de substitution n'atteint un fichier. `registered_tables.id` est un entier
auto-incrémenté — le même modèle dans deux environnements reçoit des entiers différents, si bien
qu'un vidage naïf produit un diff avec lui-même. Chaque substitut est abandonné et chaque référence
à l'un d'eux est écrite comme le chemin de sa cible.

La sérialisation est déterministe. Les clés sont émises par ordre alphabétique, les collections
filles triées par leur adresse, et le style YAML est figé. Deux environnements portant le même
modèle produisent des arbres identiques octet pour octet.

## Fusion {: #merge }

Fusionner le modèle d'un environnement dans un autre met à jour par identité : chaque objet que la
source possède est créé ou mis à jour dans la cible. Les objets que la source ne possède plus ne
sont retirés que si l'appelant demande explicitement les retraits. Une fusion qui échoue en cours
de route laisse la cible telle qu'elle était — une seule transaction.

Avant d'appliquer, appelez l'endpoint d'aperçu (`GET /{name}/merge-preview`) ou passez
`dry_run: true`. L'aperçu emprunte exactement le même chemin de code que la fusion ; c'est un
endpoint `GET`, de sorte qu'un script de CI qui se trompe d'option ne puisse pas appliquer par
accident la fusion qu'il voulait inspecter.

Une fusion laisse les liaisons, les rôles et les secrets de la cible exactement tels qu'ils
étaient. Un environnement de développement ne perd pas ses propres connexions de bases de données
en prenant un modèle plus récent de prod. Prod n'acquiert pas les habilitations de dev.

### Ce que nomme le rapport {: #what-the-report-names }

Le rapport de fusion liste, par chemin, ce qui a été ajouté, modifié, retiré et laissé inchangé. Il
nomme également les éventuels **conflits** — les objets que les deux côtés ont modifiés depuis leur
dernier commit commun. Un conflit est signalé et non résolu : la source l'emporte, ce qui est le
sens même d'une fusion dans une cible. Provisa n'offre aucune résolution de conflit, aucun marqueur
de fusion, aucun choix objet par objet. La valeur de la liste des conflits est le signal — deux
personnes éditaient le même objet sans le savoir.

Un objet que les deux côtés ont modifié vers la même valeur est un accord, pas un conflit. Lorsque
les deux environnements ne partagent aucun ancêtre, la base vaut `None` dans le rapport et la liste
de conflits vide signifie que rien n'a été comparé, non que rien ne s'est heurté.

La fusion atterrit en un seul commit écrasé sur la branche de la cible. Le message de commit est
obligatoire et ne doit pas être vide — il est le seul compte rendu de la plage de travail que
l'écrasement représente. Les commits de la source restent où ils sont et demeurent déployables par
SHA par la suite.

## Pull

Un pull prend ce que le distant détient pour un environnement et en fait le modèle. Il n'avance pas
la branche locale en fast-forward directement ; il applique l'arbre récupéré par le chemin de
déploiement ordinaire, de sorte que la validation et l'audit qui régissent un déploiement manuel
régissent aussi un pull.

Comme une fusion, un pull rapporte ce qu'il a écrasé — les objets que l'arbre entrant a modifiés et
que l'environnement local avait modifiés lui aussi depuis le dernier commit commun des deux lignes.
Une modification locale non validée est un environnement en dérive (voir Historique ci-dessous) ;
un pull la nomme comme une modification ordinaire dans le rapport.

Un pull est refusé lorsque les deux lignes ont **divergé** — chacune détient des commits que
l'autre n'a pas. Le refus emporte la liste des objets que les deux côtés ont touchés, afin que la
personne qui doit maintenant décider quel travail survit sache quels objets regarder.

## Historique {: #history }

Chaque déploiement avance le curseur de l'environnement dans sa propre ligne de commits. Une
annulation recule d'un commit ; un rétablissement avance de nouveau vers la position que
l'annulation avait quittée. Aucune des deux opérations ne supprime de commit — reculer ajoute une
position, cela ne réécrit pas l'historique.

Une branche est amorcée à la pointe de l'environnement dont elle est issue : une annulation
s'arrête donc à ce point d'amorçage et ne s'aventure pas sur les commits de l'environnement parent.

Les indicateurs `can_undo` et `can_redo` voyagent avec la réponse listant les environnements. Tous
deux valent `false` lorsque la projection ne détient pas le commit que le plan de contrôle nomme —
un état que la conception admet, appelé **en dérive**. Un nœud dont le magasin de dépôts n'a jamais
reçu un commit donné liste tout de même ses environnements ; seules les réponses d'historique
changent.

## Autorisation {: #authorization }

Les environnements sont régis par deux droits. Aucun des deux n'appartient à un analyste par défaut
.

| Droit | Qui le détient (à l'amorçage) | Ce qu'il régit |
| --- | --- | --- |
| `environment_management` | org_admin, developer | Créer et supprimer des environnements |
| `environment_switch` | org_admin, developer | Être servi par un environnement autre que prod |

`prod` n'exige aucun droit — c'est lui qui sert une requête ne nommant rien, et le refuser
reviendrait à refuser chaque requête.

L'application a lieu au point de sélection, avant qu'aucune route ne soit atteinte. Un membre
dépourvu d'`environment_switch` est refusé sur toutes les surfaces d'un coup — HTTP, GraphQL, SQL
et les protocoles de niveau fil — parce que l'environnement est lié dans l'intergiciel, non dans
les gestionnaires individuels.

Un analyste ne portant aucun droit d'environnement peut interroger `prod` et ne voit pas le
sélecteur d'environnement. Un prestataire à qui l'on accorde le rôle d'analyste ne voit aucune
surface d'environnements et ne peut ni créer ni rejoindre un environnement autre que la production.

### Autorité du propriétaire d'un environnement {: #environment-owner-authority }

Créer un environnement est le seul chemin par lequel un membre en lecture seule acquiert des droits
d'édition du modèle. À l'intérieur de l'environnement qu'il a créé, le créateur détient
les capacités du rôle `developer` — moins les droits sur les données (`write`, `full_results`,
`usage`). Des droits de construction du modèle, pas des droits sur les données.

L'octroi est dérivé d'`environments.created_by` au moment de l'autorisation, jamais écrit dans une
table d'octrois. Supprimer l'environnement le retire du même geste.

L'appartenance aux domaines limite toujours ce que le propriétaire peut modifier. Créer une branche
change ce qu'un membre peut faire ; cela ne change jamais à quels domaines il peut le faire
.

## Environnements protégés {: #protected-environments }

Un environnement peut être protégé. Une fusion ou un déploiement vers un environnement protégé
n'est pas appliqué au moment de la demande ; il est proposé, et quelqu'un d'autre que le demandeur
doit l'approuver.

`prod` est protégé automatiquement dès que l'organisation compte plus d'un membre. Une organisation
à membre unique ne peut pas satisfaire « quelqu'un d'autre que le demandeur » : la règle n'y est
donc pas appliquée — elle rendrait `prod` impossible à fusionner. Tout environnement peut être
marqué protégé par un org_admin.

Une demande de fusion est une ligne, pas une boîte de dialogue de confirmation. L'approbateur est
par définition une personne différente du demandeur et n'est pas présent au moment de la demande ;
une confirmation éphémère forcerait l'approbation à l'intérieur de la session du demandeur, soit le
seul agencement que l'exigence interdit.

La ligne de la demande porte le rapport de fusion aux côtés du message du demandeur. L'obsolescence
est dérivée au moment de la lecture, jamais stockée : replanifier à la lecture et comparer au
rapport stocké est la seule version qui ne puisse pas se tromper. Une demande obsolète doit être
redemandée. Le demandeur ne peut pas approuver sa propre demande.

États du cycle de vie d'une demande : `requested` → `approved`/`rejected` → `applied`. `stale` est
dérivé.

La même porte gère les déploiements depuis une référence de dépôt : la demande épingle le SHA au
moment de la proposition. Si la référence bouge entre la proposition et la décision, l'approbateur
lit le rapport du commit épinglé, non du nouveau.

! ! ! note
    L'interface des demandes de fusion se trouve sous l'onglet **Demandes de fusion** du panneau
    Environnements. La colonne **Rapport** montre ce qui changerait, en nombre ; la ligne se
    déplie pour montrer le détail objet par objet.

## Les commandes CLI `env` {: #the-env-cli-commands }

`provisa env deploy` envoie le modèle situé à une référence dans un environnement. Elle sort avec 0
lorsque le déploiement a été appliqué ou n'était qu'une simulation, et avec 2 lorsque
l'environnement est protégé et que le déploiement n'a été que proposé — un pipeline qui traiterait
une approbation en attente comme un déploiement livré aurait tort, et le code de sortie le dit.

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
