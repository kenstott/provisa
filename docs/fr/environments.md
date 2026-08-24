# Environnements

Un environnement est une copie nommée du modèle gouverné d'une organisation. La copie est
physiquement un schéma PostgreSQL distinct — pas une colonne discriminante, pas un préfixe, un
véritable schéma — de sorte que chaque requête existante du dépôt est correcte à l'intérieur d'un
environnement sans rien réécrire, et que les lignes d'un environnement ne peuvent pas atteindre la
lecture d'un autre par la faute d'un prédicat oublié (REQ-1487, REQ-1488).
[tool-verified: `environments.py` module docstring ; `org_schema()` at environments.py lines 86-96]

Chaque organisation démarre avec un environnement nommé `prod`. Il ne peut être ni supprimé ni
renommé. Une requête qui ne nomme aucun environnement est servie par `prod` ; une requête nommant
un environnement inexistant est refusée. [tool-verified: `PROD = "prod"` at environments.py line 44 ; `select_environment()`
at env_routing.py lines 93-129]

Les environnements sont disponibles pour les organisations sur un forfait payant. [inferred: REQ-1507]

## Noms d'environnement

Un nom doit correspondre à `[a-z][a-z0-9_]{1,31}` — de deux à trente-deux caractères parmi les
lettres minuscules, les chiffres et les tirets bas, commençant par une lettre. `prod` et les noms
commençant par `pg_` sont refusés. La longueur maximale pour une organisation donnée dépend de son
propre identifiant : PostgreSQL tronque silencieusement un identifiant dépassant 63 octets, et le
plus long nom de schéma qu'un environnement dérive est ce contre quoi le plafond protège.
[tool-verified: `ENV_NAME_PATTERN` at environments.py line 59 ; `validate_env_name()` at
environments.py lines 119-142 ; `max_env_name_length()` at environments.py lines 108-116]

## Ce qu'emporte une copie

Chaque table du schéma de l'organisation appartient à exactement une classe (REQ-1489). La
classification est une liste d'autorisation, pas une liste d'exclusion : une table ajoutée
ultérieurement ne voyage pas tant que quelqu'un n'a pas nommé sa classe ici, si bien que le mode de
défaillance d'une table oubliée est un test rouge. [tool-verified: `CLASSIFIED`
constant and module docstring, env_classes.py lines 19-22]

| Classe | Tables | Ce qui se passe à la copie |
| --- | --- | --- |
| CARRIED | domains, naming_rules, registered_tables, table_columns, relationships, metrics, roles, rls_rules, tags, tag_param_values, tag_assignments, glossary terms, materialized_views, calendars, api_endpoints, tracked_functions, tracked_webhooks, table_meta_links | Copiées intégralement |
| IDENTITY_ONLY | sources, api_sources, kafka_sources, kafka_sinks | Les champs d'identité et de gouvernance voyagent ; les valeurs de connexion restent sur place (voir Liaisons) |
| SEEDED_AT_CREATION | roles, user_role_assignments | Copiées uniquement à la première création d'un environnement ; les fusions ultérieures n'y touchent pas |
| PARTIAL | org_settings | Copiées clé par clé : les paramètres de gouvernance voyagent, les clés nommant une cible externe ou un runtime propre à l'environnement restent sur place |
| NEVER_SENSITIVE | org_secrets, user_directory | Jamais copiées |
| NEVER_RUNTIME | mv_refresh_log, relationship_candidates, admin_audit_log, et d'autres | Jamais copiées |

[tool-verified: `CARRIED`, `IDENTITY_ONLY`, `SEEDED_AT_CREATION`, `PARTIAL`, `NEVER_SENSITIVE`,
`NEVER_RUNTIME` frozensets, env_classes.py lines 29-113]

`SEEDED_AT_CREATION` existe pour résoudre un problème précis. Un nouvel environnement a besoin de
rôles et d'affectations, faute de quoi il ouvre sans que personne ne puisse agir. Mais une fusion
ultérieure qui emporterait la ligne `developer` de `prod` écraserait la version restreinte dont une
branche restreinte pourrait avoir besoin, faisant du chemin de revue la voie d'escalade. Les rôles
et les affectations voyagent donc une seule fois, à la création, et deviennent ensuite la réponse
propre à chaque environnement. [tool-verified: env_classes.py lines 65-71 ; env_copy.py lines 41-44]

## Liaisons

Les liaisons sont les colonnes qui disent où une source pointe réellement — `host`, `port`,
`database`, `username` et les autres. Elles ne voyagent jamais dans aucune copie. Un environnement
qui n'a pas été lié est marqué `unbound` plutôt que laissé vide : un hôte vide n'est pas un hôte
absent, et le constructeur de connexion le lirait comme `localhost:5432`. [tool-verified: `BOUND_COLUMN = "bound"` at
env_classes.py line 143 ; `BINDING_COLUMNS` dict at env_classes.py lines 155-172]

Les sources d'un environnement se résolvent de l'une des deux façons suivantes.

**Base** — l'environnement porte ses propres identifiants. Un org_admin crée une base puis lie
chaque source explicitement. [tool-verified: `CreateEnvBody.inherit_connections = False` (default) at
environments_router.py line 227 ; "binding a base is an org_admin's act" comment at line 358]

**Branche** — l'environnement hérite des identifiants de la base par référence. Rien n'est copié.
Lorsqu'une requête a besoin d'une connexion, la résolution remonte la chaîne `branched_from` et
s'arrête au premier environnement dont la ligne est liée. Faire tourner un identifiant sur la base
se propage à chacune de ses branches sans aucune action à mener. Le révoquer le révoque pour toutes
d'un coup. Aucun secret n'est jamais matérialisé là où une branche, un export ou un dépôt pourrait
l'emporter.
[tool-verified: `resolve()` at env_bindings.py lines 114-151 ; `lineage()` at env_bindings.py
lines 74-102 ; env_bindings.py module docstring lines 11-33]

Pour créer une branche, cochez **Hériter des connexions** dans le panneau Environnements. Par
défaut, l'option est désactivée.
[tool-verified: `environmentsTab.json` key `inheritConnections`; `inheritHelp2` string]

## La projection git

Chaque écriture dans le modèle valide le résultat sur la branche git de l'environnement. Le dépôt
est une projection du modèle, jamais son autorité : Provisa lit et écrit le plan de contrôle ; le
dépôt en est le registre, pas la source. Déployer un arbre exige un appel explicite — une pull
request fusionnée sur l'hôte git ne se déploie pas d'elle-même (REQ-1524, REQ-1526). [tool-verified:
deploy endpoint docstring at environments_router.py lines 777-791]

Chaque entité obtient un fichier. Le chemin est l'URI REQ-1385 privé de son schéma d'URI et de son
organisation : `provisa://acme/sales/tables/Order` devient `sales/tables/Order.yaml`. Les sources
atterrissent dans `sources/`, les commandes dans `commands/`, les métriques dans `metrics/`. Les
lignes filles qui cascadent depuis un parent — colonnes, relations, règles RLS — sont écrites à
l'intérieur du fichier du parent, non comme des fichiers à part.
[tool-verified: `table_path()` at env_files.py line 109-115 ; `kind_path()` at env_files.py
lines 118-120 ; `COMMANDS_DIR = "commands"` at env_project.py line 71 ; env_files.py module
docstring lines 17-24]

Les commandes et leurs affectations d'étiquettes survivent à l'aller-retour. Une étiquette posée
sur une commande est routée vers le fichier propre à la commande (`commands/<name>.yaml`) ; une
étiquette qui n'appartient à aucun fichier disparaît de la projection et serait supprimée au
prochain déploiement de cet arbre. [tool-verified:
env_project.py lines 346-364 ; `owner_command_name` routing in `_assignments_for()` at
env_project.py lines 137-164]

Aucune clé de substitution n'atteint un fichier. `registered_tables.id` est un entier
auto-incrémenté — le même modèle dans deux environnements reçoit des entiers différents, si bien
qu'un vidage naïf produit un diff avec lui-même. Chaque substitut est abandonné et chaque référence
à l'un d'eux est écrite comme le chemin de sa cible.
[tool-verified: `STORAGE_COLUMNS` and `_model_columns()` at env_files.py lines 62-128 ;
env_project.py docstring lines 26-27]

La sérialisation est déterministe. Les clés sont émises par ordre alphabétique, les collections
filles triées par leur adresse, et le style YAML est figé. Deux environnements portant le même
modèle produisent des arbres identiques octet pour octet. [tool-verified: `dump()` at env_files.py lines 131-143]

## Fusion

Fusionner le modèle d'un environnement dans un autre met à jour par identité : chaque objet que la
source possède est créé ou mis à jour dans la cible. Les objets que la source ne possède plus ne
sont retirés que si l'appelant demande explicitement les retraits. Une fusion qui échoue en cours
de route laisse la cible telle qu'elle était — une seule transaction. [tool-verified: `copy_model()` at env_copy.py lines 216-234 ; REQ-1490 description]

Avant d'appliquer, appelez l'endpoint d'aperçu (`GET /{name}/merge-preview`) ou passez
`dry_run: true`. L'aperçu emprunte exactement le même chemin de code que la fusion ; c'est un
endpoint `GET`, de sorte qu'un script de CI qui se trompe d'option ne puisse pas appliquer par
accident la fusion qu'il voulait inspecter. [tool-verified:
`preview_merge()` docstring at environments_router.py lines 1086-1095]

Une fusion laisse les liaisons, les rôles et les secrets de la cible exactement tels qu'ils
étaient. Un environnement de développement ne perd pas ses propres connexions de bases de données
en prenant un modèle plus récent de prod. Prod n'acquiert pas les habilitations de dev.
[tool-verified: env_copy.py lines 269-287 ; REQ-1490 scenario]

### Ce que nomme le rapport

Le rapport de fusion liste, par chemin, ce qui a été ajouté, modifié, retiré et laissé inchangé. Il
nomme également les éventuels **conflits** — les objets que les deux côtés ont modifiés depuis leur
dernier commit commun. Un conflit est signalé et non résolu : la source l'emporte, ce qui est le
sens même d'une fusion dans une cible. Provisa n'offre aucune résolution de conflit, aucun marqueur
de fusion, aucun choix objet par objet. La valeur de la liste des conflits est le signal — deux
personnes éditaient le même objet sans le savoir (REQ-1555).
[tool-verified: `CopyReport.conflicts` at env_copy.py lines 151-165 ; `detect_conflicts()` called
at env_copy.py lines 261-263 ; REQ-1555 description]

Un objet que les deux côtés ont modifié vers la même valeur est un accord, pas un conflit. Lorsque
les deux environnements ne partagent aucun ancêtre, la base vaut `None` dans le rapport et la liste
de conflits vide signifie que rien n'a été comparé, non que rien ne s'est heurté. [tool-verified: `CopyReport.compared`
property at env_copy.py lines 164-166 ; env_copy.py lines 255-264]

La fusion atterrit en un seul commit écrasé sur la branche de la cible. Le message de commit est
obligatoire et ne doit pas être vide — il est le seul compte rendu de la plage de travail que
l'écrasement représente. Les commits de la source restent où ils sont et demeurent déployables par
SHA par la suite.
[tool-verified: `_squash()` docstring at environments_router.py lines 663-680 ;
`MergeBody.message` comment at environments_router.py lines 258-260]

## Pull

Un pull prend ce que le distant détient pour un environnement et en fait le modèle. Il n'avance pas
la branche locale en fast-forward directement ; il applique l'arbre récupéré par le chemin de
déploiement ordinaire, de sorte que la validation et l'audit qui régissent un déploiement manuel
régissent aussi un pull.
[tool-verified: `pull_environment()` docstring at environments_router.py lines 1450-1462]

Comme une fusion, un pull rapporte ce qu'il a écrasé — les objets que l'arbre entrant a modifiés et
que l'environnement local avait modifiés lui aussi depuis le dernier commit commun des deux lignes.
Une modification locale non validée est un environnement en dérive (voir Historique ci-dessous) ;
un pull la nomme comme une modification ordinaire dans le rapport.
[tool-verified: REQ-1556 description ; `pull_environment()` at environments_router.py
lines 1485-1519]

Un pull est refusé lorsque les deux lignes ont **divergé** — chacune détient des commits que
l'autre n'a pas. Le refus emporte la liste des objets que les deux côtés ont touchés, afin que la
personne qui doit maintenant décider quel travail survit sache quels objets regarder. [tool-verified: `state["diverged"]` check at
environments_router.py lines 1491-1503 ; `_collisions()` at environments_router.py
lines 1581-1602]

## Historique

Chaque déploiement avance le curseur de l'environnement dans sa propre ligne de commits. Une
annulation recule d'un commit ; un rétablissement avance de nouveau vers la position que
l'annulation avait quittée. Aucune des deux opérations ne supprime de commit — reculer ajoute une
position, cela ne réécrit pas l'historique.
[tool-verified: `_move()` docstring at environments_router.py lines 854-868]

Une branche est amorcée à la pointe de l'environnement dont elle est issue : une annulation
s'arrête donc à ce point d'amorçage et ne s'aventure pas sur les commits de l'environnement parent.
[tool-verified:
`origin_sha` comment at environments_router.py lines 428-448 ; `_move()` at
environments_router.py lines 907-916]

Les indicateurs `can_undo` et `can_redo` voyagent avec la réponse listant les environnements. Tous
deux valent `false` lorsque la projection ne détient pas le commit que le plan de contrôle nomme —
un état que la conception admet, appelé **en dérive**. Un nœud dont le magasin de dépôts n'a jamais
reçu un commit donné liste tout de même ses environnements ; seules les réponses d'historique
changent (REQ-1561). [tool-verified: `_with_history()`
at environments_router.py lines 316-344 ; REQ-1561 description]

## Autorisation

Les environnements sont régis par deux droits. Aucun des deux n'appartient à un analyste par défaut
(REQ-1573).
[tool-verified: REQ-1573 description ; `MANAGE_CAPABILITY = "environment_management"` and
`SWITCH_CAPABILITY = "environment_switch"` at environments_router.py line 110 and
env_routing.py line 53]

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
[tool-verified: `select_environment()` at env_routing.py lines 93-129 ; env_routing.py
module docstring lines 28-34]

Un analyste ne portant aucun droit d'environnement peut interroger `prod` et ne voit pas le
sélecteur d'environnement. Un prestataire à qui l'on accorde le rôle d'analyste ne voit aucune
surface d'environnements et ne peut ni créer ni rejoindre un environnement autre que la production.
[tool-verified: REQ-1573 use_case and scenario]

### Autorité du propriétaire d'un environnement

Créer un environnement est le seul chemin par lequel un membre en lecture seule acquiert des droits
d'édition du modèle (REQ-1528). À l'intérieur de l'environnement qu'il a créé, le créateur détient
les capacités du rôle `developer` — moins les droits sur les données (`write`, `full_results`,
`usage`). Des droits de construction du modèle, pas des droits sur les données. [tool-verified: `ENVIRONMENT_OWNER_CAPABILITIES` at env_authority.py lines 75-77 ;
`_DATA_RIGHTS` at env_authority.py lines 74-77 ; env_authority.py module docstring lines 14-38]

L'octroi est dérivé d'`environments.created_by` au moment de l'autorisation, jamais écrit dans une
table d'octrois. Supprimer l'environnement le retire du même geste.
[tool-verified: env_authority.py module docstring lines 39-42 ; `environment_owner()` at
env_authority.py lines 84-98]

L'appartenance aux domaines limite toujours ce que le propriétaire peut modifier. Créer une branche
change ce qu'un membre peut faire ; cela ne change jamais à quels domaines il peut le faire
(REQ-1530).
[tool-verified: `domains_within()` at env_authority.py lines 121-145]

## Environnements protégés (REQ-1504)

Un environnement peut être protégé. Une fusion ou un déploiement vers un environnement protégé
n'est pas appliqué au moment de la demande ; il est proposé, et quelqu'un d'autre que le demandeur
doit l'approuver.

`prod` est protégé automatiquement dès que l'organisation compte plus d'un membre. Une organisation
à membre unique ne peut pas satisfaire « quelqu'un d'autre que le demandeur » : la règle n'y est
donc pas appliquée — elle rendrait `prod` impossible à fusionner. Tout environnement peut être
marqué protégé par un org_admin.
[tool-verified: `is_protected()` at env_approvals.py lines 79-96 ; `protectedHelp2` UI string
in environmentsTab.json line 28]

Une demande de fusion est une ligne, pas une boîte de dialogue de confirmation. L'approbateur est
par définition une personne différente du demandeur et n'est pas présent au moment de la demande ;
une confirmation éphémère forcerait l'approbation à l'intérieur de la session du demandeur, soit le
seul agencement que l'exigence interdit. [tool-verified: env_approvals.py module docstring lines 11-17]

La ligne de la demande porte le rapport de fusion aux côtés du message du demandeur. L'obsolescence
est dérivée au moment de la lecture, jamais stockée : replanifier à la lecture et comparer au
rapport stocké est la seule version qui ne puisse pas se tromper. Une demande obsolète doit être
redemandée. Le demandeur ne peut pas approuver sa propre demande. [tool-verified: `STALE` constant and `effective_state()` at
env_approvals.py lines 53, 215-243 ; `decide()` lines 265-268]

États du cycle de vie d'une demande : `requested` → `approved`/`rejected` → `applied`. `stale` est
dérivé.
[tool-verified: `REQUESTED`, `APPROVED`, `REJECTED`, `APPLIED`, `STALE` at env_approvals.py
lines 47-53]

La même porte gère les déploiements depuis une référence de dépôt : la demande épingle le SHA au
moment de la proposition. Si la référence bouge entre la proposition et la décision, l'approbateur
lit le rapport du commit épinglé, non du nouveau. [tool-verified: `request_deploy()` at env_approvals.py lines
150-189 ; env_approvals.py docstring lines 26-27]

! ! ! note
    L'interface des demandes de fusion se trouve sous l'onglet **Demandes de fusion** du panneau
    Environnements. La colonne **Rapport** montre ce qui changerait, en nombre ; la ligne se
    déplie pour montrer le détail objet par objet. [tool-verified: `environmentsTab.json` keys `requestsTitle`, `colReport`,
    `approve`, `reject`]

## Les commandes CLI `env`

`provisa env deploy` envoie le modèle situé à une référence dans un environnement. Elle sort avec 0
lorsque le déploiement a été appliqué ou n'était qu'une simulation, et avec 2 lorsque
l'environnement est protégé et que le déploiement n'a été que proposé — un pipeline qui traiterait
une approbation en attente comme un déploiement livré aurait tort, et le code de sortie le dit.
[tool-verified: `_cmd_env_deploy()` at cli.py lines 389-411]

```
provisa env deploy --org acme --env prod --ref main --token <token> --api <url>
```

`provisa env fetch` amène les branches distantes de l'organisation dans le dépôt local. Un
déploiement peut alors nommer `origin/<branch>`. [tool-verified: `_cmd_env_fetch()` at cli.py lines 414-426]

```
provisa env fetch --org acme --api <url> --token <token>
```

Les deux commandes acceptent `--api` (l'URL de l'API Provisa) et `--token` (un jeton bearer).
Définissez `PROVISA_API_URL` et `PROVISA_API_TOKEN` dans l'environnement pour éviter de les passer
à chaque appel. [inferred: shared `_api_call()` helper]

Le pipeline de CI type pour un flux adossé à un dépôt :

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
provisa env deploy --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## Voir aussi

- [Déploiement](deployment.md) — comment monter le plan de contrôle auquel les environnements se connectent
- [Commandes](commands.md) — fonctions et webhooks suivis qui apparaissent dans l'arbre de chaque environnement
