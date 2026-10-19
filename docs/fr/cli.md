# Référence CLI {: #cli-reference }

La commande `provisa` est le point d'entrée unique du niveau embarqué installé par pip. Elle démarre le runtime, gère les licences, déclenche la publication des métadonnées, déploie les modèles et pilote la bannière de maintenance. Vous n'avez besoin ni de Docker, ni de Node, ni de services externes.

Installez-la avec l'extra `embedded`, qui apporte également les extensions DuckDB hors ligne
et le plan de contrôle PostgreSQL embarqué :

```bash
pip install 'provisa[embedded]'
```

**Exigences de plateforme.** `provisa run` requiert Python 3.12 et une plateforme disposant d'une roue (wheel)
pgserver : linux x86_64, macOS ou Windows x86_64. Linux aarch64 n'a ni roue pgserver ni distribution
source, donc le palier embarqué ne s'y exécute pas. Utilisez le palier conteneur sur aarch64.

---

## Options partagées {: #shared-options }

Plusieurs sous-commandes appellent l'API HTTP de Provisa. Elles partagent trois indicateurs et deux variables
d'environnement.

| Indicateur | Défaut | Repli sur variable d'environnement |
| --- | --- | --- |
| `--api <url>` | `http://127.0.0.1:8000` | `PROVISA_API_URL` |
| `--token <token>` | _(aucun)_ | `PROVISA_API_TOKEN` |
| `--timeout <seconds>` | 300 (30 pour `maintenance`) | _(aucune)_ |

`--api` est l'URL de base d'une instance Provisa en cours d'exécution. En multi-location, l'hôte nomme l'organisation : `https://acme.provisa.org` mène au locataire acme. `--token` est un jeton Bearer. Quand il est vide, aucun en-tête `Authorization` n'est envoyé, ce qu'attend un déploiement sans authentification.

Définissez les deux variables dans votre environnement CI pour éviter de les répéter à chaque appel :

```bash
export PROVISA_API_URL=https://acme.provisa.org
export PROVISA_API_TOKEN=<token>
```

Sous-commandes acceptant ces indicateurs : `metadata export`, chaque sous-commande `env` (`list`, `show`,
`create`, `delete`, `diff`, `merge`, `deploy`, `revert`, `redo`, `fetch`, `push`, `pull`),
`maintenance on`, `maintenance off`, `maintenance status`.

---

## provisa run

Démarre le système Provisa embarqué dans un seul processus : le serveur d'API et le serveur des fichiers statiques et du proxy de l'interface. Ni Docker, ni Node, ni services externes n'interviennent.

```
provisa run [--demo] [--host HOST] [--api-port PORT] [--ui-port PORT]
            [--no-browser] [--reset] [--data-dir DIR]
```

### Indicateurs {: #flags }

| Indicateur | Défaut | Remarques |
| --- | --- | --- |
| `--demo` | désactivé | Charge la démo intégrée — domaines d'exemple pet-store et shelter sur SQLite embarqué |
| `--host` | `127.0.0.1` | Adresse de liaison pour les deux serveurs |
| `--api-port` | `8000` | Port du serveur API |
| `--ui-port` | `3000` | Port du serveur statique/proxy de l'interface |
| `--no-browser` | désactivé | Ne pas ouvrir de navigateur quand l'interface est prête ; affiche tout de même l'URL |
| `--reset` | désactivé | Supprime et reconstruit le magasin du plan de contrôle embarqué avant le démarrage ; à utiliser après une mise à niveau de Provisa si le démarrage signale une incohérence de schéma |
| `--data-dir` | `~/.provisa/native` | Répertoire contenant le cluster PostgreSQL embarqué et le cache d'extensions DuckDB |

### Variables d'environnement {: #environment-variables }

`provisa run` lit plusieurs variables supplémentaires avant que les serveurs HTTP ne démarrent.
Définissez-les pour surcharger les défauts que `load_profile("native", ...)` appliquerait sinon.

| Variable | Effet |
| --- | --- |
| `TRINO_HOST` / `TRINO_PORT` | Remplace le moteur DuckDB embarqué par un coordinateur Trino fourni par le client |
| `PROVISA_ENGINE_URL` | Autre façon de pointer vers un moteur de fédération externe |
| `PROVISA_CONFIG` | Fichier de configuration à charger ; `--demo` le règle sur la configuration de démo intégrée |
| `PROVISA_DEMO` | Réglé à `1` par `--demo` ; marque la session comme une exécution de démo |
| `PROVISA_DEMO_DIR` | Chemin vers le répertoire de données d'exemple de la démo ; réglé par `--demo` |
| `PROVISA_DUCKDB_EXT_DIR` | Répertoire d'extensions DuckDB pré-déployé ; réglé automatiquement depuis le paquet `provisa-duckdb-ext` si présent ; son absence signifie que DuckDB télécharge depuis le réseau à la première utilisation |

### Séquence de démarrage {: #startup-sequence }

1. Vérification de plateforme — s'arrête avec un message clair sur un Python non pris en charge ou un pgserver manquant.
2. `--reset` (si demandé) — supprime le cluster PostgreSQL embarqué ; il se reconstruit à l'étape suivante.
3. Configuration de démo (si `--demo`) — règle `PROVISA_CONFIG` et `PROVISA_DEMO_DIR`.
4. Environnement embarqué — démarre le plan de contrôle PostgreSQL, résout son URL de socket, et
   déploie les extensions DuckDB hors ligne si `provisa-duckdb-ext` est installé.
5. Vérification de dérive de schéma — scrute le plan de contrôle en direct à la recherche de colonnes manquantes. Si des colonnes sont trouvées,
   affiche une invite `--reset` et sort avec le code 1. V1 n'a pas de migrations ; une colonne ajoutée dans une
   version plus récente nécessite une réinitialisation.
6. Les deux serveurs démarrent simultanément. L'annonceur de disponibilité sonde `GET /ready` (pas `/health` — le
   point d'entrée `/ready` confirme que le magasin est rattaché et que le moteur est chaud) et ouvre le navigateur
   dès qu'il retourne 200.

### Codes de sortie {: #exit-codes }

| Code | Signification |
| --- | --- |
| 0 | Arrêt propre (Ctrl-C) |
| 1 | Erreur de démarrage (échec de la vérification de plateforme, configuration de démo manquante, dérive de schéma détectée) |

### Exemple {: #example }

```bash
# Start with the demo data
provisa run --demo

# Start on non-default ports, no browser
provisa run --api-port 8080 --ui-port 4000 --no-browser

# Upgrade: reset the control plane first, then start
provisa run --reset

# Point at an external Trino cluster instead of the embedded DuckDB engine
TRINO_HOST=trino.internal TRINO_PORT=8080 provisa run
```

---

## provisa license apply

Vérifie et installe un fichier de licence hors ligne. Le fichier est le `license.json` émis par
provisa.dev.

```
provisa license apply <file>
```

| Argument | Remarques |
| --- | --- |
| `file` | Chemin du fichier de licence ; l'expansion `~` est appliquée |

Le code de sortie 0 signifie que la licence est valide et installée. Le code de sortie 1 signifie qu'elle a été rejetée ; la
raison est affichée sur stderr.

```bash
provisa license apply ~/Downloads/license.json
```

---

## provisa license status

Affiche l'ID de la machine, l'état d'essai, les jours écoulés, et la validité de la licence.

```
provisa license status
```

Aucun flag. Affiche cinq lignes (ID de machine, date de première détection, jours écoulés, état de l'essai et état de la licence) et se termine avec 0.

```
Machine ID:   a1b2c3d4e5f6...
First seen:   2026-07-01
Elapsed:      83.0 days
Trial:        active
Licensed:     no (no license installed)
```

---

## provisa metadata export

Déclenche la publication des métadonnées à la demande du serveur en cours d'exécution. La commande envoie à `POST /admin/metadata-export/publish`, le point de terminaison qu'appelle le bouton **Publish now** de l'onglet Admin ; les deux chemins envoient donc le même instantané complet.

```
provisa metadata export [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Indicateur | Défaut | Remarques |
| --- | --- | --- |
| `--api` | `$PROVISA_API_URL`, puis `http://127.0.0.1:8000` | En multilocation, le nom d'hôte nomme l'organisation |
| `--token` | `$PROVISA_API_TOKEN` | Jeton Bearer pour une identité détenant `org_settings` ; à omettre sur les déploiements non authentifiés |
| `--timeout` | `300` | Secondes avant l'abandon de l'appel HTTP |

| Code de sortie | Signification |
| --- | --- |
| 0 | Chaque actif publié |
| 1 | Publication partielle ou échec de connexion ; les erreurs par actif s'affichent sur stderr |

```bash
provisa metadata export \
  --api  https://acme.provisa.org \
  --token "$PROVISA_API_TOKEN"

# Cron example — daily at 06:00
# 0 6 * * *  provisa metadata export --api https://acme.provisa.org >> /var/log/provisa-export.log 2>&1
```

[Export de métadonnées](metadata-export.md#from-the-command-line) donne la référence de configuration complète : fournisseurs, identifiants, `reconcile_cron` et contenu de l'instantané.

---

## provisa env

Exécute le cycle de vie des environnements contre un Provisa en fonctionnement. Chaque sous-commande est un appel
à l'API d'administration qu'appelle l'interface ; la CLI ne décide de rien. La liste de ce que porte une copie, les règles de
fusion et de suppression, la liaison, la protection et les droits de l'appelant appartiennent au serveur, elles valent donc
quelle que soit la manière dont l'opération est invoquée.

```
provisa env {list,show,create,delete,diff,merge,deploy,revert,redo,fetch,push,pull} --org ORG ...
```
### Options acceptées par chaque sous-commande {: #flags-every-subcommand-takes }

| Option | Obligatoire | Notes |
| --- | --- | --- |
| `--org` | oui | Organisation qui détient l'environnement |
| `--json` | non | Affiche la réponse de l'API en JSON, pour un script |
| `--api` | non | Voir [Options communes](#shared-options) |
| `--token` | non | Jeton bearer d'une identité autorisée à effectuer l'opération |
| `--timeout` | non | Secondes ; 300 par défaut |

Les sous-commandes qui agissent sur un environnement acceptent aussi `--env ENV` (obligatoire). `list`, `diff`,
`merge` et `fetch` non ; `diff` et `merge` nomment leurs environnements avec `--from` et `--into`.

### Codes de sortie {: #exit-codes_1 }

| Code | Signification |
| --- | --- |
| 0 | Terminé, ou un essai à blanc qui n'a rien appliqué |
| 1 | Refusé. Le code de la règle qui refuse s'affiche sur stderr sous la forme `refused (<code>): <detail>` ; avec `--json` c'est `{"refused": {...}}` sur stdout |
| 2 | Proposé, non appliqué : l'environnement cible est protégé et le changement attend une approbation |

Le code de sortie 2 est voulu. Un pipeline qui traiterait une approbation en attente comme un déploiement publié
se tromperait.

### provisa env list

Liste les environnements de l'organisation et le build où se trouve chacun. Aucune option au-delà des options communes.

```
provisa env list --org ORG [--json]
```
### provisa env show

Affiche un environnement et son build actuel. Sort avec 1 et `environments.unknown` lorsque
l'environnement n'existe pas.

```
provisa env show --org ORG --env ENV [--json]
```
### provisa env create

Crée un environnement contenant le modèle d'un autre, avec ses données choisies à la création. L'API
décide quels choix l'appelant peut faire.

```
provisa env create --org ORG --env NAME --data-mode MODE
                   [--from SOURCE] [--mutation-handling HANDLING] [--expires-at ISO8601]
```
| Option | Obligatoire | Notes |
| --- | --- | --- |
| `--env` | oui | Nom du nouvel environnement |
| `--from` | non | Environnement de départ ; `prod` par défaut |
| `--data-mode` | oui | `inherit`, `unbound`, `test_fake` ou `test_synthetic` : les connexions de la source par référence, aucune jusqu'à ce qu'il lie les siennes, ou des données de test |
| `--mutation-handling` | non | `refused`, `reversible` ou `direct` ; `refused` par défaut |
| `--expires-at` | non | Instant ISO-8601 après lequel l'environnement est supprimé |

```bash
provisa env create --org acme --env feature-x --from prod --data-mode test_fake
```
### provisa env delete

Retire un environnement. Ses branches ne partent que si elles sont nommées.

```
provisa env delete --org ORG --env ENV [--delete-branch] [--delete-remote-branch]
```
| Option | Notes |
| --- | --- |
| `--delete-branch` | Supprime aussi sa branche |
| `--delete-remote-branch` | Supprime aussi sa branche sur le distant |

### provisa env diff

Indique ce que ferait la fusion d'un environnement dans un autre. C'est un GET de `merge-preview`, il ne peut donc
pas écrire ; confiez-le à un runner qui ne doit rien changer.

```
provisa env diff --org ORG --from SOURCE --into TARGET [--removals] [--select PATH ...]
```
| Option | Obligatoire | Notes |
| --- | --- | --- |
| `--from` | oui | Environnement depuis lequel on fusionne |
| `--into` | oui | Environnement dans lequel on fusionne |
| `--removals` | non | Retire aussi de la cible ce que la source n'a plus |
| `--select PATH` | non | Limite la fusion à cet objet, par son chemin dans le modèle (`metrics/revenue.yaml`) ; à répéter pour d'autres. Ce qu'il requiert et qui manque à la cible est emporté avec lui et listé |

### provisa env merge

Fusionne un environnement dans un autre, par identité.

```
provisa env merge --org ORG --from SOURCE --into TARGET
                  [--removals] [--select PATH ...] [--dry-run] [--message MSG]
                  [--retire-source] [--retire-remote]
```
| Option | Obligatoire | Notes |
| --- | --- | --- |
| `--from` | oui | Environnement depuis lequel on fusionne |
| `--into` | oui | Environnement dans lequel on fusionne |
| `--removals` | non | Retire aussi de la cible ce que la source n'a plus |
| `--select PATH` | non | Limite la fusion à cet objet, par son chemin dans le modèle (`metrics/revenue.yaml`) ; à répéter pour d'autres. Ce qu'il requiert et qui manque à la cible est emporté avec lui et listé |
| `--dry-run` | non | Affiche le rapport de fusion sans rien appliquer |
| `--message` | non | Ce que fait la fusion ; obligatoire pour appliquer |
| `--retire-source` | non | Supprime la source une fois son travail arrivé |
| `--retire-remote` | non | Avec `--retire-source`, supprime aussi la branche de la source sur le distant |

### provisa env deploy

Déploie le modèle d'une ref git dans un environnement, faisant de cet arbre le modèle actuel de l'environnement
. C'est la commande qu'exécute un pipeline de déploiement ; un déploiement est toujours une
invocation portant une identité contre un plan de contrôle nommé.

```
provisa env deploy --org ORG --env ENV --ref REF [--dry-run] [--message MSG]
```
| Option | Obligatoire | Notes |
| --- | --- | --- |
| `--env` | oui | Environnement qui contiendra le modèle déployé |
| `--ref` | oui | Branche ou commit du dépôt de l'organisation |
| `--dry-run` | non | Indique ce que ferait le déploiement ; n'applique rien |
| `--message` | non | Note reportée sur une demande d'approbation lorsque la cible est protégée |

Un chargement dans un environnement existant n'applique jamais de rôles. `--seed` n'existe plus : l'API refuse un amorçage
avec `environments.seed_only_at_creation`.

```bash
# Fetch remote branches, then deploy
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"

provisa env deploy \
  --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```
### provisa env revert

Ramène un environnement à un build précédent. Sans `--to`, il recule d'un commit
dans l'historique propre de l'environnement. Avec `--to`, c'est le déploiement ordinaire de ce build.

```
provisa env revert --org ORG --env ENV [--to BUILD] [--message MSG]
```
| Option | Obligatoire | Notes |
| --- | --- | --- |
| `--env` | oui | Environnement à ramener |
| `--to` | non | Build à déployer ; par défaut celui qui précède le build actuel |
| `--message` | non | Note reportée sur une demande d'approbation |

### provisa env redo

Avance de nouveau vers le build dont un retour en arrière est parti.

```
provisa env redo --org ORG --env ENV
```
### provisa env fetch

Récupère les branches distantes de l'organisation dans son dépôt Provisa. Exécutez-la avant un déploiement
lorsque vous voulez nommer `origin/<branch>`. Provisa ne l'exécute jamais sur minuterie.

```
provisa env fetch --org ORG
```
Affiche une ligne par branche récupérée, `origin/<name>  <sha12>`.

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# origin/main  a1b2c3d4e5f6
# origin/dev   9f8e7d6c5b4a
```
### provisa env push

Envoie la branche d'un environnement vers le distant de l'organisation. Affiche `nothing to send` lorsqu'il
n'y a rien à envoyer.

```
provisa env push --org ORG --env ENV
```
### provisa env pull

Applique ce que le distant détient pour un environnement. Affiche
`<env> already holds what the remote does` lorsqu'il n'y a rien à appliquer. Le code de sortie 2 s'applique comme
pour deploy.

```
provisa env pull --org ORG --env ENV
```
Pour l'explication complète des classes d'environnement, des règles de protection, des rapports de fusion et du
cycle de vie des approbations, voir [Environnements](environments.md#the-env-cli-commands).

---

## provisa maintenance on

Affiche la bannière de maintenance planifiée sur le déploiement. Lancez-la avant tout travail planifié qui arrête le plan de données. Changer `var.engine_cluster_mode` en est un exemple, car cela remplace le cluster du moteur et chaque shard qu'il porte.

```
provisa maintenance on [--message MSG] [--ends-at ISO8601]
                       [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Indicateur | Remarques |
| --- | --- |
| `--message` | Surcharge le libellé standard du déploiement ; le défaut est le message standard du serveur |
| `--ends-at` | Instant ISO-8601 auquel le travail devrait se terminer, par ex. `2026-08-14T22:30:00Z` ; le défaut est l'absence d'estimation |
| `--api` | Voir [Options partagées](#shared-options) |
| `--token` | Jeton Bearer pour une identité détenant `platform_settings` |
| `--timeout` | Défaut 30 s |

Affiche l'état de bannière résultant et sort avec le code 0. Lève `SystemExit` en cas d'échec HTTP ou de connexion.

```bash
provisa maintenance on \
  --message "Engine cluster rolling upgrade" \
  --ends-at "2026-09-22T03:00:00Z" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## provisa maintenance off

Efface la bannière de maintenance une fois le travail terminé.

```
provisa maintenance off [--api URL] [--token TOKEN] [--timeout SECONDS]
```

Affiche l'état de bannière résultant (active : false) et sort avec le code 0.

```bash
provisa maintenance off --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# Maintenance notice: OFF
```

---

## provisa maintenance status

Affiche l'état actuel de la bannière de maintenance sans le modifier.

```
provisa maintenance status [--api URL] [--token TOKEN] [--timeout SECONDS]
```

Affiche l'état de la bannière et sort avec le code 0.

```
Maintenance notice: ON
  Message:  Engine cluster rolling upgrade
  Since:    2026-09-22T01:15:00Z
  Ends at:  2026-09-22T03:00:00Z
```

---

## Référence rapide {: #quick-reference }

| Commande | Ce qu'elle fait |
|---|---|
| `provisa run` | Démarre l'API + l'interface embarquées |
| `provisa run --demo` | Démarre avec les données d'exemple pet-store / shelter |
| `provisa run --reset` | Reconstruit le plan de contrôle avant de démarrer |
| `provisa license apply <file>` | Installe un fichier de licence hors ligne |
| `provisa license status` | Affiche l'ID machine et l'état d'essai / de licence |
| `provisa metadata export` | Publie l'instantané de métadonnées à la demande |
| `provisa env list --org ORG` | Liste les environnements de l'organisation et leurs builds |
| `provisa env show --org ORG --env ENV` | Affiche un environnement |
| `provisa env create --org ORG --env ENV --data-mode MODE` | Crée un environnement à partir d'un autre |
| `provisa env delete --org ORG --env ENV` | Retire un environnement |
| `provisa env diff --org ORG --from A --into B` | Aperçu d'une fusion (lecture seule) |
| `provisa env merge --org ORG --from A --into B` | Fusionne un environnement dans un autre |
| `provisa env deploy --org ORG --env ENV --ref REF` | Déploie une ref dans un environnement |
| `provisa env revert --org ORG --env ENV [--to BUILD]` | Recule d'un commit, ou déploie un build |
| `provisa env redo --org ORG --env ENV` | Avance de nouveau |
| `provisa env fetch --org ORG` | Récupère les branches distantes dans le dépôt Provisa |
| `provisa env push --org ORG --env ENV` | Envoie la branche d'un environnement au distant |
| `provisa env pull --org ORG --env ENV` | Applique ce que détient le distant |
| `provisa maintenance on` | Lève la bannière de maintenance |
| `provisa maintenance off` | Efface la bannière de maintenance |
| `provisa maintenance status` | Affiche l'état actuel de la bannière |

## Voir aussi {: #see-also }

- [Environnements](environments.md) — modèle d'environnement, environnements protégés, cycle de vie d'approbation de déploiement
- [Export de métadonnées](metadata-export.md) — fournisseurs de catalogue, configuration, et contenu de l'instantané
- [Déploiement](deployment.md) — palier conteneur et déploiement cloud
