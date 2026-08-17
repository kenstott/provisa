# Importer depuis Hasura

Provisa peut convertir des métadonnées Hasura existantes en un `config.yaml` Provisa, en préservant les tables suivies, les relations, les autorisations et les schémas distants.

## Import interactif (Admin → Import Hasura Config)

La surface d'administration exécute les mêmes convertisseurs : un import ne nécessite ni accès shell ni allers-retours de fichier de configuration. Requiert la capacité `org_settings` ; l'import atterrit dans l'organisation dans laquelle agit la session.

1. **Téléverser.** Choisissez un répertoire de métadonnées Hasura v2 zippé, un projet DDN zippé, un export de métadonnées consolidé (`.yaml`/`.json`, y compris l'enveloppe `{resource_version, metadata}` que retourne l'API de métadonnées), ou un unique fichier `.hml`. Laissez le format sur *Detect automatically* sauf si le téléversement est ambigu.
2. **Associer les domaines** (optionnel). Chaque paire associe un schéma v2 ou un sous-graphe DDN à un domaine Provisa ; tout ce qui n'est pas associé conserve son nom d'origine.
3. **Convertir et prévisualiser.** Le serveur convertit et retourne des comptages, les avertissements du convertisseur, et la configuration générée. Rien n'est écrit à cette étape.
4. **Réviser et modifier.** La configuration est modifiable sur place — détails de connexion, noms de domaines, noms de rôles. Ce que vous appliquez est ce qui est affiché.
5. **Appliquer.** *Replace the existing semantic layer* supprime chaque source, table, rôle et règle absent de la configuration ; laissé désactivé, l'import fusionne avec ce que possède déjà l'organisation. L'application charge la configuration et reconstruit les schémas de l'organisation.

Endpoints : `POST /admin/import/hasura/preview` et `POST /admin/import/hasura/apply`.

---

## Hasura v2

### Exporter les métadonnées

Depuis votre console ou CLI Hasura :

```bash
hasura metadata export --output metadata.yaml
```

Ou utilisez l'API Hasura :

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

### Convertir

Le convertisseur v2 lit un **répertoire** de métadonnées Hasura (la structure produite par `hasura metadata export`, ou la structure plate `tables.yaml` / `actions.yaml`) et écrit une configuration Provisa :

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Omettez `-o` pour écrire la configuration sur stdout.

Options :

| Option | Rôle |
| ------ | --------- |
| `-o`, `--output` | Chemin YAML de sortie (par défaut : stdout) |
| `--source-overrides` | Fichier YAML avec des surcharges de connexion par source (hôte, port, identifiants) |
| `--domain-map` | Correspondances schéma-vers-domaine sous forme de paires `SCHEMA=DOMAIN` |
| `--auth-env-file` | Fichier `.env` avec la configuration d'authentification ; convertit JWT/JWK, le secret admin, et la carte des revendications |
| `--dry-run` | Analyse et valide sans écrire de sortie |

### Ce qui est converti

| Concept Hasura | Équivalent Provisa |
| --------------- | ------------------- |
| Table suivie | `tables[]` avec `publish: true` |
| Relation objet | `relationships[]` avec `cardinality: many-to-one` |
| Relation tableau | `relationships[]` avec `cardinality: one-to-many` |
| Autorisation select | Visibilité de rôle + filtre RLS |
| Autorisation colonne | `visible_to` / `writable_by` |
| Autorisation insert/update/delete | `writable_by` de mutation + RLS |
| Schéma distant | Enregistrement de source `graphql_remote` |
| Champ calculé | Entrée `functions[]` avec `kind: query` |

### Limitations

- **Actions** : converties automatiquement — les actions à gestionnaire HTTP deviennent des mutations `webhooks[]` ; les actions avec un gestionnaire non-HTTP (base de données) deviennent un espace réservé `functions[]` et émettent un avertissement invitant à réviser le gestionnaire
- **Déclencheurs d'événements** : convertis en configuration `event_triggers` par table (opérations, URL de webhook, politique de nouvelle tentative) et émettent un avertissement signalant une fidélité limitée
- **Schémas distants** : convertis en entrées de source `graphql_remote`
- **Fonctions SQL personnalisées** : nécessitent une révision — les cas simples se convertissent en entrées `functions[]`, les cas complexes nécessitent un travail manuel
- **Déclencheurs cron** : convertis en entrées de configuration `scheduler`, en préservant l'expression cron et l'indicateur d'activation

---

## Hasura DDN (v3)

### Localiser le projet HML

Le convertisseur DDN lit directement le **répertoire** du projet DDN contenant les fichiers `.hml` — aucune étape de build de supergraphe n'est requise. Le premier composant de répertoire sous la racine du projet est pris comme nom de sous-graphe ; les fichiers sous `globals/` sont assignés au sous-graphe `globals`.

### Convertir

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

Omettez `-o` pour écrire la configuration sur stdout.

Options :

| Option | Rôle |
| ------ | --------- |
| `-o`, `--output` | Chemin YAML de sortie (par défaut : stdout) |
| `--source-overrides` | Fichier YAML avec des surcharges de connexion par source |
| `--domain-map` | Correspondances sous-graphe-vers-domaine sous forme de paires `SUBGRAPH=DOMAIN` |
| `--aggregates-output` | Chemin de sortie pour le fichier annexe des expressions d'agrégation (par défaut : `<output>-aggregates.yaml`) |
| `--dry-run` | Analyse et valide sans écrire de sortie |

Les métadonnées `AggregateExpression` sont préservées dans un fichier annexe `*-aggregates.yaml`.

### Ce qui est converti

| Concept DDN | Équivalent Provisa |
| ------------ | ------------------- |
| Modèle de sous-graphe | `tables[]` sous une source |
| Relation | `relationships[]` |
| Règle d'autorisation | Filtre RLS |
| Commande | Mutation webhook ou vue |
| Connecteur | Entrée de source avec détails de connexion |

### Limitations

- **Connecteurs Lambda** (fonctions TypeScript/Python) nécessitent une configuration manuelle de webhook
- **Plugins de cycle de vie** n'ont pas d'équivalent direct
- **Modes d'authentification DDN** se mappent aux fournisseurs d'authentification Provisa mais les chemins de revendication JWT peuvent nécessiter des ajustements

---

## Après l'import

1. Révisez le `config.yaml` généré — portez attention aux `warnings` du convertisseur
2. Vérifiez les identifiants de connexion (le convertisseur utilise des valeurs d'espace réservé)
3. Démarrez Provisa et confirmez que les tables apparaissent dans l'Explorer
4. Exécutez vos requêtes GraphQL existantes — le schéma est compatible avec les motifs courants
5. Soumettez les requêtes pour approbation via l'API Admin ou l'UI avant d'activer la gouvernance en production
