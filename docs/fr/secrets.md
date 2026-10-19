# Secrets

**Les noms entrent. Les valeurs ne ressortent jamais.**

Aucun point de terminaison de l'API ne renvoie la valeur d'un secret stocké, et aucune interface ne propose de bouton « afficher ». Qui a perdu une valeur la remplace, avec le même appel et le même formulaire que ceux qui l'ont créée. Ce n'est pas un choix de politique : le chemin de lecture n'existe tout simplement pas dans le code.

---

## Syntaxe des références {: #reference-syntax }

Trois formes de référence sont valides partout où Provisa résout des identifiants :

| Forme | Résolue depuis | Qui peut l'utiliser |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | L'environnement du processus serveur | Configuration de déploiement uniquement |
| `${secret:NAME}` | Le coffre-fort de l'organisation — partagé par tous les membres | Tout champ acceptant une référence d'identifiant |
| `${user:NAME}` | Le coffre-fort personnel de la personne qui agit | Tout champ acceptant une référence d'identifiant |

Sur un déploiement multi-locataire, gardez les identifiants de la plateforme dans un service de secrets et hors des variables d'environnement. Un administrateur de locataire peut écrire `${env:...}` dans l'un de ses champs d'identifiant, donc toute valeur de l'environnement du processus est à la portée de chaque locataire. Ne laissez dans l'environnement que l'identifiant propre au backend de secrets.

La résolution est à sécurité intégrée (fail-closed) de bout en bout. Un nom de fournisseur inconnu, un nom non défini et un backend injoignable lèvent tous une erreur. Une référence qui n'a pas pu être résolue n'est jamais remplacée silencieusement par une chaîne vide.

### Format des noms {: #name-format }

Les noms de secrets doivent correspondre à `[A-Za-z_][A-Za-z0-9_]*` : lettres, chiffres et tirets bas, en commençant par une lettre ou un tiret bas. La raison est pratique. `${secret:NAME}` est analysé par la grammaire des références, qui lit jusqu'à l'accolade fermante `}` ; un nom contenant une accolade, une espace ou deux-points produirait donc une référence analysée comme autre chose.

---

## Deux coffres-forts, un seul service {: #two-vaults-one-service }

Chaque organisation possède deux coffres-forts. Tous deux vivent à l'intérieur du même service de secrets.

**Coffre-fort de l'organisation.** L'identifiant qu'un administrateur d'organisation y range est partagé : chaque membre qui référence `${secret:DATABASE_TOKEN}` obtient la même valeur. Utilisez-le pour les identifiants que l'*organisation* possède, comme un mot de passe de base de données partagé, une clé de compte de service ou un jeton de déploiement. Lire ou écrire dans le coffre-fort de l'organisation exige la capacité `org_settings`.

**Coffre-fort personnel.** Un identifiant stocké ici appartient à une seule personne. Quand deux personnes détiennent chacune un `GIT_TOKEN`, `${user:GIT_TOKEN}` se résout en celui de la personne qui agit ; le même texte de référence donne donc à chacun son propre identifiant. Qui n'a rien stocké reçoit une erreur, jamais la valeur de quelqu'un d'autre. Aucune capacité ne protège le coffre-fort personnel, car détenir son propre identifiant n'est pas un privilège accordé par un administrateur, et aucune syntaxe de requête ne nomme le coffre-fort d'une autre personne.

La portée fait partie de la référence elle-même, et aucune permission ne l'entoure. `${secret:NAME}` et `${user:NAME}` ne répondent jamais l'un pour l'autre.

---

## Choisir un service de secrets {: #choosing-a-secrets-service }

**Admin → Sécurité → Service de secrets.** Toute personne détenant la capacité `platform_settings` voit le panneau. Il liste chaque backend que connaît la version, que son SDK soit installé ou non, et une ligne grisée nomme le paquet Python manquant au lieu de masquer l'option.

Cinq backends sont livrés :

| Clé | Libellé | Nécessite |
| ----- | ------- | ------- |
| `provisa` | Provisa (intégré, chiffré) | Rien ; c'est la valeur par défaut |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault (secrets) | `azure-keyvault-secrets` |

La sélection échoue de façon fermée : un backend inconnu ou indisponible arrête le démarrage avec une erreur, et Provisa ne se rabat jamais en silence sur un autre.

### L'identifiant propre au backend {: #the-backends-own-credential }

L'identifiant de connexion d'un backend central relève de la configuration du processus, et il vient de `${env:...}` seulement. `${secret:...}` ne peut pas le fournir, car un service de secrets dont l'identifiant vit en lui-même ne pourrait jamais être ouvert. La chaîne de confiance se termine dans l'environnement de l'hôte, par conception. Provisa résout chaque valeur de configuration d'un backend depuis l'environnement avant de construire le backend.

Par exemple, une configuration Vault dans `provisa.yaml` :

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### Service centralisé ou magasin intégré {: #central-service-vs-built-in }

Quand un service central est configuré, Provisa y lit et n'y écrit jamais. Créer et supprimer des entrées revient aux outils propres du service ; la page Secrets le dit et ne propose pas de bouton de création.

Avec le backend intégré `provisa` actif, la page Secrets est entièrement modifiable. Vous créez, remplacez et supprimez des secrets depuis l'interface ou via l'API.

---

### Les secrets d'une organisation dans un service central {: #one-organizations-secrets-in-a-central-service }

Un déploiement qui héberge de nombreuses organisations et lit un service central le lit, pour chaque organisation, à l'intérieur du préfixe propre à cette organisation. Une référence reste le nom court (`${secret:db#password}`) ; le préfixe est ajouté à la lecture.

| Service | Où est conservé le secret `db` de l'organisation `acme` |
|---|---|
| HashiCorp Vault (KV v2) | `orgs/acme/db` |
| AWS Secrets Manager | `orgs/acme/db` |
| GCP Secret Manager | `org-acme--db` |
| Azure Key Vault | `o4-acme-db` (`o`, la longueur de l'identifiant de l'organisation, l'identifiant avec `_` écrit `-`, puis le nom) |

Une référence est refusée, nommément, lorsque :

- son nom sortirait du préfixe : il commence par `/`, contient un segment `..` ou une barre oblique inverse, ou contient `/` dans GCP ou Azure, dont les noms sont plats ;
- son nom est trop long pour le service une fois préfixé (GCP 255 caractères, Azure 127) ; c'est refusé à l'enregistrement de la référence, avec la limite indiquée ;
- aucune organisation n'est liée à l'opération qui la lit.

Un déploiement à une seule organisation lit une référence exactement telle qu'elle est écrite, sans préfixe : le service auquel il est relié est celui de cette organisation, et rien n'est à déplacer.

**Passer au préfixe.** Sur un déploiement de nombreuses organisations, chaque secret auquel une organisation fait référence doit se trouver sous le préfixe de cette organisation. Copiez chacun à cet endroit avec l'outillage propre du service, puis raccourcissez la référence au nom à l'intérieur du préfixe (`${secret:teams/data/db}`, conservé sous `orgs/acme/db`, devient `${secret:db}`). Une référence dont le secret n'a pas été déplacé échoue comme introuvable ; elle n'est pas lue depuis l'endroit où il se trouvait.

## Le magasin intégré de Provisa {: #provisas-built-in-store }

C'est le comportement par défaut quand aucun service central n'est configuré. Chaque ligne de `secrets_store` contient un blob d'enveloppe chiffré. La colonne `value` est binaire, et la clé de déchiffrement vit dans l'environnement du processus, hors de la base de données. Une copie du plan de contrôle sans la clé maîtresse du déploiement ne contient que du texte chiffré.

Le chiffrement n'est jamais facultatif. Sans clé de chiffrement configurée pour tout le processus, le magasin utilise un trousseau local. Si l'hôte n'a pas de trousseau pour conserver une clé, le magasin refuse d'écrire, et la valeur n'est jamais stockée en clair.

**Forme de stockage** :

| Colonne | Type | Rôle |
| -------- | ------ | --------- |
| `org_id` | Text | L'organisation propriétaire de ce secret |
| `owner_id` | Text | `"*"` pour le coffre-fort de l'organisation ; identifiant d'utilisateur pour le coffre-fort personnel |
| `name` | Text | Le nom de référence |
| `value` | LargeBinary | Blob d'enveloppe chiffrée |
| `description` | Text | À quoi sert le secret — jamais déduit de la valeur |
| `updated_by` | Text | Qui l'a défini en dernier |

Une organisation qui a créé sa propre clé de chiffrement a ses secrets enveloppés sous cette clé ; sinon, ils le sont sous celle du déploiement. Voir [Chiffrement au repos](security.md#encryption-at-rest-and-the-organizations-own-keys).

La colonne `value` n'est sélectionnée par aucune requête de listage.

---

## Endpoints d'API {: #api-endpoints }

Toutes les routes se trouvent sous `/admin/orgs/{org_id}`. Le coffre-fort de l'organisation exige `org_settings` dans cette organisation. Le coffre-fort personnel n'exige aucune capacité : le propriétaire est lu dans l'identité authentifiée, et aucun paramètre de requête ne nomme le coffre-fort de quelqu'un d'autre.

| Méthode | Chemin | Ce qu'elle fait |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | Lister les noms et références du coffre-fort de l'organisation |
| `PUT` | `/secrets/{name}` | Créer ou remplacer un secret de l'organisation |
| `DELETE` | `/secrets/{name}` | Supprimer un secret de l'organisation |
| `GET` | `/my-secrets` | Lister les noms et références personnels de l'appelant |
| `PUT` | `/my-secrets/{name}` | Créer ou remplacer un des secrets de l'appelant |
| `DELETE` | `/my-secrets/{name}` | Supprimer un des secrets de l'appelant |

Chaque réponse renvoie des métadonnées, jamais la valeur : le nom, la description, `updated_at`, `updated_by` et la chaîne `reference` à coller. Le corps du `PUT` porte `value` (obligatoire) et `description` (facultatif). Remplacer est le même appel que créer, car le nom est l'identité et il n'existe pas d'ID distinct.

Chaque écriture est consignée dans le journal d'audit. L'entrée nomme l'auteur et le secret. Ni la valeur ni sa longueur ne sont consignées.

---

## Où `${secret:NAME}` se résout {: #where-secretname-resolves }

La résolution a lieu à l'intérieur d'une opération liée. Rien n'est résolu à l'import ni au démarrage. Le magasin lit et déchiffre les secrets de l'organisation une fois, au début de l'opération, et garde la table pendant toute sa durée. Hors d'une opération liée, `${secret:NAME}` lève une erreur.

Deux sites d'appel établissent cette liaison :

**Opérations Git distantes.** L'URL distante du dépôt d'une organisation peut contenir une référence `${secret:...}` ou `${user:...}`, comme un jeton de push intégré à l'URL. Le routeur des environnements lie alors le coffre-fort de l'organisation et le coffre-fort personnel de l'utilisateur qui agit autour de l'appel git. Avec la forme `${user:GIT_TOKEN}`, un commit est enregistré sous l'identifiant de la personne qui a poussé. Aucun compte de service partagé n'intervient.

**Lecture des clés d'API des fournisseurs d'IA.** Quand Provisa lit la clé du fournisseur de LLM d'une organisation et que cette clé est stockée comme référence `${secret:NAME}`, il lie le coffre-fort de l'organisation pour cette requête. La référence est résolue à la sortie, et le texte de la référence n'atteint jamais le fournisseur.

---

## Clés de fournisseur d'IA d'une organisation comme références de secret {: #org-ai-vendor-keys-as-secret-references }

La clé de fournisseur d'IA d'une organisation (Anthropic, OpenAI et d'autres) peut être stockée sous forme de référence `${secret:NAME}` plutôt que de clé littérale.

Rangez d'abord la clé dans le coffre-fort de l'organisation :

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

Puis réglez la configuration d'IA de l'organisation pour qu'elle la référence :

```
vendor key field → ${secret:OPENAI_KEY}
```

La référence est stockée chiffrée dans `org_secrets`. Au moment de la requête, Provisa résout `${secret:OPENAI_KEY}` dans le coffre-fort de l'organisation et remet la clé littérale au SDK du fournisseur. La rotation de l'entrée du coffre-fort prend effet immédiatement, sans changement de configuration côté paramètres de l'organisation.

---

## Les identifiants propres des magasins du déploiement {: #the-deployments-own-store-credentials }

Les mots de passe des magasins que Provisa exécute (PostgreSQL, Redis, MinIO, le compte administrateur de Grafana et le secret partagé du cluster Trino) ne sont des secrets dans aucun des deux coffres. Ils sont générés par déploiement et conservés par le déploiement : dans `~/.provisa/store-credentials.env` (mode 600), dans l'état Terraform ou dans le Secret Kubernetes que désigne `credentials.existingSecret` du chart Helm. Voir [Durcissement du déploiement](security.md#deployment-hardening).

---

## Accès de l'administrateur de la plateforme {: #platform-admin-access }

Un administrateur de plateforme qui exploite le plan de contrôle ne peut lire les valeurs secrètes d'aucune organisation. La garde `org_settings` refuse d'emblée `cross_org` et le contournement de plateforme, car administrer le cycle de vie d'une organisation ne donne aucun accès en lecture aux identifiants qu'elle conserve. Le serveur l'impose lui-même, quoi qu'affiche l'interface.

---

## Voir aussi {: #see-also }

- [Modèle de sécurité](security.md) — contrôle d'accès en couches, authentification et journalisation d'audit
- [Référence de configuration](configuration.md) — syntaxe `${env:VAR}` pour les identifiants au niveau du processus
