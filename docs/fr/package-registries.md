# Registres de paquets

Les sources Node et Python installent leur propre code et leurs dépendances. **Admin → Registres de paquets** indique d'où elles installent : les registres npm, les index PyPI et les hôtes Git de l'organisation. Un administrateur de l'organisation les saisit une fois, et chaque installation Node et Python de l'organisation les utilise. Le registre npm public et le PyPI public n'ont besoin d'aucune entrée. (REQ-1984, REQ-1993)

Chaque champ contient ce qui a été saisi : une valeur, ou une référence comme `${secret:npm_token}` ou `${env:NPM_TOKEN}`, résolue dans le coffre propre à l'organisation à chaque installation. Une référence qui ne se résout en rien arrête l'installation et nomme le registre et le champ. Provisa n'écrit jamais dans le coffre et n'invente jamais le nom d'un secret. Voir [Secrets](secrets.md).

---

## Registres npm {: #npm-registries }

| Champ | Contient |
|---|---|
| URL du registre | L'adresse du registre, par exemple `https://artifactory.example.com/artifactory/api/npm/npm/` |
| Portée | Une portée comme `@acme`, dont les paquets viennent de ce registre. Vide pour aucune |
| Jeton ou utilisateur:mot de passe | Un jeton d'accès, ou `user:password` |
| Miroir de chaque installation | Tout paquet sans portée propre est installé par ce registre. Un par organisation |
| Autorité de certification (PEM) | Voir [Une autorité de certification privée](#a-private-certificate-authority) |

Un jeton est écrit dans le `.npmrc` de l'installation comme `_authToken` et envoyé comme jeton bearer. `user:password`, par exemple un utilisateur Artifactory et sa clé d'API, est écrit comme `_auth` (base64) et envoyé en Basic. L'un comme l'autre ne va qu'à l'hôte du registre. (REQ-2088)

## Index PyPI {: #pypi-indexes }

| Champ | Contient |
|---|---|
| URL de l'index | L'adresse simple-index de l'index, par exemple `https://artifactory.example.com/artifactory/api/pypi/pypi/simple/` |
| Jeton ou utilisateur:mot de passe | Un jeton d'accès, envoyé comme l'utilisateur `__token__`, ou `user:password` |
| Index par défaut de chaque installation | Tout paquet Python est installé depuis cet index au lieu du PyPI public. Un par organisation |
| Autorité de certification (PEM) | Voir [Une autorité de certification privée](#a-private-certificate-authority) |

Les index et leurs identifiants parviennent à uv dans l'environnement propre à l'installation, jamais dans un fichier.

## Hôtes Git {: #git-hosts }

Une entrée d'hôte Git couvre les dépôts d'où sont récupérés le code d'une source ou ses dépendances Git.

| Champ | Contient |
|---|---|
| Hôte | L'hôte, avec son port et un préfixe de chemin quand le serveur en a : `github.com`, `gitlab.example.com`, `bitbucket.example.com:7990/bitbucket` |
| Jeton ou utilisateur:jeton | Un jeton, ou `user:token` |
| Autorité de certification (PEM) | Voir [Une autorité de certification privée](#a-private-certificate-authority) |

Une URL de dépôt est associée à l'entrée dont l'hôte, le port et le préfixe de chemin la commencent. Quand plusieurs correspondent, la plus longue l'emporte : une entrée pour `bitbucket.example.com:7990/bitbucket/scm/data` couvre ce projet et une pour `bitbucket.example.com:7990/bitbucket` couvre le reste. (REQ-2088)

Un jeton seul est envoyé avec l'utilisateur `x-access-token`. D'autres fournisseurs attendent un utilisateur ; saisissez alors `user:token` :

| Fournisseur | Utilisateur |
|---|---|
| GitHub | `x-access-token`, la valeur par défaut : saisissez le jeton seul |
| GitLab | N'importe quel nom d'utilisateur non vide |
| Bitbucket Cloud | `x-token-auth` pour un jeton d'accès de dépôt ou d'espace de travail ; votre nom d'utilisateur ou votre e-mail pour un jeton d'API |
| Bitbucket Data Center | Votre nom d'utilisateur ; `x-token-auth` pour un jeton de projet ou de dépôt |

## Une autorité de certification privée {: #a-private-certificate-authority }

Un registre, un index ou un serveur Git derrière votre propre autorité de certification, comme un proxy Artifactory dans votre réseau, a besoin que cette autorité soit approuvée. Collez son certificat PEM dans le champ **Autorité de certification (PEM)** de l'entrée, ou une référence à un secret qui le contient. Laissez le champ vide pour un serveur dont le certificat est approuvé publiquement. Une valeur sans certificat PEM est refusée à l'enregistrement de l'entrée, et une référence qui ne se résout en aucun est refusée à l'installation. (REQ-2088)

L'autorité est approuvée en plus des autorités publiques, jamais à leur place :

- Les récupérations de Provisa par l'entrée (les métadonnées et le tarball d'un paquet, un wheel, un dépôt Git) vérifient le serveur par rapport à elle.
- Pendant une installation, uv la lit par `SSL_CERT_FILE`, npm et Bun par `cafile` dans le `.npmrc` propre à l'installation, Deno par `DENO_CERT` et Git par `GIT_SSL_CAINFO`.

Ces réglages désignent un seul fichier contenant les autorités publiques et celles de chaque entrée. Le fichier est écrit dans le répertoire temporaire de l'installation et supprimé avec lui ; rien en dehors de l'installation ne le lit.
