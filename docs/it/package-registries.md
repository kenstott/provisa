# Registri di pacchetti

Le sorgenti Node e Python installano il proprio codice e le proprie dipendenze. **Admin → Registri di pacchetti** indica da dove installano: i registri npm, gli indici PyPI e gli host Git dell'organizzazione. Un amministratore dell'organizzazione li inserisce una volta, e ogni installazione Node e Python dell'organizzazione li usa. Il registro npm pubblico e il PyPI pubblico non richiedono alcuna voce.

Ogni campo contiene ciò che è stato digitato: un valore, oppure un riferimento come `${secret:npm_token}` o `${env:NPM_TOKEN}`, risolto nel vault dell'organizzazione a ogni installazione. Un riferimento che non si risolve in nulla interrompe l'installazione e indica il registro e il campo. Provisa non scrive mai nel vault e non inventa mai il nome di un segreto. Vedere [Segreti](secrets.md).

---

## Registri npm {: #npm-registries }

| Campo | Contiene |
|---|---|
| URL del registro | L'indirizzo del registro, ad esempio `https://artifactory.example.com/artifactory/api/npm/npm/` |
| Scope | Uno scope come `@acme`, i cui pacchetti provengono da questo registro. Vuoto per nessuno |
| Token o utente:password | Un token di accesso, oppure `user:password` |
| Mirror di ogni installazione | Ogni pacchetto senza uno scope proprio viene installato tramite questo registro. Uno per organizzazione |
| Autorità di certificazione (PEM) | Vedere [Un'autorità di certificazione privata](#a-private-certificate-authority) |

Un token viene scritto nel `.npmrc` dell'installazione come `_authToken` e inviato come token bearer. `user:password`, ad esempio un utente Artifactory e la sua chiave API, viene scritto come `_auth` (base64) e inviato come Basic. In entrambi i casi va solo all'host del registro.

## Indici PyPI {: #pypi-indexes }

| Campo | Contiene |
|---|---|
| URL dell'indice | L'indirizzo simple-index dell'indice, ad esempio `https://artifactory.example.com/artifactory/api/pypi/pypi/simple/` |
| Token o utente:password | Un token di accesso, inviato come utente `__token__`, oppure `user:password` |
| Indice predefinito di ogni installazione | Ogni pacchetto Python viene installato da questo indice invece che dal PyPI pubblico. Uno per organizzazione |
| Autorità di certificazione (PEM) | Vedere [Un'autorità di certificazione privata](#a-private-certificate-authority) |

Gli indici e le loro credenziali arrivano a uv nell'ambiente proprio dell'installazione, mai in un file.

## Host Git {: #git-hosts }

Una voce di host Git copre i repository da cui vengono recuperati il codice di una sorgente o le sue dipendenze Git.

| Campo | Contiene |
|---|---|
| Host | L'host, con porta e prefisso di percorso se il server li ha: `github.com`, `gitlab.example.com`, `bitbucket.example.com:7990/bitbucket` |
| Token o utente:token | Un token, oppure `user:token` |
| Autorità di certificazione (PEM) | Vedere [Un'autorità di certificazione privata](#a-private-certificate-authority) |

Un URL di repository viene associato alla voce con cui inizia per host, porta e prefisso di percorso. Se ne corrispondono più di una, vince la più lunga: una voce per `bitbucket.example.com:7990/bitbucket/scm/data` copre quel progetto e una per `bitbucket.example.com:7990/bitbucket` copre il resto.

Un token da solo viene inviato con l'utente `x-access-token`. Altri provider si aspettano un utente, quindi inserire `user:token`:

| Provider | Utente |
|---|---|
| GitHub | `x-access-token`, il predefinito: inserire solo il token |
| GitLab | Qualsiasi nome utente non vuoto |
| Bitbucket Cloud | `x-token-auth` per un token di accesso di repository o di workspace; il proprio nome utente o indirizzo email per un token API |
| Bitbucket Data Center | Il proprio nome utente; `x-token-auth` per un token di progetto o di repository |

## Un'autorità di certificazione privata {: #a-private-certificate-authority }

Un registro, un indice o un server Git dietro la propria autorità di certificazione, come un proxy Artifactory all'interno della rete, richiede che tale autorità sia considerata attendibile. Incollare il suo certificato PEM nel campo **Autorità di certificazione (PEM)** della voce, oppure un riferimento a un segreto che lo contiene. Lasciare il campo vuoto per un server il cui certificato è attendibile pubblicamente. Un valore senza certificato PEM viene rifiutato al salvataggio della voce, e un riferimento che non si risolve in alcuno viene rifiutato quando si esegue un'installazione.

L'autorità è considerata attendibile accanto a quelle pubbliche, mai al loro posto:

- I recuperi di Provisa tramite la voce (i metadati e il tarball di un pacchetto, un wheel, un repository Git) verificano il server rispetto a essa.
- Durante un'installazione, uv la legge tramite `SSL_CERT_FILE`, npm e Bun tramite `cafile` nel `.npmrc` proprio dell'installazione, Deno tramite `DENO_CERT` e Git tramite `GIT_SSL_CAINFO`.

Queste impostazioni indicano un unico file con le autorità pubbliche e quelle di ogni voce. Il file viene scritto nella directory temporanea dell'installazione e rimosso con essa, quindi nulla al di fuori dell'installazione lo legge.
