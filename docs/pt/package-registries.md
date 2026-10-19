# Registros de pacotes

As fontes Node e Python instalam o próprio código e as próprias dependências. **Admin → Registros de pacotes** define de onde elas instalam: os registros npm, os índices PyPI e os hosts Git da organização. Um administrador da organização os informa uma vez, e toda instalação Node e Python da organização os usa. O registro npm público e o PyPI público não precisam de entrada.

Cada campo guarda o que foi digitado: um valor, ou uma referência como `${secret:npm_token}` ou `${env:NPM_TOKEN}`, resolvida no cofre da própria organização a cada instalação. Uma referência que não resolve para nada interrompe a instalação e informa o registro e o campo. O Provisa nunca escreve no cofre nem inventa o nome de um segredo. Veja [Segredos](secrets.md).

---

## Registros npm {: #npm-registries }

| Campo | Guarda |
|---|---|
| URL do registro | O endereço do registro, por exemplo `https://artifactory.example.com/artifactory/api/npm/npm/` |
| Escopo | Um escopo como `@acme`, cujos pacotes vêm deste registro. Vazio para nenhum |
| Token ou usuário:senha | Um token de acesso, ou `user:password` |
| Espelho de toda instalação | Todo pacote sem escopo próprio é instalado por este registro. Um por organização |
| Autoridade certificadora (PEM) | Veja [Uma autoridade certificadora própria](#a-private-certificate-authority) |

Um token é escrito no `.npmrc` da instalação como `_authToken` e enviado como token bearer. `user:password`, por exemplo um usuário do Artifactory e sua chave de API, é escrito como `_auth` (base64) e enviado como Basic. Qualquer um dos dois vai somente para o host do próprio registro.

## Índices PyPI {: #pypi-indexes }

| Campo | Guarda |
|---|---|
| URL do índice | O endereço simple-index do índice, por exemplo `https://artifactory.example.com/artifactory/api/pypi/pypi/simple/` |
| Token ou usuário:senha | Um token de acesso, enviado como o usuário `__token__`, ou `user:password` |
| Índice padrão de toda instalação | Todo pacote Python é instalado deste índice em vez do PyPI público. Um por organização |
| Autoridade certificadora (PEM) | Veja [Uma autoridade certificadora própria](#a-private-certificate-authority) |

Os índices e suas credenciais chegam ao uv no ambiente da própria instalação, nunca em um arquivo.

## Hosts Git {: #git-hosts }

Uma entrada de host Git cobre os repositórios de onde vêm o código de uma fonte ou suas dependências Git.

| Campo | Guarda |
|---|---|
| Host | O host, com porta e prefixo de caminho quando o servidor os tem: `github.com`, `gitlab.example.com`, `bitbucket.example.com:7990/bitbucket` |
| Token ou usuário:token | Um token, ou `user:token` |
| Autoridade certificadora (PEM) | Veja [Uma autoridade certificadora própria](#a-private-certificate-authority) |

Uma URL de repositório é associada à entrada cujo host, porta e prefixo de caminho a iniciam. Quando várias coincidem, vence a mais longa: uma entrada para `bitbucket.example.com:7990/bitbucket/scm/data` cobre esse projeto e uma para `bitbucket.example.com:7990/bitbucket` cobre o resto.

Um token sozinho é enviado com o usuário `x-access-token`. Outros provedores esperam um usuário, então informe `user:token`:

| Provedor | Usuário |
|---|---|
| GitHub | `x-access-token`, o padrão: informe só o token |
| GitLab | Qualquer nome de usuário não vazio |
| Bitbucket Cloud | `x-token-auth` para um token de acesso de repositório ou de workspace; seu nome de usuário ou e-mail para um token de API |
| Bitbucket Data Center | Seu nome de usuário; `x-token-auth` para um token de projeto ou de repositório |

## Uma autoridade certificadora própria {: #a-private-certificate-authority }

Um registro, índice ou servidor Git atrás da sua própria autoridade certificadora, como um proxy do Artifactory dentro da sua rede, precisa que essa autoridade seja confiável. Cole o certificado PEM dela no campo **Autoridade certificadora (PEM)** da entrada, ou uma referência a um segredo que o contenha. Deixe o campo vazio para um servidor cujo certificado é publicamente confiável. Um valor sem certificado PEM é recusado ao salvar a entrada, e uma referência que não resolve para nenhum é recusada quando uma instalação é executada.

A autoridade é confiável junto às autoridades públicas, nunca no lugar delas:

- As buscas do próprio Provisa pela entrada (os metadados e o tarball de um pacote, um wheel, um repositório Git) verificam o servidor contra ela.
- Durante uma instalação, o uv a lê por `SSL_CERT_FILE`, o npm e o Bun por `cafile` no `.npmrc` da própria instalação, o Deno por `DENO_CERT` e o Git por `GIT_SSL_CAINFO`.

Essas configurações apontam para um único arquivo com as autoridades públicas e as de cada entrada. O arquivo é escrito no diretório temporário da instalação e removido com ele, de modo que nada fora da instalação o lê.
