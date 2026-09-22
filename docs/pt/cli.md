# Referência da CLI

O comando `provisa` é o único ponto de entrada para o nível embutido instalado via pip (REQ-1128).
Ele inicia o runtime, gerencia licenças, aciona a publicação de metadados, implanta modelos e
controla o banner de manutenção — sem Docker, Node ou qualquer serviço externo.

Instale-o com o extra `embedded`, que também traz as extensões DuckDB offline
e o plano de controle PostgreSQL embutido:

```bash
pip install 'provisa[embedded]'
```

**Requisitos de plataforma.** `provisa run` exige Python 3.12 e uma plataforma com wheel do
pgserver: linux x86_64, macOS ou Windows x86_64. Linux aarch64 não tem wheel do pgserver nem
distribuição de código-fonte, então o nível embutido não roda lá. Use o nível de container em
aarch64. [tool-verified: `_require_supported_interpreter()` at cli.py:513-546]

---

## Opções compartilhadas {: #opções-compartilhadas }

Vários subcomandos chamam a API HTTP do Provisa. Eles compartilham três flags e duas variáveis
de ambiente. [tool-verified: `_api_call()` at cli.py:345-376; each subcommand's argparse block
at cli.py:653-773]

| Flag | Padrão | Fallback de variável de ambiente |
| --- | --- | --- |
| `--api <url>` | `http://127.0.0.1:8000` | `PROVISA_API_URL` |
| `--token <token>` | _(nenhum)_ | `PROVISA_API_TOKEN` |
| `--timeout <seconds>` | 300 (30 para `maintenance`) | _(nenhum)_ |

`--api` é a URL base de uma instância do Provisa em execução. Sob multilocação o nome do host
nomeia a organização — `https://acme.provisa.org` roteia para o locatário da acme. `--token` é
um token Bearer; quando está vazio nenhum cabeçalho `Authorization` é enviado, o que é correto
para implantações não autenticadas. [tool-verified: cli.py:314-316, 357-365]

Defina ambas as variáveis no seu ambiente de CI para evitar repeti-las a cada chamada:

```bash
export PROVISA_API_URL=https://acme.provisa.org
export PROVISA_API_TOKEN=<token>
```

Subcomandos que aceitam essas flags: `metadata export`, `env deploy`, `env fetch`,
`maintenance on`, `maintenance off`, `maintenance status`.

---

## provisa run

Inicia o sistema Provisa embutido — servidor de API e servidor estático/proxy da UI — em um
único processo. Sem Docker, sem Node, sem serviços externos. [tool-verified: cli.py module
docstring lines 11-23; `_cmd_run()` at cli.py:549-602]

```
provisa run [--demo] [--host HOST] [--api-port PORT] [--ui-port PORT]
            [--no-browser] [--reset] [--data-dir DIR]
```

### Flags

| Flag | Padrão | Notas |
| --- | --- | --- |
| `--demo` | desligado | Carrega a demo empacotada — domínios de exemplo pet-store e shelter sobre SQLite embutido (REQ-414) |
| `--host` | `127.0.0.1` | Endereço de bind para ambos os servidores |
| `--api-port` | `8000` | Porta do servidor de API |
| `--ui-port` | `3000` | Porta do servidor estático/proxy da UI |
| `--no-browser` | desligado | Pula a abertura de um navegador quando a UI estiver pronta; ainda imprime a URL |
| `--reset` | desligado | Descarta e reconstrói o armazenamento do plano de controle embutido antes de iniciar; use após um upgrade do Provisa se a inicialização reportar incompatibilidade de esquema |
| `--data-dir` | `~/.provisa/native` | Diretório que armazena o cluster PostgreSQL embutido e o cache de extensões DuckDB |

[tool-verified: run subparser at cli.py:609-634]

### Variáveis de ambiente

`provisa run` lê várias variáveis adicionais antes de os servidores HTTP iniciarem.
Defina-as para sobrepor os padrões que `load_profile("native", ...)` aplicaria de outra forma.
[tool-verified: `_apply_embedded_env()` at cli.py:83-116]

| Variável | Efeito |
| --- | --- |
| `TRINO_HOST` / `TRINO_PORT` | Substitui o motor DuckDB embutido por um coordenador Trino fornecido pelo cliente (REQ-1129) |
| `PROVISA_ENGINE_URL` | Forma alternativa de apontar para um motor de federação externo |
| `PROVISA_CONFIG` | Arquivo de config a carregar; `--demo` define isso para a config de demo empacotada (REQ-1127) |
| `PROVISA_DEMO` | Definida como `1` por `--demo`; marca a sessão como uma execução de demo |
| `PROVISA_DEMO_DIR` | Caminho para o diretório de dados de exemplo da demo; definido por `--demo` |
| `PROVISA_CONFIG_REPLACE` | Definida como `true` por `--demo` para permitir que a config de demo sobrescreva qualquer config existente |
| `PROVISA_DUCKDB_EXT_DIR` | Diretório de extensões DuckDB pré-preparado; definido automaticamente a partir do pacote `provisa-duckdb-ext` se presente; ausente significa que o DuckDB baixa da rede no primeiro uso |

[tool-verified: `_apply_demo_config()` at cli.py:67-80; `_apply_embedded_env()` at cli.py:83-116]

### Sequência de inicialização

1. Verificação de plataforma — aborta com uma mensagem clara em Python não suportado ou pgserver ausente.
2. `--reset` (se solicitado) — descarta o cluster PostgreSQL embutido; ele é reconstruído na etapa seguinte.
3. Config de demo (se `--demo`) — define `PROVISA_CONFIG` e `PROVISA_DEMO_DIR`.
4. Ambiente embutido — inicia o plano de controle PostgreSQL, resolve sua URL de socket, e
   prepara extensões DuckDB offline se `provisa-duckdb-ext` estiver instalado.
5. Verificação de desvio de esquema — varre o plano de controle ativo em busca de colunas ausentes. Se alguma for
   encontrada, imprime uma dica de `--reset` e sai com código 1. V1 não tem migrações; uma coluna adicionada em uma
   release mais nova exige um reset. [tool-verified: `_control_plane_drift()` at cli.py:119-162]
6. Ambos os servidores iniciam concorrentemente. O anunciador de prontidão consulta `GET /ready` (não `/health` — o
   endpoint `/ready` confirma que o armazenamento está conectado e o motor está aquecido) e abre o navegador
   quando ele retorna 200. [tool-verified: `_announce_ready()` at cli.py:182-224]

### Códigos de saída

| Código | Significado |
| --- | --- |
| 0 | Encerramento limpo (Ctrl-C) |
| 1 | Erro de inicialização (verificação de plataforma falhou, config de demo ausente, desvio de esquema detectado) |

### Exemplo

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

Verifica e instala um arquivo de licença offline (REQ-1139). O arquivo é o `license.json` emitido
pela provisa.dev. [tool-verified: `_cmd_license_apply()` at cli.py:271-280]

```
provisa license apply <file>
```

| Argumento | Notas |
| --- | --- |
| `file` | Caminho para o arquivo de licença; expansão de `~` é aplicada |

Código de saída 0 significa que a licença é válida e foi instalada. Código de saída 1 significa
que foi rejeitada; o motivo é impresso no stderr. [tool-verified: cli.py:276-280]

```bash
provisa license apply ~/Downloads/license.json
```

---

## provisa license status

Mostra o ID da máquina, o estado do trial, os dias decorridos e a validade da licença (REQ-1139).
[tool-verified: `_cmd_license_status()` at cli.py:283-299]

```
provisa license status
```

Sem flags. Imprime quatro linhas — ID da máquina, data de primeiro uso, dias decorridos, estado
do trial — e estado da licença — e sai com 0. [tool-verified: cli.py:293-299]

```
Machine ID:   a1b2c3d4e5f6...
First seen:   2026-07-01
Elapsed:      83.0 days
Trial:        active
Licensed:     no (no license installed)
```

---

## provisa metadata export

Aciona a publicação de metadados sob demanda do servidor em execução (REQ-1072/REQ-1074). Envia
um POST para `POST /admin/metadata-export/publish` — o mesmo endpoint que o botão **Publish now**
da aba Admin chama, então ambos os caminhos enviam o mesmo snapshot completo. [tool-verified:
`_cmd_metadata_export()` at cli.py:302-342]

```
provisa metadata export [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Flag | Padrão | Notas |
| --- | --- | --- |
| `--api` | `$PROVISA_API_URL`, depois `http://127.0.0.1:8000` | Sob multilocação o host nomeia a organização |
| `--token` | `$PROVISA_API_TOKEN` | Token Bearer para uma identidade que detém `org_settings`; omita em implantações não autenticadas |
| `--timeout` | `300` | Segundos antes de a chamada HTTP ser abandonada |

[tool-verified: cli.py:654-669]

| Código de saída | Significado |
| --- | --- |
| 0 | Todo ativo publicado |
| 1 | Publicação parcial ou falha de conexão; erros por ativo são impressos no stderr |

[tool-verified: cli.py:335-342]

```bash
provisa metadata export \
  --api  https://acme.provisa.org \
  --token "$PROVISA_API_TOKEN"

# Cron example — daily at 06:00
# 0 6 * * *  provisa metadata export --api https://acme.provisa.org >> /var/log/provisa-export.log 2>&1
```

A referência completa de configuração — provedores, credenciais, `reconcile_cron`, e o que o
snapshot contém — está em [Exportação de Metadados](metadata-export.md#from-the-command-line).

---

## provisa env deploy

Implanta o modelo em um ref do git em um ambiente, tornando essa árvore o modelo atual do
ambiente (REQ-1496). Este é o comando que um pipeline de implantação executa; a regra é que um
deploy é sempre uma invocação carregando uma identidade contra um plano de controle nomeado.
[tool-verified: `_cmd_env_deploy()` at cli.py:395-417]

```
provisa env deploy --org ORG --env ENV --ref REF
                   [--dry-run] [--seed] [--message MSG]
                   [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Flag | Obrigatório | Notas |
| --- | --- | --- |
| `--org` | sim | Organização que possui o ambiente |
| `--env` | sim | Ambiente que armazenará o modelo implantado |
| `--ref` | sim | Branch ou SHA de commit no repositório da organização |
| `--dry-run` | não | Reporta o que mudaria; não aplica nada |
| `--seed` | não | Também aplica classes somente de criação (roles); correto apenas quando este deploy cria o ambiente pela primeira vez |
| `--message` | não | Nota carregada em uma solicitação de aprovação quando o ambiente de destino é protegido |
| `--api` | não | Veja [Opções compartilhadas](#opções-compartilhadas) |
| `--token` | não | Veja [Opções compartilhadas](#opções-compartilhadas) |
| `--timeout` | não | Padrão 300 s |

[tool-verified: cli.py:677-711]

| Código de saída | Significado |
| --- | --- |
| 0 | Deploy aplicado, ou `--dry-run` concluído |
| 2 | Ambiente é protegido; o deploy foi apenas proposto, não aplicado |

O código de saída 2 é intencional. Um pipeline que tratasse uma aprovação pendente como um deploy
liberado estaria errado. [tool-verified: cli.py:416-417]

```bash
# Fetch remote branches, then deploy
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"

provisa env deploy \
  --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

Para a explicação completa das classes de ambiente, regras de proteção, relatórios de merge, e o
ciclo de vida de aprovação, veja [Environments](environments.md#the-env-cli-commands).

---

## provisa env fetch

Busca os branches remotos da organização em seu repositório Provisa (REQ-1541). Execute isso
antes de um deploy quando você quiser nomear `origin/<branch>`. [tool-verified:
`_cmd_env_fetch()` at cli.py:420-432]

```
provisa env fetch --org ORG [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Flag | Obrigatório | Notas |
| --- | --- | --- |
| `--org` | sim | Organização cujo remoto é buscado |
| `--api` | não | Veja [Opções compartilhadas](#opções-compartilhadas) |
| `--token` | não | Token Bearer para um administrador de organização |
| `--timeout` | não | Padrão 300 s |

[tool-verified: cli.py:716-733]

Imprime uma linha por branch buscado — `origin/<name>  <sha12>`. Sai com 0 em caso de sucesso;
levanta `SystemExit` com uma mensagem de erro em caso de falha de HTTP ou conexão.

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# origin/main  a1b2c3d4e5f6
# origin/dev   9f8e7d6c5b4a
```

---

## provisa maintenance on

Ativa o banner de manutenção programada na implantação (REQ-1466). Execute isso antes de um
trabalho planejado que derrube o plano de dados — por exemplo, antes de alterar
`var.engine_cluster_mode`, o que substitui o cluster do motor e cada shard nele (REQ-1465).
[tool-verified: `_cmd_maintenance_on()` at cli.py:483-495]

```
provisa maintenance on [--message MSG] [--ends-at ISO8601]
                       [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Flag | Notas |
| --- | --- |
| `--message` | Sobrepõe o texto padrão da implantação; o padrão é a mensagem padrão do servidor |
| `--ends-at` | Instante ISO-8601 em que o trabalho deve terminar, ex. `2026-08-14T22:30:00Z`; o padrão é sem estimativa |
| `--api` | Veja [Opções compartilhadas](#opções-compartilhadas) |
| `--token` | Token Bearer para uma identidade que detém `platform_settings` |
| `--timeout` | Padrão 30 s |

[tool-verified: cli.py:743-773]

Imprime o estado resultante do banner e sai com 0. Levanta `SystemExit` em caso de falha de HTTP
ou conexão.

```bash
provisa maintenance on \
  --message "Engine cluster rolling upgrade" \
  --ends-at "2026-09-22T03:00:00Z" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## provisa maintenance off

Limpa o banner de manutenção assim que o trabalho é concluído (REQ-1466).
[tool-verified: `_cmd_maintenance_off()` at cli.py:498-501]

```
provisa maintenance off [--api URL] [--token TOKEN] [--timeout SECONDS]
```

Imprime o estado resultante do banner (active: false) e sai com 0.

```bash
provisa maintenance off --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# Maintenance notice: OFF
```

---

## provisa maintenance status

Mostra o estado atual do banner de manutenção sem alterá-lo (REQ-1466).
[tool-verified: `_cmd_maintenance_status()` at cli.py:504-507]

```
provisa maintenance status [--api URL] [--token TOKEN] [--timeout SECONDS]
```

Imprime o estado do banner e sai com 0.

```
Maintenance notice: ON
  Message:  Engine cluster rolling upgrade
  Since:    2026-09-22T01:15:00Z
  Ends at:  2026-09-22T03:00:00Z
```

---

## Referência rápida

| Comando | REQ | O que faz |
| --- | --- | --- |
| `provisa run` | REQ-1128 | Inicia a API + UI embutidas |
| `provisa run --demo` | REQ-414 | Inicia com dados de exemplo pet-store / shelter |
| `provisa run --reset` | REQ-1535 | Reconstrói o plano de controle antes de iniciar |
| `provisa license apply <file>` | REQ-1139 | Instala um arquivo de licença offline |
| `provisa license status` | REQ-1139 | Mostra o ID da máquina e o estado de trial / licença |
| `provisa metadata export` | REQ-1072 | Publica o snapshot de metadados sob demanda |
| `provisa env fetch --org ORG` | REQ-1541 | Busca branches remotos para o repositório Provisa |
| `provisa env deploy --org ORG --env ENV --ref REF` | REQ-1496 | Implanta um ref em um ambiente |
| `provisa maintenance on` | REQ-1466 | Ativa o banner de manutenção |
| `provisa maintenance off` | REQ-1466 | Limpa o banner de manutenção |
| `provisa maintenance status` | REQ-1466 | Mostra o estado atual do banner |

## Veja também

- [Environments](environments.md) — modelo de ambiente, ambientes protegidos, ciclo de vida de aprovação de deploy
- [Metadata Export](metadata-export.md) — provedores de catálogo, configuração, e o que o snapshot contém
- [Deployment](deployment.md) — nível de container e implantação em nuvem
