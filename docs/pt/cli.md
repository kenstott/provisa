# Referência da CLI {: #cli-reference }

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

## Opções compartilhadas {: #shared-options }

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

Subcomandos que aceitam essas flags: `metadata export`, todo subcomando `env` (`list`, `show`,
`create`, `delete`, `diff`, `merge`, `deploy`, `revert`, `redo`, `fetch`, `push`, `pull`),
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

### Variáveis de ambiente {: #environment-variables }

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
| `PROVISA_DUCKDB_EXT_DIR` | Diretório de extensões DuckDB pré-preparado; definido automaticamente a partir do pacote `provisa-duckdb-ext` se presente; ausente significa que o DuckDB baixa da rede no primeiro uso |

[tool-verified: `_apply_demo_config()` at cli.py:67-80; `_apply_embedded_env()` at cli.py:83-116]

### Sequência de inicialização {: #startup-sequence }

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

### Códigos de saída {: #exit-codes }

| Código | Significado |
| --- | --- |
| 0 | Encerramento limpo (Ctrl-C) |
| 1 | Erro de inicialização (verificação de plataforma falhou, config de demo ausente, desvio de esquema detectado) |

### Exemplo {: #example }

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

## provisa env

Executa o ciclo de vida dos ambientes contra um Provisa em execução (REQ-1498). Cada subcomando é uma chamada à
API de administração que a interface chama; a CLI não decide nada. A lista do que uma cópia leva, as regras de
merge e de remoção, o vínculo, a proteção e os direitos do chamador são do servidor, então valem
seja qual for a forma como a operação é invocada. [tool-verified: cli_envs.py:11-28]

```
provisa env {list,show,create,delete,diff,merge,deploy,revert,redo,fetch,push,pull} --org ORG ...
```
### Flags que todo subcomando aceita {: #flags-every-subcommand-takes }

| Flag | Obrigatória | Notas |
| --- | --- | --- |
| `--org` | sim | Organização que contém o ambiente |
| `--json` | não | Imprime a resposta da API como JSON, para um script |
| `--api` | não | Veja [Opções compartilhadas](#shared-options) |
| `--token` | não | Token bearer de uma identidade que pode executar a operação |
| `--timeout` | não | Segundos; padrão 300 |

Os subcomandos que agem sobre um ambiente também aceitam `--env ENV` (obrigatório). `list`, `diff`,
`merge` e `fetch` não; `diff` e `merge` nomeiam seus ambientes com `--from` e `--into`.
[tool-verified: cli_envs.py:349-374, `_command()`]

### Códigos de saída {: #exit-codes_1 }

| Código | Significado |
| --- | --- |
| 0 | Feito, ou um ensaio que não aplicou nada |
| 1 | Recusado. O código da regra que recusa é impresso em stderr como `refused (<code>): <detail>`; com `--json` é `{"refused": {...}}` em stdout |
| 2 | Proposto, não aplicado: o ambiente de destino está protegido e a mudança aguarda uma aprovação |

O código de saída 2 é intencional. Um pipeline que tratasse uma aprovação pendente como um deploy liberado
estaria errado. [tool-verified: cli_envs.py:21-28, 40-41, 64-78, 120-135]

### provisa env list

Lista os ambientes da organização e o build em que cada um está. Nenhuma flag além das compartilhadas.
[tool-verified: cli_envs.py:141-149]

```
provisa env list --org ORG [--json]
```
### provisa env show

Mostra um ambiente e seu build atual. Sai com 1 e `environments.unknown` quando o
ambiente não existe. [tool-verified: cli_envs.py:163-183]

```
provisa env show --org ORG --env ENV [--json]
```
### provisa env create

Cria um ambiente que contém o modelo de outro, com seus dados escolhidos na criação (REQ-1942). A API
decide quais escolhas o chamador pode fazer. [tool-verified: cli_envs.py:197-216]

```
provisa env create --org ORG --env NAME --data-mode MODE
                   [--from SOURCE] [--mutation-handling HANDLING] [--expires-at ISO8601]
```
| Flag | Obrigatória | Notas |
| --- | --- | --- |
| `--env` | sim | Nome do novo ambiente |
| `--from` | não | Ambiente de partida; padrão `prod` |
| `--data-mode` | sim | `inherit`, `unbound`, `test_fake` ou `test_synthetic`: as conexões da fonte por referência, nenhuma até que vincule as suas, ou dados de teste |
| `--mutation-handling` | não | `refused`, `reversible` ou `direct`; padrão `refused` |
| `--expires-at` | não | Instante ISO-8601 após o qual o ambiente é excluído |

[tool-verified: cli_envs.py:391-416]

```bash
provisa env create --org acme --env feature-x --from prod --data-mode test_fake
```
### provisa env delete

Aposenta um ambiente. Seus branches só saem quando nomeados. [tool-verified: cli_envs.py:219-232]

```
provisa env delete --org ORG --env ENV [--delete-branch] [--delete-remote-branch]
```
| Flag | Notas |
| --- | --- |
| `--delete-branch` | Exclui também seu branch |
| `--delete-remote-branch` | Exclui também seu branch no remoto |

[tool-verified: cli_envs.py:418-424]

### provisa env diff

Informa o que faria o merge de um ambiente em outro. É um GET de `merge-preview`, então
não pode escrever; entregue-o a um runner que não deve mudar nada. [tool-verified:
cli_envs.py:186-191]

```
provisa env diff --org ORG --from SOURCE --into TARGET [--removals] [--select PATH ...]
```
| Flag | Obrigatória | Notas |
| --- | --- | --- |
| `--from` | sim | Ambiente de onde se faz o merge |
| `--into` | sim | Ambiente para onde se faz o merge |
| `--removals` | não | Remove também do destino o que a fonte não tem mais |
| `--select PATH` | não | Limita o merge a este objeto, pelo seu caminho no modelo (`metrics/revenue.yaml`); repita para mais. O que ele exige e falta ao destino é levado junto e listado (REQ-1506) |

[tool-verified: cli_envs.py:426-439]

### provisa env merge

Faz o merge de um ambiente em outro, por identidade (REQ-1490). [tool-verified: cli_envs.py:235-250]

```
provisa env merge --org ORG --from SOURCE --into TARGET
                  [--removals] [--select PATH ...] [--dry-run] [--message MSG]
                  [--retire-source] [--retire-remote]
```
| Flag | Obrigatória | Notas |
| --- | --- | --- |
| `--from` | sim | Ambiente de onde se faz o merge |
| `--into` | sim | Ambiente para onde se faz o merge |
| `--removals` | não | Remove também do destino o que a fonte não tem mais |
| `--select PATH` | não | Limita o merge a este objeto, pelo seu caminho no modelo (`metrics/revenue.yaml`); repita para mais. O que ele exige e falta ao destino é levado junto e listado (REQ-1506) |
| `--dry-run` | não | Imprime o relatório de merge sem aplicar nada |
| `--message` | não | O que o merge faz; obrigatório para aplicar |
| `--retire-source` | não | Exclui a fonte depois que seu trabalho chegar |
| `--retire-remote` | não | Com `--retire-source`, exclui também o branch da fonte no remoto |

[tool-verified: cli_envs.py:429-451]

### provisa env deploy

Implanta o modelo de uma ref git em um ambiente, tornando essa árvore o modelo atual do ambiente
(REQ-1496). É o comando que um pipeline de implantação executa; um deploy é sempre uma
invocação com uma identidade contra um plano de controle nomeado. [tool-verified:
cli_envs.py:253-267]

```
provisa env deploy --org ORG --env ENV --ref REF [--dry-run] [--message MSG]
```
| Flag | Obrigatória | Notas |
| --- | --- | --- |
| `--env` | sim | Ambiente que terá o modelo implantado |
| `--ref` | sim | Branch ou commit do repositório da organização |
| `--dry-run` | não | Informa o que o deploy faria; não aplica nada |
| `--message` | não | Nota levada para uma solicitação de aprovação quando o destino está protegido |

[tool-verified: cli_envs.py:453-468]

Uma carga em um ambiente existente nunca aplica funções. `--seed` não existe mais: a API recusa um seed
com `environments.seed_only_at_creation`.

```bash
# Fetch remote branches, then deploy
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"

provisa env deploy \
  --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```
### provisa env revert

Leva um ambiente de volta a um build anterior (REQ-1543). Sem `--to`, volta um commit
no histórico próprio do ambiente. Com `--to`, é o deploy comum desse build.
[tool-verified: cli_envs.py:270-284]

```
provisa env revert --org ORG --env ENV [--to BUILD] [--message MSG]
```
| Flag | Obrigatória | Notas |
| --- | --- | --- |
| `--env` | sim | Ambiente a reverter |
| `--to` | não | Build a implantar; padrão é o anterior ao build atual |
| `--message` | não | Nota levada para uma solicitação de aprovação |

[tool-verified: cli_envs.py:470-480]

### provisa env redo

Avança de novo em direção ao build de onde um revert partiu. [tool-verified:
cli_envs.py:287-301]

```
provisa env redo --org ORG --env ENV
```
### provisa env fetch

Busca os branches remotos da organização no seu repositório Provisa (REQ-1541). Execute antes de um deploy
quando quiser nomear `origin/<branch>`. O Provisa nunca o executa por temporizador. [tool-verified:
cli_envs.py:307-320]

```
provisa env fetch --org ORG
```
Imprime uma linha por branch buscado, `origin/<name>  <sha12>`.

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# origin/main  a1b2c3d4e5f6
# origin/dev   9f8e7d6c5b4a
```
### provisa env push

Envia o branch de um ambiente para o remoto da organização (REQ-1546). Imprime `nothing to send` quando
não há nada a enviar. [tool-verified: cli_envs.py:323-331]

```
provisa env push --org ORG --env ENV
```
### provisa env pull

Aplica o que o remoto tem para um ambiente (REQ-1547). Imprime
`<env> already holds what the remote does` quando não há nada a aplicar. O código de saída 2 vale como
no deploy. [tool-verified: cli_envs.py:334-343]

```
provisa env pull --org ORG --env ENV
```
Para a explicação completa das classes de ambiente, das regras de proteção, dos relatórios de merge e do
ciclo de vida das aprovações, veja [Ambientes](environments.md#the-env-cli-commands).

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
| `--api` | Veja [Opções compartilhadas](#shared-options) |
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

## Referência rápida {: #quick-reference }

| Comando | REQ | O que faz |
| --- | --- | --- |
| `provisa run` | REQ-1128 | Inicia a API + UI embutidas |
| `provisa run --demo` | REQ-414 | Inicia com dados de exemplo pet-store / shelter |
| `provisa run --reset` | REQ-1535 | Reconstrói o plano de controle antes de iniciar |
| `provisa license apply <file>` | REQ-1139 | Instala um arquivo de licença offline |
| `provisa license status` | REQ-1139 | Mostra o ID da máquina e o estado de trial / licença |
| `provisa metadata export` | REQ-1072 | Publica o snapshot de metadados sob demanda |
| `provisa env list --org ORG` | REQ-1498 | Lista os ambientes da organização e seus builds |
| `provisa env show --org ORG --env ENV` | REQ-1498 | Mostra um ambiente |
| `provisa env create --org ORG --env ENV --data-mode MODE` | REQ-1942 | Cria um ambiente a partir de outro |
| `provisa env delete --org ORG --env ENV` | REQ-1498 | Aposenta um ambiente |
| `provisa env diff --org ORG --from A --into B` | REQ-1490 | Prévia de um merge (somente leitura) |
| `provisa env merge --org ORG --from A --into B` | REQ-1490 | Faz o merge de um ambiente em outro |
| `provisa env deploy --org ORG --env ENV --ref REF` | REQ-1496 | Implanta uma ref em um ambiente |
| `provisa env revert --org ORG --env ENV [--to BUILD]` | REQ-1543 | Volta um commit, ou implanta um build |
| `provisa env redo --org ORG --env ENV` | REQ-1543 | Avança de novo |
| `provisa env fetch --org ORG` | REQ-1541 | Busca os branches remotos no repositório Provisa |
| `provisa env push --org ORG --env ENV` | REQ-1546 | Envia o branch de um ambiente ao remoto |
| `provisa env pull --org ORG --env ENV` | REQ-1547 | Aplica o que o remoto tem |
| `provisa maintenance on` | REQ-1466 | Ativa o banner de manutenção |
| `provisa maintenance off` | REQ-1466 | Limpa o banner de manutenção |
| `provisa maintenance status` | REQ-1466 | Mostra o estado atual do banner |

## Veja também {: #see-also }

- [Environments](environments.md) — modelo de ambiente, ambientes protegidos, ciclo de vida de aprovação de deploy
- [Metadata Export](metadata-export.md) — provedores de catálogo, configuração, e o que o snapshot contém
- [Deployment](deployment.md) — nível de container e implantação em nuvem
