# Importando do Hasura

O Provisa consegue converter metadados existentes do Hasura em um `config.yaml` do Provisa, preservando tabelas rastreadas, relacionamentos, permissões, e esquemas remotos.

## Importação interativa (Admin → Import Hasura Config)

A superfície de admin roda os mesmos conversores, então uma importação não exige acesso a shell nem
ida e volta de arquivo de config. Exige a capacidade `org_settings`; a importação é aplicada na
organização em que a sessão está atuando.

1. **Upload.** Escolha um diretório de metadados do Hasura v2 zipado, um projeto DDN zipado, uma
   exportação de metadados consolidada (`.yaml`/`.json`, incluindo o envelope `{resource_version, metadata}`
   que a API de metadados retorna), ou um único `.hml`. Deixe o formato em *Detect automatically*,
   a menos que o upload seja ambíguo.
2. **Mapear domínios** (opcional). Cada par mapeia um esquema v2 ou um subgrafo DDN para um domínio
   Provisa; o que não for mapeado mantém o nome original.
3. **Converter e pré-visualizar.** O servidor converte e retorna contagens, avisos do conversor, e a
   configuração gerada. Nada é escrito nesta etapa.
4. **Revisar e editar.** A configuração é editável no local — detalhes de conexão, nomes de domínio,
   nomes de função. O que você aplica é o que é mostrado.
5. **Aplicar.** *Replace the existing semantic layer* exclui toda fonte, tabela, função e regra
   ausente da configuração; deixado desmarcado, a importação faz merge com o que a organização já tem.
   Aplicar carrega a configuração e reconstrói os esquemas da organização.

Endpoints: `POST /admin/import/hasura/preview` e `POST /admin/import/hasura/apply`.

---

## Hasura v2

### Exportar Metadados

Do seu console ou CLI Hasura:

```bash
hasura metadata export --output metadata.yaml
```

Ou use a API Hasura:

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

### Converter

O conversor v2 lê um **diretório** de metadados Hasura (o layout produzido por `hasura metadata export`, ou o layout plano `tables.yaml` / `actions.yaml`) e escreve uma config Provisa:

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Omita `-o` para escrever a config no stdout.

Flags:

| Flag | Propósito |
| ------ | --------- |
| `-o`, `--output` | Caminho de saída YAML (padrão: stdout) |
| `--source-overrides` | Arquivo YAML com sobreposições de conexão por fonte (host, porta, credenciais) |
| `--domain-map` | Mapeamentos de esquema para domínio como pares `SCHEMA=DOMAIN` |
| `--auth-env-file` | Arquivo `.env` com config de autenticação; converte JWT/JWK, segredo de admin, e mapa de claims |
| `--dry-run` | Analisa e valida sem escrever saída |

### O Que É Convertido

| Conceito Hasura | Equivalente Provisa |
| --------------- | ------------------- |
| Tabela rastreada | `tables[]` com `publish: true` |
| Relacionamento de objeto | `relationships[]` com `cardinality: many-to-one` |
| Relacionamento de array | `relationships[]` com `cardinality: one-to-many` |
| Permissão de select | Visibilidade de função + filtro RLS |
| Permissão de coluna | `visible_to` / `writable_by` |
| Permissão de insert/update/delete | Mutação `writable_by` + RLS |
| Esquema remoto | Registro de fonte `graphql_remote` |
| Campo computado | Entrada `functions[]` com `kind: query` |

### Limitações

- **Actions** convertem automaticamente: actions com handler HTTP se tornam mutações `webhooks[]`; actions com handler não-HTTP (banco de dados) se tornam um placeholder `functions[]` e emitem um aviso para revisar o handler
- **Event triggers** convertem para config `event_triggers` por tabela (operações, URL do webhook, política de retry) e emitem um aviso observando fidelidade limitada
- **Esquemas remotos** convertem para entradas de fonte `graphql_remote`
- **Funções SQL personalizadas** exigem revisão — casos simples convertem para entradas `functions[]`, casos complexos exigem trabalho manual
- **Cron triggers** convertem para entradas de config `scheduler`, preservando a expressão cron e a flag enabled

---

## Hasura DDN (v3)

### Localizar o projeto HML

O conversor DDN lê o **diretório** do projeto DDN de arquivos `.hml` diretamente — nenhum passo de build de supergrafo é exigido. O primeiro componente de diretório sob a raiz do projeto é tomado como o nome do subgrafo; arquivos sob `globals/` são atribuídos ao subgrafo `globals`.

### Converter

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

Omita `-o` para escrever a config no stdout.

Flags:

| Flag | Propósito |
| ------ | --------- |
| `-o`, `--output` | Caminho de saída YAML (padrão: stdout) |
| `--source-overrides` | Arquivo YAML com sobreposições de conexão por fonte |
| `--domain-map` | Mapeamentos de subgrafo para domínio como pares `SUBGRAPH=DOMAIN` |
| `--aggregates-output` | Caminho de saída para o sidecar de expressões de agregado (padrão: `<output>-aggregates.yaml`) |
| `--dry-run` | Analisa e valida sem escrever saída |

Metadados de `AggregateExpression` são preservados em um arquivo sidecar `*-aggregates.yaml`.

### O Que É Convertido

| Conceito DDN | Equivalente Provisa |
| ------------ | ------------------- |
| Modelo de subgrafo | `tables[]` sob uma fonte |
| Relacionamento | `relationships[]` |
| Regra de permissão | Filtro RLS |
| Command | Mutação de webhook ou view |
| Conector | Entrada de fonte com detalhes de conexão |

### Limitações

- **Conectores Lambda** (funções TypeScript/Python) exigem configuração manual de webhook
- **Plugins de ciclo de vida** não têm equivalente direto
- **Modos de autenticação DDN** mapeiam para provedores de autenticação Provisa mas caminhos de claim JWT podem precisar de ajuste

---

## Após a Importação

1. Revise o `config.yaml` gerado — preste atenção aos `warnings` do conversor
2. Verifique as credenciais de conexão (o conversor usa valores placeholder)
3. Inicie o Provisa e confirme que as tabelas aparecem no Explorer
4. Rode suas consultas GraphQL existentes — o esquema é compatível para padrões comuns
5. Envie consultas para aprovação via API de Administração ou UI antes de habilitar a governança de produção
