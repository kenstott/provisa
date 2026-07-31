# Início Rápido para Desenvolvedores

Para avaliar o Provisa sem compilar a partir do código-fonte, veja o [Início Rápido](index.md) — baixe o instalador para macOS, Windows ou Linux e execute `provisa start`. (REQ-223, REQ-224, REQ-227)

Este guia é para rodar o Provisa **a partir do repositório** — desenvolvimento ativo, depuração ou contribuição.

---

## Pré-requisitos

- **Docker Desktop** (em execução)
- **Python 3.12+**
- **Node.js 20+**
- **Git**

---

## 1. Clonar e configurar

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` cria `.venv/`, instala todas as dependências Python via `pip install -e ".[dev]"`, e configura os git hooks em `.githooks/`. [tool-verified: setup.sh lines 5–9]

---

## 2. Iniciar tudo

```bash
./start-ui.sh
```

Quando terminar de iniciar, você verá:

```yaml
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**O que ele inicia:** [tool-verified: start-ui.sh]

- Serviços centrais do Docker Compose (`docker-compose.core.yml`) — PostgreSQL, PgBouncer, Trino, Redis (REQ-055)
- Overlay de desenvolvimento do Docker Compose (`docker-compose.dev.yml`) — MinIO, Kafka, MongoDB, Elasticsearch, Neo4j, Fuseki, Debezium, Schema Registry (REQ-055)
- API de backend na porta 8001 (hot-reload em mudanças em `provisa/` e `config/`) (REQ-618)
- Servidor de desenvolvimento Vite da UI na porta 3000 (HMR)
- Rastreamento OpenTelemetry e Grafana em `http://localhost:3100`. A stack de observabilidade é um perfil docker-compose `observability` opcional (OTel Collector, Prometheus, Tempo, Grafana), não ativado por padrão no nível da plataforma; `start-ui.sh` o habilita como conveniência de script de desenvolvimento, a menos que você passe `--no-observability`. (REQ-302, REQ-303, REQ-330)

**Ctrl+C** para tudo — backend, UI e todos os serviços Docker — e reverte quaisquer patches de configuração. (REQ-619)

**Ctrl+R** reinicia apenas o backend (útil após uma mudança de configuração que o hot-reload não capta). (REQ-619)

### Opções

`--no-observability` — Desabilita o rastreamento distribuído. Por padrão, `start-ui.sh` baixa o agente Java do OpenTelemetry se ainda não estiver presente, aplica patch no `jvm.config` do Trino para carregá-lo, e inicia o coletor OTel, Prometheus, Tempo e Grafana. Passe `--no-observability` para pular tudo isso. O patch do `jvm.config` é revertido no Ctrl+C. [tool-verified: start-ui.sh lines 15, 67–82] (REQ-330)

`--seed-data` — Popula o Kafka com dados de demonstração depois que os serviços Docker estão saudáveis. Não executado por padrão. [tool-verified: start-ui.sh lines 14, 173–178]

`--keep-docker` — Deixa os serviços do Docker Compose rodando após o Ctrl+C em vez de chamar `docker compose down`. [tool-verified: start-ui.sh lines 16, 301–306] (REQ-619)

`--reset-volumes` — Apaga todos os volumes Docker e reinicia com um estado limpo. Útil para recuperação de falha do Docker. [tool-verified: start-ui.sh line 19] (REQ-170)

`--demo` — Inicia fontes de dados de demonstração adicionais (esquema pet-store PostgreSQL, mock OpenAPI petstore, SQLite, e um GraphQL remoto). Popula usuários e pedidos do petstore automaticamente. [tool-verified: start-ui.sh lines 17, 55–171]

`--idp=basic|firebase` — Habilita um provedor de identidade para autenticação. Sem essa flag, o backend roda sem provedor de autenticação e todas as requisições são tratadas como `admin`. [tool-verified: start-ui.sh line 18; provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 57–68] (REQ-120, REQ-124)

---

## 3. Conectar uma fonte de dados

O Provisa lê a configuração de `config/`. Adicione um arquivo de fonte — por exemplo `config/sources/my-db.yaml`:

```yaml
sources:
  - id: my-pg
    type: postgresql
    host: localhost
    port: 5432
    database: mydb
    username: myuser
    password: ${MY_DB_PASSWORD}
    tables:
      - id: orders
        publish: true
        columns:
          - name: id
          - name: amount
          - name: region
          - name: customer_id
```

Defina a variável de ambiente e o backend a captará no próximo reload:

```bash
export MY_DB_PASSWORD=secret
```

Veja [docs/configuration.md](configuration.md) para a referência YAML completa e todos os tipos de fonte suportados.

---

## 4. Execute sua primeira consulta

```bash
# GraphQL
curl -s -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}' | jq

# SQL — use the /data/sql endpoint
curl -s -X POST http://localhost:8001/data/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT id, amount, region FROM orders LIMIT 5"}' | jq
```

Nenhuma autenticação é exigida quando não há uma seção `auth` em `config/provisa.yaml` (o padrão em desenvolvimento). A função padrão é `admin`. [tool-verified: provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 56–68] (REQ-120, REQ-267)

---

## 5. Abra a UI

Abra `http://localhost:3000` em um navegador.

A barra de navegação tem quatro menus de nível superior: [tool-verified: provisa-ui/src/components/NavBar.tsx lines 39–80]

- **Explore** — Explorador de Esquema (`/schema`), editor GraphQL (`/query`), editor Cypher (`/graph`), editor SQL (`/sql`)
- **Model** — Views e Commands
- **Security** — Políticas de segurança em nível de linha e mascaramento de coluna (REQ-038, REQ-041)
- **Admin** — Visão geral, domínios, cache, tarefas programadas, saúde do sistema, observabilidade, usuários, organizações, funções

A API GraphQL de administração está em `http://localhost:8001/admin/graphql`. [tool-verified: provisa/api/app.py line 3389] (REQ-620)

---

## Solução de problemas

**Backend não inicia** — verifique `.logs/server.log`. A causa mais comum é uma variável de ambiente ausente ou um conflito de porta na 8001. [tool-verified: start-ui.sh line 202] (REQ-618)

**Serviços Docker não saudáveis** — execute `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps` para ver qual serviço está travado. O motor de federação leva ~30 segundos na primeira inicialização. (REQ-055)

**Conflito de porta na 3000 ou 8001** — `start-ui.sh` mata processos obsoletos nessas portas antes de iniciar. Se outra coisa possui a porta, pare-a manualmente primeiro. [tool-verified: start-ui.sh lines 197–199] (REQ-619)

**Início limpo** — pare o script, então execute `./start-ui.sh --reset-volumes` para apagar todos os volumes e reiniciar. [tool-verified: start-ui.sh line 19] (REQ-170)

---

## Próximos passos

| Objetivo | Doc |
| ------ | ----- |
| Referência completa de configuração YAML | [configuration.md](configuration.md) |
| Segurança em nível de linha, mascaramento de coluna, autenticação | [security.md](security.md) |
| Todos os tipos de fonte suportados | [sources.md](sources.md) |
| Subscriptions em tempo real | [subscriptions.md](subscriptions.md) |
| JDBC, ferramentas de BI, Arrow Flight, Apollo Federation | [integrations.md](integrations.md) |
| Cliente Python | [python-client.md](python-client.md) |
| Implantação em produção | [deployment.md](deployment.md) |
