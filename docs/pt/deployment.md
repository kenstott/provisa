# Implantação

## Escolhendo um Caminho de Implantação

O Provisa suporta seis caminhos de implantação. Escolha com base no seu público e contexto operacional:

| Caminho | Artefato / Script | Melhor para |
| ------ | ------------------- | ---------- |
| **Desenvolvimento** | `start-ui.sh` | Desenvolvimento a partir do código-fonte, avaliação com dados de demonstração completos |
| **Instalador macOS** | `Provisa-<version>-macOS.dmg` | Estações de trabalho de desenvolvedor, avaliação |
| **Instalador Windows** | `Provisa-<version>-windows-x64.exe` | Estações de trabalho de desenvolvedor, avaliação |
| **AppImage Linux** | `Provisa.AppImage` | Servidores on-prem, VMs em nuvem, ambientes air-gapped |
| **VMs em Nuvem (AWS)** | `terraform/deploy.sh` | Implantação multi-nó em nuvem com balanceadores de carga |
| **Kubernetes** | `helm/provisa/` | Times que já operam K8s |

### VM vs Kubernetes

Ambos são de nível empresarial. O caminho VM/AppImage é mais simples: nenhum cluster para provisionar, nenhuma política de CNI ou RBAC para configurar, e o AppImage é inteiramente autocontido (REQ-223). Ele se encaixa naturalmente em ferramentas de gerenciamento de servidor existentes (Ansible, Puppet, agentes Datadog, forwarders Splunk, etc.).

Escolha Kubernetes apenas se seu time já opera um cluster K8s e quer que o Provisa participe desse modelo operacional (deploys contínuos, HPA, observabilidade unificada) (REQ-056). As capacidades são equivalentes — Kubernetes adiciona overhead operacional, não capacidade.

### Aquisição de imagem e varredura de segurança

Todos os caminhos de produção exigem obter os artefatos do Provisa antes que qualquer implantação possa rodar. "Air-gapped" se refere ao que acontece no momento da instalação na máquina alvo — os artefatos devem ser adquiridos primeiro.

**Instaladores macOS e Windows:** Baixe da [página de releases do GitHub](https://github.com/provisa/provisa/releases). Totalmente empacotado; nenhuma internet necessária após o download (REQ-227). Destinado a dev/avaliação, não produção — nenhum gate de varredura de imagem esperado.

**Caminho AppImage:** Baixe da [página de releases do GitHub](https://github.com/provisa/provisa/releases) e transfira para a máquina alvo. O AppImage empacota todas as imagens de componente como tarballs dentro de um sistema de arquivos squashfs (REQ-294) — a maioria dos scanners de registro não consegue inspecioná-las no local. Contate sua equipe de conta Provisa para digests de imagem de componente para verificar contra seu scanner independentemente.

**Caminho Terraform:** O AppImage deve ser enviado ao S3 antes de rodar `terraform/deploy.sh`. Nós EC2 o baixam na inicialização via função IAM — eles exigem acesso S3 de saída (direto ou via endpoint de gateway VPC). Aplique a mesma política de varredura do caminho AppImage.

**Caminho Helm / Kubernetes:** Imagens individuais devem ser enviadas a um registro que o cluster consiga alcançar. Este caminho é mais compatível com varredura baseada em registro (Prisma Cloud, Aqua, Trivy, AWS Inspector) — imagens são objetos de primeira classe que os scanners entendem nativamente. Para clusters air-gapped, espelhe imagens para um registro interno e sobreponha referências em `values.yaml` (REQ-294).

---

## Desenvolvimento (a partir do código-fonte)

### Recomendado: `start-ui.sh`

A forma mais fácil de rodar o Provisa a partir do código-fonte. Inicia toda a infraestrutura, a API de backend, e o servidor de desenvolvimento da UI em um único comando (REQ-055). Ctrl+C desliga tudo de forma limpa.

**Pré-requisitos:** Docker Desktop, Node.js, virtualenv Python em `.venv/`

```bash
./start-ui.sh
```

O que ele faz:

- Inicia `docker-compose.core.yml` + `docker-compose.dev.yml` (todos os serviços core + demo) e aguarda ficarem saudáveis (REQ-055)
- Semeia o Kafka com dados de demonstração
- Sincroniza dependências Python de `.venv/`
- Inicia a API de backend na porta 8001 (logs em `.logs/server.log`) (REQ-558)
- Inicia o servidor de desenvolvimento Vite UI na porta 3000 (REQ-559)
- Imprime URLs e aguarda; Ctrl+C para tudo e derruba o compose

```yaml
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**Opções:**

`--reset-volumes` — Roda `docker compose down -v` antes de iniciar, destruindo todos os volumes Docker (dados PostgreSQL, objetos MinIO, estado Redis, etc.) (REQ-170). Use quando quiser um ambiente completamente limpo — após uma mudança de esquema durante o desenvolvimento, ou quando o Docker travou e deixou volumes corrompidos. **Todos os dados serão perdidos.**

`--observability` — Adiciona instrumentação completa de rastreamento e métricas. Baixa o agente Java OpenTelemetry e corrige o `jvm.config` do Trino para carregá-lo, instrumenta o backend do Provisa com exportação OTLP, e inicia o coletor OTel, Prometheus, Tempo, e Grafana (`http://localhost:3100`) (REQ-330). A correção do `jvm.config` é revertida automaticamente no Ctrl+C.

### Passos manuais (somente backend, sem UI)

Se você só precisa da API:

1. Instale o [Docker Desktop](https://docs.docker.com/get-docker/)
2. Inicie os serviços core:

   ```bash
   docker compose -f docker-compose.core.yml up -d
   ```

3. Inicie a API:

   ```bash
   uvicorn main:app --reload --port 8001
   ```

4. Verifique: `curl http://localhost:8001/health`

### Stack completa (Provisa em container)

Para rodar a API como um container em vez de no host:

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### Serviços

**Core (`docker-compose.core.yml`) — sempre necessário:**

| Serviço | Porta | Propósito |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | Metadados de config + catálogo Iceberg (REQ-169) |
| PgBouncer | 6432 | Pooling de conexão (REQ-053) |
| Motor de federação | 8080 | Federação de consulta (REQ-028) |
| Redis | 6379 | Cache de resultado de consulta (REQ-371) |
| MinIO | 9000/9001 | Armazenamento de objeto compatível com S3 (REQ-029, REQ-171) |

**Demo (`docker-compose.dev.yml`) — opcional, incluído por `start-ui.sh`:**

| Serviço | Porta | Propósito |
| --------- | ------ | --------- |
| MongoDB | 27017 | Fonte NoSQL de demonstração |
| Kafka | 9092 | Fonte de streaming de demonstração |
| Schema Registry | 8081 | Gerenciamento de esquema Avro/Protobuf de demonstração |
| Debezium | — | Conector CDC de demonstração |
| Elasticsearch | 9200 | Fonte de busca de demonstração |
| Neo4j | 7474/7687 | Fonte de grafo de demonstração |
| Fuseki | 3030 | Triplestore SPARQL de demonstração |
| OpenTelemetry Collector | — | Coleta de rastreamento (com `--observability`) (REQ-302) |
| Prometheus | 9090 | Métricas (com `--observability`) (REQ-330) |
| Tempo | — | Armazenamento de rastreamento (com `--observability`) (REQ-330) |
| Grafana | 3100 | Painéis (com `--observability`) (REQ-330) |

### Backend de telemetria (`otlp2sql`)

A stack `--observability` acima (Collector → Tempo/Prometheus/Grafana) é um
caminho de telemetria. O outro é `otlp2sql` (`provisa.observability.otlp2sql`): um
receptor OTLP/HTTP que escreve traces, métricas, e logs em um banco de dados SQL
escolhido por uma URL SQLAlchemy, extraindo os atributos de span `provisa.*` na ingestão
para que nenhum job de compactação separado rode. Escritas são agrupadas em lote
(`OTLP2SQL_BATCH_MAX_ROWS`, padrão 1000; `OTLP2SQL_BATCH_MAX_SECS`, padrão 2s).

A telemetria tem seu próprio armazenamento, separado do banco de dados do control-plane. Selecione
o backend com `PROVISA_OPS_DB_URL`:

| `PROVISA_OPS_DB_URL` | Backend | Notas |
| --- | --- | --- |
| *(não definido)* | DuckDB dedicado sob `~/.provisa/telemetry/` | padrão; sem servidor, sem Docker |
| `clickhouse+native://user@host/otel` | ClickHouse | ingestão de alta taxa com merges automáticos em segundo plano |
| `postgresql+psycopg2://user@host/otel` | PostgreSQL | volume moderado |
| `trino://user@host:8080/otel` | Trino / Iceberg | tecnicamente funciona, **não recomendado** — veja abaixo |

**Sobre `trino://`:** o dialeto SQLAlchemy Trino emite DDL Trino válido e
`INSERT`s, então é tecnicamente viável como backend `otlp2sql`. Não é
recomendado para nada além de taxas de ingestão baixas. Cada flush de lote se torna um
`INSERT` distribuído do Trino mais um snapshot Iceberg, então telemetria de alta taxa
produz muitos arquivos e snapshots pequenos e ainda precisa de
`ALTER TABLE ... EXECUTE optimize` / `expire_snapshots` periódico — que o `otlp2sql`
não roda. Também coloca o motor de consulta no caminho crítico de ingestão.

Para telemetria de alto volume no Trino/Iceberg, use `otlp2parquet` em vez disso: ele
escreve parquet no armazenamento de objeto sem passar pelo Trino, e uma compactação Trino
programada mescla os arquivos brutos nas tabelas Iceberg ativas. Para um único
motor que lida tanto com ingestão de alta taxa quanto com compactação, prefira ClickHouse.

Aponte os exportadores OTLP da aplicação e do Trino (`OTEL_EXPORTER_OTLP_ENDPOINT`) para o
endpoint `otlp2sql`, e registre o domínio ops contra a mesma
`PROVISA_OPS_DB_URL` para que ele leia o que o receptor escreveu.

---

## Instalador macOS

Para estações de trabalho de desenvolvedor e avaliação. Totalmente air-gapped — nenhuma internet necessária após o download (REQ-227).

O instalador base é uma **instalação nativa**: motor de federação DuckDB + control plane SQLite + cache em memória (fakeredis), sem Docker, VM, Trino, Redis, ou MinIO (REQ-972, REQ-979). O motor de federação é uma escolha do assistente — DuckDB (nativo, padrão), Trino-on-Docker, ou um motor externo (REQ-973). A observabilidade é auto-telemetria sempre ativa visível na Administração; a stack Docker de coletor/Prometheus/Grafana é uma demonstração externa opcional, não um interruptor ligado/desligado (REQ-975). O pacote de dados de demonstração é opcional e desligado por padrão (REQ-978). Trino, a stack de observabilidade Docker, e a demonstração são add-ons pesados resolvidos local-first (diretório adjacente ao instalador, volumes montados, `~/Downloads`, depois release do GitHub), para que empresas possam pré-preparar tarballs para instalações air-gapped (REQ-977).

### Passos

1. Baixe `Provisa-<version>-macOS.dmg` da [página de releases do GitHub](https://github.com/provisa/provisa/releases)
2. Abra o DMG e arraste **Provisa.app** para `/Applications`
3. Dê duplo clique em **Provisa.app** — a configuração de primeiro lançamento roda uma vez; o assistente oferece as escolhas de motor, observabilidade, e demonstração acima (REQ-1007)
4. Abra o Terminal:

   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

   (REQ-224)

### Persistência de dados

Todos os dados são armazenados em `~/.provisa/` (REQ-224). Para remover tudo: `provisa uninstall`.

---

## Instalador Windows

Para estações de trabalho de desenvolvedor e avaliação. Totalmente air-gapped — nenhuma internet necessária após o download (REQ-227).

Assim como no macOS, o instalador base do Windows é um **nível nativo**: um runtime Python autônomo + wheel provisa + DuckDB/pg_duckdb + control plane SQLite, sem enviar Docker, sem VM, e sem imagens de container (REQ-979). O motor de federação (Trino), a stack de observabilidade, e o pacote de dados de demonstração são adicionados depois via instaladores em camadas separados, em ordem: o instalador Container (`Provisa-Container-<version>.exe`, que adiciona WSL2 + containerd + Trino), depois o instalador Obs (exige o nível container), depois o instalador Demo (exige Core + Obs). A orientação de primeiro lançamento explica como inicializar o motor de federação rodando o instalador Container (REQ-1005).

### Passos

1. Baixe `Provisa-<version>-windows-x64.exe` da [página de releases do GitHub](https://github.com/provisa/provisa/releases)
2. Execute o instalador — nenhum direito de administrador necessário; instala em `%LOCALAPPDATA%\Programs\Provisa\`
3. Abra **Provisa First Launch** no Menu Iniciar — a configuração nativa roda uma vez e imprime a orientação dos próximos passos para os add-ons em camadas (REQ-1005)
4. Abra um novo terminal:

   ```text
   provisa status
   provisa open
   ```

   (REQ-224)

### Persistência de dados

Todos os dados são armazenados em `%USERPROFILE%\.provisa\`.

---

## AppImage Linux — VM de Nó Único ou Multi-Nó

### O que é

`Provisa.AppImage` é um único executável autocontido empacotando (REQ-223, REQ-228):

- Um daemon Docker rootless (`dockerd-rootless.sh` + `rootlesskit`) — nenhum Docker de sistema ou root necessário
- Todos os tarballs de imagem de container (PostgreSQL, PgBouncer, MinIO, Redis, motor de federação, API Provisa) (REQ-294)
- O wrapper de CLI do Provisa e o script de configuração de primeiro lançamento

A imagem Provisa é pré-construída no momento do empacotamento — código-fonte Python nunca é incluído.

### Quando usar

- Bare metal ou VM on-premises (nó único ou multi-nó)
- VMs em nuvem sem um cluster K8s
- Ambientes air-gapped (REQ-294)
- Quando você quer operações mais simples do que Kubernetes

---

### Passos — Nó Único

1. Baixe `Provisa.AppImage` da [página de releases do GitHub](https://github.com/provisa/provisa/releases) e transfira para a máquina alvo
2. Torne-o executável:

   ```bash
   chmod +x Provisa.AppImage
   ```

3. Rode a configuração de primeiro lançamento:

   ```bash
   ./Provisa.AppImage
   ```

4. O assistente de configuração pergunta:
   - **Função** → selecione `primary`
   - **Orçamento de RAM** → quantidade de RAM a alocar (0 = toda disponível); determina a contagem de workers do Trino
   - **Hostname** → o endereço anunciado deste nó
   - **Porta da API** → padrão `8000` (REQ-560)
5. A configuração carrega todas as imagens de container (~2–5 minutos), escreve a config, e inicia os serviços
6. Verifique:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Passos — Multi-Nó (Primário)

Execute estes passos no nó primário primeiro. Secundários devem ser configurados após o primário estar rodando.

1. Baixe e transfira `Provisa.AppImage` para a máquina primária
2. Abra as portas de firewall necessárias (secundários se conectarão de entrada nestas):

   | Porta | Serviço |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | Coordenador do motor de federação |
   | 8000 | API Provisa |

3. Torne executável e rode:

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. O assistente de configuração pergunta:
   - **Função** → selecione `primary`
   - **Orçamento de RAM**, **hostname**, **porta da API** → responda como para nó único
5. Após a configuração terminar, anote o **IP privado** desta máquina — secundários precisarão dele
6. O assistente imprime um bloco upstream nginx — salve-o para sua configuração de balanceador de carga
7. Verifique:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Passos — Multi-Nó (Cada Secundário)

Repita estes passos em cada nó adicional após o primário estar rodando e alcançável.

1. Baixe e transfira `Provisa.AppImage` para a máquina secundária
2. Confirme que o secundário consegue alcançar o primário:

   ```bash
   curl http://<primary-ip>:8000/health
   ```

3. Torne executável e rode:

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. O assistente de configuração pergunta:
   - **Função** → selecione `secondary`
   - **IP do primário** → digite o IP do nó primário (conectividade é verificada ao vivo)
   - **Orçamento de RAM**, **hostname**, **porta da API** → responda como acima
5. A configuração carrega um conjunto de imagem reduzido (sem PostgreSQL, PgBouncer, MinIO, Redis — esses rodam somente no primário) (REQ-561), inicia a API Provisa e um worker do motor de federação
6. Verifique:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

7. Adicione este nó ao upstream do seu balanceador de carga

---

### Topologia primário / secundário

**O nó primário** roda todos os serviços singleton:

| Serviço | Por que singleton |
| --------- | --------------- |
| PostgreSQL | Esquema compartilhado, config da aplicação, modelo semântico |
| Redis | Cache de resultado de consulta compartilhado e estado de subscription (REQ-371) |
| MinIO | Armazenamento de objeto compartilhado para resultados de redirecionamento e snapshots de MV (REQ-029) |
| Coordenador do motor de federação | Todos os workers (primário + secundários) se registram aqui (REQ-028) |

**Nós secundários** rodam somente:

- API Provisa — sem estado; lê toda a config do PostgreSQL no primário na inicialização (REQ-057, REQ-562)
- Worker do motor de federação — se auto-registra com o coordenador no primário (REQ-028)

Todo o estado da aplicação flui através do PostgreSQL do primário. Nenhuma sincronização manual necessária. (REQ-562)

---

### Primeiro lançamento não interativo (automatizado)

Para Terraform, cloud-init, ou Ansible — passe flags em vez de responder prompts:

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

O modo não interativo instala uma unit systemd (`/etc/systemd/system/provisa.service`) para iniciar na inicialização. (REQ-563)

| Flag | Descrição |
| ------ | ------------- |
| `--non-interactive` | Pular todos os prompts; instalar unit systemd |
| `--role primary\|secondary` | Função do nó |
| `--primary-ip <ip>` | IP do nó primário (exigido para secundário) |
| `--ram-gb <n>` | RAM a alocar (0 = toda disponível) |

---

## Implantação de VM em Nuvem — Terraform (AWS)

Provisiona um cluster Provisa multi-nó completo na AWS — VPC, security groups, instâncias EC2, ALB, NLB — em um comando interativo. (REQ-564)

### Arquivos

| Arquivo | Propósito |
| ------ | --------- |
| `terraform/deploy.sh` | Wrapper interativo — coleta parâmetros, valida credenciais, escreve `terraform.tfvars`, roda apply |
| `terraform/aws/variables.tf` | Todas as definições de variável com padrões |
| `terraform/aws/main.tf` | VPC, subnets, security groups, IAM, EC2, ALB, NLB |
| `terraform/aws/outputs.tf` | URLs de endpoint e IPs de nó |

### Passos

1. Baixe `Provisa.AppImage` da [página de releases do GitHub](https://github.com/provisa/provisa/releases)

2. Envie-o para um bucket S3 na sua conta AWS:

   ```bash
   aws s3 cp Provisa.AppImage s3://<your-bucket>/releases/Provisa.AppImage
   ```

3. Garanta que as credenciais AWS estejam disponíveis no seu shell (qualquer uma):
   - Variáveis de ambiente: `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
   - Perfil nomeado: `export AWS_PROFILE=my-profile`
   - Sessão SSO ativa: `aws sso login`

4. (Opcional) Se você quer acesso SSH aos nós, crie um par de chaves EC2 na sua região alvo e anote o nome do par de chaves

5. Rode o wrapper de implantação:

   ```bash
   bash terraform/deploy.sh
   ```

6. Responda às perguntas do assistente (veja a tabela de referência abaixo). O script verifica se o AppImage existe no S3 antes de prosseguir e aborta se não existir

7. Revise o resumo da implantação e confirme

8. O Terraform provisiona toda a infraestrutura (~5–10 minutos). Após o apply, o script imprime:

   ```text
   api_endpoint      = "http://<alb-dns>:8000"
   flight_endpoint   = "<nlb-dns>:8815"
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

   (REQ-564, REQ-143)

9. (Opcional) Aponte registros DNS para os nomes DNS do ALB e NLB

10. Verifique:

    ```bash
    curl http://<api_endpoint>/health
    ```

### Perguntas do assistente

| Pergunta | Padrão | Notas |
| ---------- | --------- | ------- |
| Provedor de nuvem | — | Somente AWS hoje |
| Credenciais AWS | — | Verifica sessão ativa primeiro |
| Região | `us-east-1` | |
| Contagem de nós | `2` | 1 = somente primário, sem LB; 2+ = primário + secundários + ALB/NLB |
| Tipo de instância | `m7i.2xlarge` | Veja o guia de dimensionamento abaixo |
| Tamanho do volume raiz | `100 GB` | Por nó |
| Orçamento de RAM | `0` (toda RAM) | Determina a contagem de workers do Trino por nó |
| Bucket S3 | — | Verificado ao vivo antes de prosseguir |
| Chave S3 | `releases/Provisa.AppImage` | |
| Acesso SSH | Não | Exige nome de par de chaves existente + CIDR admin |
| CIDR da VPC | `10.0.0.0/16` | |

### Guia de dimensionamento de instância

| Tipo | vCPU | RAM | Workers Trino/nó | Caso de uso |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 GB | 0 | Dev / conjuntos de dados pequenos |
| `m7i.2xlarge` | 8 | 32 GB | 1 | Produção pequena |
| `m7i.4xlarge` | 16 | 64 GB | 2 | Produção média |
| `m7i.8xlarge` | 32 | 128 GB | 4 | Produção grande |

Todos os nós contribuem workers para um coordenador no primário (REQ-028). Um cluster `m7i.4xlarge` de 3 nós produz 6 workers Trino no total.

### O que é provisionado

- VPC com duas subnets públicas em duas zonas de disponibilidade (REQ-564)
- Security groups: grupo LB (ingresso público em 8000/8815), grupo de nós (LB → nós, intra-cluster, SSH opcional)
- Função IAM + perfil de instância com S3 GetObject no bucket do AppImage
- Instância EC2 primária — roda primeiro lançamento em modo `--non-interactive --role primary`
- Instâncias EC2 secundárias (node_count − 1) — rodam primeiro lançamento em modo `--non-interactive --role secondary --primary-ip <primary private IP>`; dependem do primário completar primeiro
- ALB na porta 8000 — API HTTP, health-checks `/health` (REQ-560)
- NLB na porta 8815 — Arrow Flight / gRPC (REQ-143)
- Ambos os LBs se anexam a todos os nós

### Lista de verificação de pré-requisitos

- [ ] Permissões IAM: EC2 completo, ELB completo, VPC completo, criação de função IAM, S3 GetObject no bucket do AppImage
- [ ] `Provisa.AppImage` enviado ao S3
- [ ] Nós EC2 têm acesso S3 de saída (internet direta ou endpoint de gateway VPC S3)
- [ ] Par de chaves EC2 existe na região alvo (se SSH for necessário)
- [ ] Terraform ≥ 1.5 instalado localmente
- [ ] Registros DNS planejados para ALB / NLB (opcional mas recomendado)
- [ ] Certificado ACM pronto se HTTPS for exigido (não incluído no Terraform base)

### Segredos

Nenhum segredo é embutido no Terraform. O AppImage gera credenciais durante o primeiro lançamento e as escreve em `~/.provisa/config.yaml` em cada nó (REQ-563). Para produção, recupere o token de administrador do nó primário após a implantação:

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes / Helm

### Quando usar

Seu time já opera um cluster Kubernetes e quer que o Provisa participe desse modelo operacional (REQ-056). Se você está avaliando o Provisa ou implantando on-premises sem um cluster existente, o caminho AppImage é mais simples.

Nota: o AppImage do Provisa não consegue rodar dentro de um pod Kubernetes — ele exige FUSE e um daemon Docker rootless, que não estão disponíveis em perfis de segurança de pod padrão.

### Passos

1. Confirme o acesso ao cluster:

   ```bash
   kubectl cluster-info
   ```

2. Baixe e espelhe imagens para seu registro interno (exigido para ambientes air-gapped ou varridos; pule se estiver puxando de registros públicos diretamente) (REQ-294):

   | Imagem | Usada para |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | API Provisa |
   | `trinodb/trino:480` | Coordenador + workers do motor de federação (REQ-169) |
   | `postgres:16` | PostgreSQL no cluster (se `postgresql.enabled`) (REQ-169) |
   | `edoburu/pgbouncer:latest` | PgBouncer no cluster (se `pgbouncer.enabled`) (REQ-053) |
   | `redis:7.2` | Redis no cluster (se `redis.enabled` e sem `redis.host`) (REQ-371) |
   | `minio/minio:latest` | MinIO no cluster (se `minio.enabled`) (REQ-029) |

   Para ambientes com varredura de registro:
   - Envie cada imagem ao seu registro de staging
   - Rode seu scanner (Prisma Cloud, Aqua, Trivy, AWS Inspector) e obtenha aprovação
   - Promova ao seu registro interno de produção

3. Decida antes de instalar:
   - **PostgreSQL** — no cluster (`postgresql.enabled: true`) ou gerenciado externo (`postgresql.host`)? Externo recomendado para produção
   - **Redis** — no cluster ou externo (`redis.host`)? Mude a senha padrão (`redis.password`)
   - **MinIO / S3** — MinIO no cluster ou S3 nativo? Para AWS, use S3 com uma função IAM
   - **Segredos** — passe via `--set` para avaliação; use External Secrets ou Vault Agent para produção

4. Instale o chart:

   ```bash
   helm install provisa helm/provisa/ \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   Se usando um registro interno, adicione sobreposições de imagem:

   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

5. Verifique se os pods estão rodando:

   ```bash
   kubectl get pods -n provisa
   ```

6. Verifique a API:

   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

7. (Opcional) Habilite ingress para acesso externo — defina `ingress.enabled: true` e configure seu controlador de ingress

### Lista de verificação de pré-requisitos

- [ ] Kubernetes 1.26+, Helm 3.12+
- [ ] Classe de armazenamento suportando PVCs `ReadWriteOnce` (para serviços com estado no cluster)
- [ ] Imagens disponíveis para o cluster (registro público ou interno)
- [ ] Endpoint + credenciais PostgreSQL (se externo)
- [ ] Endpoint + credenciais Redis (se externo)
- [ ] Bucket S3 + credenciais ou função IAM
- [ ] Token de administrador escolhido
- [ ] Controlador de ingress configurado (se acesso externo necessário)

### Valores principais

| Valor | Padrão | Descrição |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | Réplicas da API Provisa (sem estado) (REQ-057) |
| `config.pgHost` | `postgres` | Host PostgreSQL |
| `config.pgPassword` | | Senha PostgreSQL |
| `config.adminToken` | | Token bearer da API de administração |
| `redis.enabled` | `true` | Implantar StatefulSet Redis no cluster (REQ-371) |
| `redis.host` | `""` | Definir para usar Redis externo |
| `redis.port` | `6379` | |
| `redis.password` | `"provisa"` | Mude isto |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | Implantar motor de federação (REQ-028) |
| `trino.workers` | `2` | Réplicas de worker do motor de federação (REQ-056) |
| `postgresql.enabled` | `true` | Implantar PostgreSQL no cluster (REQ-169) |
| `postgresql.host` | `""` | Definir para usar PostgreSQL externo |
| `minio.enabled` | `true` | Implantar MinIO no cluster (REQ-029) |
| `s3.endpoint` | | URL de endpoint compatível com S3 |
| `s3.bucket` | `provisa-results` | Bucket para redirecionamento de resultado grande (REQ-029, REQ-137) |
| `ingress.enabled` | `false` | Habilitar ingress |

### Escalonamento

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

Workers do motor de federação escalam independentemente — mais workers aumentam a vazão e a capacidade de consulta concorrente (REQ-056). (REQ-057)

### Atualizando config

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## Alta Disponibilidade e Recuperação

O Provisa aplica um modelo de recuperação em duas camadas em todos os modos de implantação (REQ-703):

- **Camada 1 — erros transitórios.** Operações de leitura tentam novamente por até 30 segundos em erros transitórios usando backoff exponencial com jitter completo. Ajuste o orçamento com `PROVISA_RETRY_BUDGET_SECS`. Operações de escrita nunca são retentadas internamente, e erros de memória nunca são retentáveis.
- **Camada 2 — falha de componente.** Um observador interno do motor detecta e reinicia componentes de software falhados em 2–3 minutos.

Falhas em nível de máquina e cluster continuam sendo responsabilidade do operador — provisione nós redundantes e um balanceador de carga (caminhos Terraform e Helm acima) para tolerância à perda de nó.

## Dependências do Motor de Federação

Os motores de federação de warehouse exigem pacotes Python e componentes de nível de sistema além da instalação padrão do Provisa. Todos os pacotes Python listados aqui são declarados em `pyproject.toml` e instalados como parte do `pip install provisa` ou `pip install -e .` padrão [tool-verified: `pyproject.toml` lines 44–52].

Os pacotes Python vêm com a instalação padrão do Provisa — nenhum extra opcional exigido para qualquer motor de warehouse. Os itens de nível de sistema (driver ODBC, CLIs de nuvem, chaves de conta de serviço) devem ser instalados separadamente.

### Pacotes Python (já nas dependências core)

[tool-verified: `pyproject.toml` lines 41–52]

| Pacote | Motor | Propósito |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | Conexão de SQL warehouse; Arrow Cloud Fetch (REQ-987) |
| `snowflake-connector-python[pandas]` | Snowflake | Conexão + `fetch_arrow_table` Arrow-nativo (REQ-988) |
| `google-cloud-bigquery` | BigQuery | Execução de consulta |
| `google-cloud-bigquery-storage` | BigQuery | Storage Read API para leituras Arrow-nativas |
| `google-cloud-storage` | BigQuery | Staging GCS para links de tabela externa |
| `pyodbc` | Fabric, Synapse | Conexão ODBC para endpoints T-SQL |
| `azure-identity` | Fabric, Synapse | Token Azure AD via `DefaultAzureCredential` |
| `clickhouse-connect` | ClickHouse | Leituras colunares HTTP |
| `protobuf>=6.33.5,<7` | BigQuery, gRPC | Pin de compatibilidade — `google-cloud-*` e OTel compartilham um runtime protobuf; `<7` os mantém alinhados |
| `grpcio-status<1.82` | gRPC | Alinha com o pin `protobuf<7` |

### Requisitos de nível de sistema

Esses não são pacotes Python — devem ser instalados no host ou container que roda o Provisa.

**Microsoft Fabric e Azure Synapse (ODBC)**

`pyodbc` se conecta através do Microsoft ODBC Driver for SQL Server (`msodbcsql18`). O driver deve ser instalado no host — não via pip. [tool-verified: `mssql_warehouse_runtime.py` line 84 `"ODBC Driver 18 for SQL Server"` default]

macOS:

```bash
brew install microsoft/mssql-release/msodbcsql18
```

Linux (Ubuntu/Debian):

```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

O Provisa detecta o driver automaticamente. Para sobrepor o nome do driver (para instalações não padrão), defina:

```bash
export PROVISA_MSSQL_ODBC_DRIVER="ODBC Driver 17 for SQL Server"
```

**Autenticação Azure AD (Fabric e Synapse)**

Ambos os motores autenticam via `azure.identity.DefaultAzureCredential` [tool-verified: `mssql_warehouse_runtime.py:79`, `fabric_shortcuts.py:46`]. `DefaultAzureCredential` verifica fontes de credencial em ordem: variáveis de ambiente, identidade de carga de trabalho, identidade gerenciada, VS Code, `az login`, e outras.

Para desenvolvimento local, `az login` é o caminho mais simples:

```bash
az login
```

Para produção, use identidade gerenciada (em VMs Azure ou AKS) — nenhum gerenciamento de credencial necessário. Para autenticação por service-principal, defina:

```bash
export AZURE_TENANT_ID=<tenant>
export AZURE_CLIENT_ID=<app-id>
export AZURE_CLIENT_SECRET=<secret>
```

**BigQuery (conta de serviço)**

`google-cloud-bigquery` usa Application Default Credentials. Para desenvolvimento local, aponte para um arquivo de chave de conta de serviço:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

Para produção em GCP (Cloud Run, GKE com Workload Identity, Compute Engine), a biblioteca detecta a conta de serviço anexada automaticamente — nenhuma variável de ambiente necessária.

A conta de serviço precisa de:

- `roles/bigquery.dataViewer` — ler dados
- `roles/bigquery.jobUser` — rodar consultas
- `roles/bigquery.dataEditor` — criar tabelas externas (para ATTACH)
- `roles/storage.objectViewer` — ler objetos GCS para tabelas externas

**Databricks (certificado CA em ambientes de proxy de desenvolvimento)**

Se o Provisa roda atrás de um proxy que intercepta TLS (Charles, mitmproxy, proxies corporativos), o conector SQL do Databricks pode rejeitar o certificado do proxy. Passe um bundle CA personalizado:

```bash
export REQUESTS_CA_BUNDLE=/path/to/your/proxy-ca.pem
```

O conector Databricks herda isso do `requests` — nenhuma variável de ambiente específica do Databricks é necessária.

### Lista de verificação por motor

**Databricks** (REQ-987)

- [ ] `databricks-sql-connector` instalado (padrão)
- [ ] URL de motor com `http_path`: `databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] Token de acesso pessoal ou token de service principal
- [ ] `REQUESTS_CA_BUNDLE` definido se atrás de proxy que intercepta TLS

**Snowflake** (REQ-988)

- [ ] `snowflake-connector-python[pandas]` instalado (padrão)
- [ ] URL de motor: `snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `account` em `PROVISA_ENGINE_URL` ou `federation_hints`

**BigQuery** (REQ-989)

- [ ] `google-cloud-bigquery`, `google-cloud-bigquery-storage`, `google-cloud-storage` instalados (padrão)
- [ ] `GOOGLE_APPLICATION_CREDENTIALS` definido (dev) ou identidade de carga de trabalho configurada (prod)
- [ ] `GOOGLE_CLOUD_PROJECT` definido se o projeto não puder ser inferido da conta de serviço
- [ ] Conta de serviço tem funções BigQuery Data Viewer + Job User

**Microsoft Fabric** (REQ-989)

- [ ] `pyodbc` + `azure-identity` instalados (padrão)
- [ ] Driver de sistema `msodbcsql18` instalado
- [ ] `FABRIC_SQL_SERVER` e `FABRIC_DATABASE` definidos
- [ ] Autenticação Azure AD: `az login` (dev) ou identidade gerenciada / service principal (prod)
- [ ] `FABRIC_WORKSPACE_ID` definido se usando links de armazenamento de objeto externo

**Azure Synapse** (REQ-989)

- [ ] Mesmos requisitos Python + sistema que Fabric
- [ ] `SYNAPSE_SQL_SERVER` e `SYNAPSE_DATABASE` definidos
- [ ] Mesma configuração de autenticação Azure AD que Fabric

**ClickHouse** (REQ-986)

- [ ] `clickhouse-connect` instalado (padrão)
- [ ] URL de motor: `clickhouse+http://user:pass@host:8123/database`
- [ ] `secure: "true"` em `federation_hints` para TLS (porta 8443)

---

## Variáveis de Ambiente

| Variável | Padrão | Propósito |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | Senha PostgreSQL |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Caminho para o arquivo de config (REQ-528) |
| `PROVISA_REDIRECT_ENABLED` | `false` | Habilitar redirecionamento de resultado grande para S3 (REQ-029, REQ-137) |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Limite de contagem de linha para redirecionamento (REQ-029) |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Bucket S3 (REQ-029) |
| `PROVISA_REDIRECT_ENDPOINT` | | URL de endpoint compatível com S3 (REQ-029) |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL de URL pré-assinada (segundos) (REQ-141) |
| `REDIS_HOST` | `localhost` | Host Redis |
| `REDIS_PORT` | `6379` | Porta Redis |
| `REDIS_PASSWORD` | | Senha Redis |
| `REDIS_TLS` | `false` | Habilitar TLS para Redis |
| `TRINO_HOST` | `localhost` | Host do coordenador do motor de federação Trino (REQ-028, REQ-054) |
| `TRINO_PORT` | `8080` | Porta HTTP do coordenador do motor de federação Trino (REQ-028, REQ-054) |
| `PROVISA_ENGINE` | `duckdb` | Chave do motor de federação ativo (REQ-989); sobrepõe a config persistida |
| `PROVISA_ENGINE_URL` | | URL de conexão para motores orientados por URL (Databricks, Snowflake, ClickHouse, BigQuery, Fabric, Synapse, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | | Sobreposição de URL do armazenamento de materialização; padrão é o próprio armazenamento do motor |
| `PROVISA_MSSQL_ODBC_DRIVER` | `ODBC Driver 18 for SQL Server` | Nome do driver ODBC para Fabric / Synapse |
| `GOOGLE_APPLICATION_CREDENTIALS` | | Caminho para o JSON de chave de conta de serviço GCP (BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | | ID do projeto GCP (BigQuery; inferido da conta de serviço quando não definido) |
| `FABRIC_SQL_SERVER` | | Hostname do endpoint de análise SQL do Microsoft Fabric |
| `FABRIC_DATABASE` | | Nome do banco de dados Fabric |
| `FABRIC_WORKSPACE_ID` | | GUID do workspace Fabric (exigido para atalhos de armazenamento de objeto externo) |
| `SYNAPSE_SQL_SERVER` | | Hostname do pool SQL dedicado ou serverless do Azure Synapse |
| `SYNAPSE_DATABASE` | | Nome do banco de dados Synapse |
| `AZURE_TENANT_ID` | | Tenant Azure AD (autenticação service-principal para Fabric/Synapse) |
| `AZURE_CLIENT_ID` | | ID do cliente da aplicação Azure AD |
| `AZURE_CLIENT_SECRET` | | Segredo do cliente da aplicação Azure AD |
| `REQUESTS_CA_BUNDLE` | | Caminho do bundle CA personalizado (conector Databricks, proxy TLS de dev) |

---

## Comandos CLI

```bash
provisa start              # Start all services
provisa stop               # Stop all services
provisa restart            # Restart
provisa status             # Show service health
provisa open               # Open the UI in the browser
provisa logs               # Tail service logs
provisa export             # Print current config as YAML to stdout
provisa export FILE        # Write current config as YAML to FILE
provisa import FILE        # Replace running config with YAML from FILE
```

(REQ-224, REQ-164)

### Fluxo de promoção de config (dev → test → prod)

Todas as configurações específicas de ambiente (strings de conexão, segredos, portas) pertencem a variáveis de ambiente ou gerenciadores de segredo — não à config exportada. O YAML exportado captura seu modelo semântico: fontes, domínios, funções, views. (REQ-164)

```bash
# On dev — export after making changes in the UI
provisa export > config.yaml
git add config.yaml && git commit -m "chore: update semantic model"
git push

# On test/prod — pull and import
git pull
provisa import config.yaml
```


Veja também: [Ambientes](environments.md) explica como gerenciar cópias nomeadas e isoladas por esquema do seu modelo governado.
