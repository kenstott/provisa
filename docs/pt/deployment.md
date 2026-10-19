# Implantação {: #deployment }

## Escolhendo um Caminho de Implantação {: #choosing-a-deployment-path }

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

Ambos são de nível empresarial. O caminho VM/AppImage é mais simples: nenhum cluster para provisionar, nenhuma política de CNI ou RBAC para configurar, e o AppImage é inteiramente autocontido. Ele se encaixa naturalmente em ferramentas de gerenciamento de servidor existentes (Ansible, Puppet, agentes Datadog, forwarders Splunk, etc.).

Escolha Kubernetes só se sua equipe já opera um cluster K8s e quer que o Provisa se encaixe nesse jeito de operar (deploys graduais, HPA, observabilidade unificada). Os dois oferecem as mesmas capacidades; o Kubernetes acrescenta carga operacional.

### Aquisição de imagem e varredura de segurança {: #image-acquisition-and-security-scanning }

Todo caminho de produção precisa dos artefatos do Provisa antes que qualquer deployment possa rodar. "Air-gapped" descreve o que acontece na hora da instalação na máquina de destino, então você obtém os artefatos antes.

**Instaladores para macOS e Windows:** Baixe-os na [página de releases do GitHub](https://github.com/provisa/provisa/releases). Eles vêm completos e não precisam de internet depois do download. São feitos para desenvolvimento e avaliação, então não se espera uma barreira de varredura de imagens.

**Caminho AppImage:** Baixe-o na [página de releases do GitHub](https://github.com/provisa/provisa/releases) e copie-o para a máquina de destino. O AppImage empacota cada imagem de componente como um tarball dentro de um sistema de arquivos squashfs, que a maioria dos scanners de registro não consegue inspecionar no lugar. Peça à sua equipe de conta do Provisa os digests das imagens de componentes para conferi-los você mesmo no seu scanner.

**Caminho Terraform:** Envie o AppImage para o S3 antes de rodar `terraform/deploy.sh`. Os nós EC2 o baixam na inicialização pelo papel IAM, então precisam de acesso de saída ao S3, direto ou por um endpoint de gateway da VPC. Aplique a mesma política de varredura do caminho AppImage.

**Caminho Helm / Kubernetes:** Envie as imagens individuais para um registro que o cluster alcance. Esse caminho é o que melhor combina com varredura baseada em registro (Prisma Cloud, Aqua, Trivy, AWS Inspector), porque as imagens são objetos que os scanners já entendem. Para um cluster isolado, espelhe as imagens num registro interno e sobrescreva as referências em `values.yaml`.

---

## Desenvolvimento (a partir do código-fonte) {: #development-from-source }

### Recomendado: `start-ui.sh` {: #recommended-start-uish }

A forma mais fácil de rodar o Provisa a partir do código-fonte. Inicia toda a infraestrutura, a API de backend, e o servidor de desenvolvimento da UI em um único comando. Ctrl+C desliga tudo de forma limpa.

**Pré-requisitos:** Docker Desktop, Node.js, virtualenv Python em `.venv/`

```bash
./start-ui.sh
```

O que ele faz:

- Inicia `docker-compose.core.yml` + `docker-compose.dev.yml` (todos os serviços core + demo) e aguarda ficarem saudáveis
- Semeia o Kafka com dados de demonstração
- Sincroniza dependências Python de `.venv/`
- Inicia a API de backend na porta 8001 (logs em `.logs/server.log`)
- Inicia o servidor de desenvolvimento Vite UI na porta 3000
- Imprime URLs e aguarda; Ctrl+C para tudo e derruba o compose

```yaml
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**Opções:**

`--reset-volumes` — Roda `docker compose down -v` antes de iniciar, destruindo todos os volumes Docker (dados PostgreSQL, objetos MinIO, estado Redis, etc.). Use quando quiser um ambiente completamente limpo: após uma mudança de esquema durante o desenvolvimento, ou quando o Docker travou e deixou volumes corrompidos. **Todos os dados serão perdidos.**

`--observability` — Adiciona instrumentação completa de rastreamento e métricas. Baixa o agente Java OpenTelemetry e corrige o `jvm.config` do Trino para carregá-lo, instrumenta o backend do Provisa com exportação OTLP, e inicia o coletor OTel, Prometheus, Tempo, e Grafana (`http://localhost:3100`). A correção do `jvm.config` é revertida automaticamente no Ctrl+C.

### Passos manuais (somente backend, sem UI) {: #manual-steps-backend-only-no-ui }

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

### Stack completa (Provisa em container) {: #full-stack-provisa-in-container }

Para rodar a API como um container em vez de no host:

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### Serviços {: #services }

**Core (`docker-compose.core.yml`) — sempre necessário:**

| Serviço | Porta | Propósito |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | Metadados de config + catálogo Iceberg |
| PgBouncer | 6432 | Pooling de conexão |
| Motor de federação | 8080 | Federação de consulta |
| Redis | 6379 | Cache de resultado de consulta |
| MinIO | 9000/9001 | Armazenamento de objeto compatível com S3 |

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
| OpenTelemetry Collector | — | Coleta de rastreamento (com `--observability`) |
| Prometheus | 9090 | Métricas (com `--observability`) |
| Tempo | — | Armazenamento de rastreamento (com `--observability`) |
| Grafana | 3100 | Painéis (com `--observability`) |

Os stores, o motor de federação e esses serviços de telemetria publicam em `PROVISA_STORE_BIND_IP` (`127.0.0.1` se não definido), com senhas geradas para a implantação. Veja [Fortalecimento da implantação](security.md#deployment-hardening).

### Backend de telemetria (`otlp2sql`) {: #telemetry-backend-otlp2sql }

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

**Com `trino://`:** o dialeto Trino do SQLAlchemy emite DDL e `INSERT` válidos do Trino, então pode servir de backend do `otlp2sql`, mas só o recomendamos para taxas de ingestão baixas. Cada descarga de lote vira um `INSERT` distribuído do Trino mais um snapshot do Iceberg, então telemetria em alta taxa gera muitos arquivos e snapshots pequenos e ainda precisa de um `ALTER TABLE ... EXECUTE optimize` / `expire_snapshots` periódico, que o `otlp2sql` não roda. Também coloca o motor de consultas no caminho crítico da ingestão.

Para telemetria de alto volume no Trino/Iceberg, use `otlp2parquet` em vez disso: ele
escreve parquet no armazenamento de objeto sem passar pelo Trino, e uma compactação Trino
programada mescla os arquivos brutos nas tabelas Iceberg ativas. Para um único
motor que lida tanto com ingestão de alta taxa quanto com compactação, prefira ClickHouse.

Aponte os exportadores OTLP da aplicação e do Trino (`OTEL_EXPORTER_OTLP_ENDPOINT`) para o
endpoint `otlp2sql`, e registre o domínio ops contra a mesma
`PROVISA_OPS_DB_URL` para que ele leia o que o receptor escreveu.

---

## Instalador macOS {: #macos-installer }

Para estações de trabalho de desenvolvimento e avaliação. Totalmente isolado; não precisa de internet depois do download.

O instalador base é uma **instalação nativa**: o motor de federação DuckDB, um plano de controle PostgreSQL embutido (pgserver) e um cache em memória (fakeredis), sem Docker, VM, Trino, Redis ou MinIO. O motor de federação é escolhido no assistente: DuckDB (nativo, o padrão), Trino no Docker ou um motor externo. A observabilidade é autotelemetria sempre ligada e visível em Admin; a stack Docker de collector, Prometheus e Grafana é uma demonstração opcional à parte e não a liga nem desliga. O pacote de dados de demonstração é opcional e vem desligado. Trino, a stack de observabilidade no Docker e a demo são complementos pesados. Eles são procurados primeiro localmente (o diretório ao lado do instalador, volumes montados, `~/Downloads`) e só depois baixados da release do GitHub, para que empresas possam deixar os tarballs preparados para instalações isoladas.

### Passos {: #steps }

1. Baixe `Provisa-<version>-macOS.dmg` da [página de releases do GitHub](https://github.com/provisa/provisa/releases)
2. Abra o DMG e arraste **Provisa.app** para `/Applications`
3. Dê duplo clique em **Provisa.app**. A configuração de primeiro lançamento roda uma vez, e o assistente oferece as escolhas de motor, observabilidade, e demonstração acima
4. Abra o Terminal:

   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

### Persistência de dados {: #data-persistence }

Todos os dados são armazenados em `~/.provisa/`. Para remover tudo: `provisa uninstall`.

---

## Instalador Windows {: #windows-installer }

Para estações de trabalho de desenvolvimento e avaliação. Totalmente isolado; não precisa de internet depois do download.

Assim como no macOS, o instalador base do Windows é um **nível nativo**: um runtime Python autônomo + wheel provisa + DuckDB/pg_duckdb + control plane PostgreSQL embarcado (pgserver), sem enviar Docker, sem VM, e sem imagens de container. O motor de federação (Trino), a stack de observabilidade, e o pacote de dados de demonstração são adicionados depois via instaladores em camadas separados, em ordem: o instalador Container (`Provisa-Container-<version>.exe`, que adiciona WSL2 + containerd + Trino), depois o instalador Obs (exige o nível container), depois o instalador Demo (exige Core + Obs). A orientação de primeiro lançamento explica como inicializar o motor de federação rodando o instalador Container.

### Passos {: #steps_1 }

1. Baixe `Provisa-<version>-windows-x64.exe` da [página de releases do GitHub](https://github.com/provisa/provisa/releases)
2. Execute o instalador. Ele não exige direitos de administrador e instala em `%LOCALAPPDATA%\Programs\Provisa\`
3. Abra **Provisa First Launch** no Menu Iniciar. A configuração nativa roda uma vez e imprime a orientação dos próximos passos para os add-ons em camadas
4. Abra um novo terminal:

   ```text
   provisa status
   provisa open
   ```

### Persistência de dados {: #data-persistence_1 }

Todos os dados são armazenados em `%USERPROFILE%\.provisa\`.

---

## AppImage Linux — VM de Nó Único ou Multi-Nó {: #linux-appimage-single-or-multi-node-vm }

### O que é {: #what-it-is }

`Provisa.AppImage` é um único executável autocontido empacotando:

- Um daemon Docker rootless (`dockerd-rootless.sh` + `rootlesskit`), então não é preciso Docker de sistema nem root
- Todos os tarballs de imagem de container (PostgreSQL, PgBouncer, MinIO, Redis, motor de federação, API Provisa)
- O wrapper de CLI do Provisa e o script de configuração de primeiro lançamento

A imagem do Provisa é construída no empacotamento, e o código-fonte Python nunca é incluído.

### Quando usar {: #when-to-use }

- Bare metal ou VM on-premises (nó único ou multi-nó)
- VMs em nuvem sem um cluster K8s
- Ambientes air-gapped
- Quando você quer operações mais simples do que Kubernetes

---

### Passos — Nó Único {: #steps-single-node }

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
   - **Porta da API** → padrão `8000`
5. A configuração carrega todas as imagens de container (~2–5 minutos), escreve a config, e inicia os serviços
6. Verifique:

   ```bash
   provisa status
   curl -k https://localhost:8000/health   # -k: the node generates a self-signed certificate unless PROVISA_TLS_CERT/KEY supply one
   ```

---

### Passos — Multi-Nó (Primário) {: #steps-multi-node-primary }

Execute estes passos no nó primário primeiro. Secundários devem ser configurados após o primário estar rodando.

1. Baixe e transfira `Provisa.AppImage` para a máquina primária
2. Abra as portas de firewall necessárias apenas para os secundários (eles se conectam de entrada nestas):

   | Porta | Serviço |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | Coordenador do motor de federação |
   | 8000 | API Provisa |

3. Torne executável e rode com os stores publicados no endereço privado do nó, onde os secundários se conectam a eles. Um nó sem secundários os publica apenas em `127.0.0.1`:

   ```bash
   chmod +x Provisa.AppImage
   PROVISA_STORE_BIND_IP=<this node's private IP> ./Provisa.AppImage
   ```

4. O assistente de configuração pergunta:
   - **Função** → selecione `primary`
   - **Orçamento de RAM**, **hostname**, **porta da API** → responda como para nó único
5. Após a configuração terminar, anote o **IP privado** desta máquina. Os secundários precisam dele e das credenciais dos stores que o primário gerou: `~/.provisa/store-credentials.env` (`PROVISA_PG_PASSWORD`, `PROVISA_REDIS_PASSWORD`, `PROVISA_MINIO_ROOT_PASSWORD`, `PROVISA_GRAFANA_ADMIN_PASSWORD`, `PROVISA_TRINO_SHARED_SECRET`). Copie esse arquivo para cada secundário por um canal seguro
6. O assistente imprime um bloco upstream nginx; salve-o para sua configuração de balanceador de carga
7. Verifique:

   ```bash
   provisa status
   curl -k https://localhost:8000/health   # -k: the node generates a self-signed certificate unless PROVISA_TLS_CERT/KEY supply one
   ```

---

### Passos — Multi-Nó (Cada Secundário) {: #steps-multi-node-each-secondary }

Repita estes passos em cada nó adicional após o primário estar rodando e alcançável.

1. Baixe e transfira `Provisa.AppImage` para a máquina secundária
2. Confirme que o secundário consegue alcançar o primário:

   ```bash
   curl -k https://<primary-ip>:8000/health
   ```

3. Torne executável e rode com as credenciais dos stores do primário no ambiente (o arquivo copiado do primário). Um secundário sem elas não inicia:

   ```bash
   chmod +x Provisa.AppImage
   set -a; . ./store-credentials.env; set +a
   ./Provisa.AppImage
   ```

4. O assistente de configuração pergunta:
   - **Função** → selecione `secondary`
   - **IP do primário** → digite o IP do nó primário (conectividade é verificada ao vivo)
   - **Orçamento de RAM**, **hostname**, **porta da API** → responda como acima
5. A configuração carrega um conjunto de imagem reduzido (sem PostgreSQL, PgBouncer, MinIO e Redis, que rodam só no primário), inicia a API Provisa e um worker do motor de federação
6. Verifique:

   ```bash
   provisa status
   curl -k https://localhost:8000/health
   ```

7. Adicione este nó ao upstream do seu balanceador de carga

---

### Topologia primário / secundário {: #primary-secondary-topology }

**O nó primário** roda todos os serviços singleton:

| Serviço | Por que singleton |
| --------- | --------------- |
| PostgreSQL | Esquema compartilhado, config da aplicação, modelo semântico |
| Redis | Cache de resultado de consulta compartilhado e estado de subscription |
| MinIO | Armazenamento de objeto compartilhado para resultados de redirecionamento e snapshots de MV |
| Coordenador do motor de federação | Todos os workers (primário + secundários) se registram aqui |

**Nós secundários** rodam somente:

- API Provisa — sem estado; lê toda a config do PostgreSQL no primário na inicialização
- Worker do motor de federação — se auto-registra com o coordenador no primário

Todo o estado da aplicação flui através do PostgreSQL do primário. Nenhuma sincronização manual necessária.

---

### Primeiro lançamento não interativo (automatizado) {: #non-interactive-automated-first-launch }

Para Terraform, cloud-init, ou Ansible, passe flags em vez de responder prompts:

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

O modo não interativo instala uma unit systemd (`/etc/systemd/system/provisa.service`) para iniciar na inicialização.

| Flag | Descrição |
| ------ | ------------- |
| `--non-interactive` | Pular todos os prompts; instalar unit systemd |
| `--role primary\|secondary` | Função do nó |
| `--primary-ip <ip>` | IP do nó primário (exigido para secundário) |
| `--ram-gb <n>` | RAM a alocar (0 = toda disponível) |

---

## Implantação de VM em Nuvem — Terraform (AWS) {: #cloud-vm-deployment-terraform-aws }

Um único comando interativo provisiona um cluster completo do Provisa com vários nós na AWS: VPC, grupos de segurança, instâncias EC2, ALB e NLB.

### Arquivos {: #files }

| Arquivo | Propósito |
| ------ | --------- |
| `terraform/deploy.sh` | Wrapper interativo — coleta parâmetros, valida credenciais, escreve `terraform.tfvars`, roda apply |
| `terraform/aws/variables.tf` | Todas as definições de variável com padrões |
| `terraform/aws/main.tf` | VPC, subnets, security groups, IAM, EC2, ALB, NLB |
| `terraform/aws/outputs.tf` | URLs de endpoint e IPs de nó |

### Passos {: #steps_2 }

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
   api_endpoint      = "https://<alb-dns>:8000"
   protocol_endpoints = { flight = "<alb-dns>:8815", ... }
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

9. (Opcional) Aponte registros DNS para os nomes DNS do ALB e NLB

10. Verifique:

    ```bash
    curl http://<api_endpoint>/health
    ```

### Perguntas do assistente {: #wizard-questions }

| Pergunta | Padrão | Notas |
| ---------- | --------- | ------- |
| Provedor de nuvem | — | Somente AWS |
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

### Guia de dimensionamento de instância {: #instance-sizing-guide }

| Tipo | vCPU | RAM | Workers Trino/nó | Caso de uso |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 GB | 0 | Dev / conjuntos de dados pequenos |
| `m7i.2xlarge` | 8 | 32 GB | 1 | Produção pequena |
| `m7i.4xlarge` | 16 | 64 GB | 2 | Produção média |
| `m7i.8xlarge` | 32 | 128 GB | 4 | Produção grande |

Todos os nós contribuem workers para um coordenador no primário. Um cluster `m7i.4xlarge` de 3 nós produz 6 workers Trino no total.

### O que é provisionado {: #what-gets-provisioned }

- VPC com duas subnets públicas em duas zonas de disponibilidade
- Security groups: grupo LB (ingresso público em 8000/8815), grupo de nós (LB → nós, intra-cluster, SSH opcional)
- Função IAM + perfil de instância com S3 GetObject no bucket do AppImage
- Instância EC2 primária — roda primeiro lançamento em modo `--non-interactive --role primary`
- Instâncias EC2 secundárias (node_count − 1) — rodam primeiro lançamento em modo `--non-interactive --role secondary --primary-ip <primary private IP>`; dependem do primário completar primeiro
- ALB na porta 8000 — API HTTP, health-checks `/health`
- NLB na porta 8815 — Arrow Flight / gRPC
- Ambos os LBs se anexam a todos os nós

### Lista de verificação de pré-requisitos {: #prerequisites-checklist }

- [ ] Permissões IAM: EC2 completo, ELB completo, VPC completo, criação de função IAM, S3 GetObject no bucket do AppImage
- [ ] `Provisa.AppImage` enviado ao S3
- [ ] Nós EC2 têm acesso S3 de saída (internet direta ou endpoint de gateway VPC S3)
- [ ] Par de chaves EC2 existe na região alvo (se SSH for necessário)
- [ ] Terraform ≥ 1.5 instalado localmente
- [ ] Registros DNS planejados para ALB / NLB (opcional mas recomendado)
- [ ] Certificado ACM pronto se HTTPS for exigido (não incluído no Terraform base)

### Segredos {: #secrets }

As senhas dos stores incluídos (PostgreSQL, Redis, MinIO, o admin do Grafana) e o segredo compartilhado do cluster Trino são gerados pelo Terraform (`random_password`) e entregues a cada nó, que as mantém em seu arquivo de ambiente do systemd, modo 600. Elas ficam no estado do Terraform, então proteja o estado como protegeria as senhas. Sem o Terraform, o launcher de cada nó as gera em `~/.provisa/store-credentials.env`. O AppImage gera as demais credenciais durante o primeiro lançamento e as escreve em `~/.provisa/config.yaml` em cada nó. Para produção, recupere o token de administrador do nó primário após a implantação:

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes / Helm

### Quando usar {: #when-to-use_1 }

Seu time já opera um cluster Kubernetes e quer que o Provisa participe desse modelo operacional. Se você está avaliando o Provisa ou implantando on-premises sem um cluster existente, o caminho AppImage é mais simples.

Observação: o AppImage do Provisa não roda dentro de um pod do Kubernetes. Ele precisa de FUSE e de um daemon Docker sem root, e os perfis de segurança de pod padrão não permitem nenhum dos dois.

### Passos {: #steps_3 }

1. Confirme o acesso ao cluster:

   ```bash
   kubectl cluster-info
   ```

2. Baixe e espelhe imagens para seu registro interno (exigido para ambientes air-gapped ou varridos; pule se estiver puxando de registros públicos diretamente):

   | Imagem | Usada para |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | API Provisa |
   | `trinodb/trino:480` | Coordenador + workers do motor de federação |
   | `postgres:16` | PostgreSQL no cluster (se `postgresql.enabled`) |
   | `edoburu/pgbouncer:latest` | PgBouncer no cluster (se `pgbouncer.enabled`) |
   | `redis:7.2` | Redis no cluster (se `redis.enabled` e sem `redis.host`) |
   | `minio/minio:latest` | MinIO no cluster (se `minio.enabled`) |

   Para ambientes com varredura de registro:
   - Envie cada imagem ao seu registro de staging
   - Rode seu scanner (Prisma Cloud, Aqua, Trivy, AWS Inspector) e obtenha aprovação
   - Promova ao seu registro interno de produção

3. Decida antes de instalar:
   - **PostgreSQL** — no cluster (`postgresql.enabled: true`) ou gerenciado externo (`postgresql.host`)? Externo recomendado para produção
   - **Redis** — no cluster ou externo (`redis.host`)?
   - **Credenciais dos stores** — crie o Secret que `credentials.existingSecret` nomeia, com as chaves `pg-password`, `redis-password`, `minio-access-key`, `minio-secret-key`, `grafana-admin-password` e `trino-shared-secret`. O chart não define nenhuma senha própria e não renderiza sem ele
   - **TLS** — ativo por padrão (`tls.enabled: true`): crie o Secret `kubernetes.io/tls` que `tls.secretName` nomeia, ou defina `tls.enabled: false`
   - **MinIO / S3** — MinIO no cluster ou S3 nativo? Para AWS, use S3 com uma função IAM
   - **Segredos** — passe via `--set` para avaliação; use External Secrets ou Vault Agent para produção
   - **Cache do coordenador Trino** — um volume de 1Gi solicitado por padrão e montado em `/data/trino/cache` (`trino.coordinator.cache.persistence`: `enabled`, `size`, `storageClass`). Os resultados de describe de uma fonte Salesforce ficam ali, para que um coordenador reiniciado não gaste de novo a cota de API da organização descrevendo-a. Desative-o só quando nenhuma fonte precisar dele

4. Dê à implantação sua chave mestra. A chave cifra o cofre de segredos; todo pod da API deve
   ter a mesma, e ela deve sobreviver a qualquer pod. O chart não renderiza sem ela.

   ```bash
   kubectl create namespace provisa
   kubectl create secret generic provisa-master-key --namespace provisa \
     --from-literal=master-key="$(openssl rand -base64 32)"
   ```

   Guarde uma cópia da chave fora do cluster. Um cofre gravado com uma chave perdida depois
   não pode ser aberto. A alternativa a um Secret é um volume de dados persistente
   (`encryption.dataVolume.enabled=true`), onde o produto guarda a chave que gera; veja
   `encryption` em `values.yaml`.

5. Instale o chart:

   ```bash
   helm install provisa helm/provisa/ \
     --set encryption.existingSecret=provisa-master-key \
     --set auth.provider=oidc \
     --set auth.oidc.issuerUrl=https://login.example.com/realms/main \
     --set auth.oidc.clientId=provisa \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   O chart não renderiza até que `auth.provider` seja escolhido: `oidc` (um emissor OpenID Connect
   corporativo), `saml` (`auth.saml.idpMetadataUrl` e `auth.saml.publicUrl`), `ldap` (`auth.ldap.*`),
   `local` (somente a conta de emergência, `auth.breakGlass.*`) ou `none` (sem autenticação,
   só para desenvolvimento). Toda credencial vem de um Secret do Kubernetes; `ldap`, `saml` e `local`
   também precisam de `auth.sessionSecret.existingSecret`, a chave que assina as sessões do navegador. Veja `auth`
   em `values.yaml`.

   O coordenador Trino executa as instruções de cada organização no seu próprio grupo de recursos,
   `global.tenant-<org>`, a partir do arquivo de grupos de recursos que o chart traz. Instruções que passam pelo
   proxy Arrow Flight compartilham um grupo, `global.flight`: o proxy abre sua própria conexão Trino
   e não consegue levar a organização.

   Se usando um registro interno, adicione sobreposições de imagem:

   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

6. Verifique se os pods estão rodando:

   ```bash
   kubectl get pods -n provisa
   ```

7. Verifique a API:

   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

8. (Opcional) Habilite ingress para acesso externo: defina `ingress.enabled: true` e configure seu controlador de ingress

### Lista de verificação de pré-requisitos {: #prerequisites-checklist_1 }

- [ ] Kubernetes 1.26+, Helm 3.12+
- [ ] Classe de armazenamento suportando PVCs `ReadWriteOnce` (para serviços com estado no cluster)
- [ ] Imagens disponíveis para o cluster (registro público ou interno)
- [ ] Endpoint + credenciais PostgreSQL (se externo)
- [ ] Endpoint + credenciais Redis (se externo)
- [ ] Bucket S3 + credenciais ou função IAM
- [ ] Token de administrador escolhido
- [ ] Controlador de ingress configurado (se acesso externo necessário)

### Valores principais {: #key-values }

| Valor | Padrão | Descrição |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | Réplicas da API Provisa (sem estado) |
| `config.pgHost` | `postgres` | Host PostgreSQL |
| `config.pgPassword` | | Senha PostgreSQL |
| `config.adminToken` | | Token bearer da API de administração |
| `redis.enabled` | `true` | Implantar StatefulSet Redis no cluster |
| `redis.host` | `""` | Definir para usar Redis externo |
| `redis.port` | `6379` | |
| `credentials.existingSecret` | | Secret com as senhas dos stores (obrigatório) |
| `tls.enabled` | `true` | Serve todo endpoint por TLS a partir de `tls.secretName` |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | Implantar motor de federação |
| `trino.workers` | `2` | Réplicas de worker do motor de federação |
| `trino.internalTls` | `false` | Criptografa o tráfego coordenador-worker com um certificado gerado pelo chart; torna consultas com muito shuffle de 10% a mais de 100% mais lentas |
| `postgresql.enabled` | `true` | Implantar PostgreSQL no cluster |
| `postgresql.host` | `""` | Definir para usar PostgreSQL externo |
| `minio.enabled` | `true` | Implantar MinIO no cluster |
| `s3.endpoint` | | URL de endpoint compatível com S3 |
| `s3.bucket` | `provisa-results` | Bucket para redirecionamento de resultado grande |
| `ingress.enabled` | `false` | Habilitar ingress |

### Escalonamento {: #scaling }

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

Os workers do motor de federação escalam de forma independente, e mais workers significam mais vazão e mais consultas ao mesmo tempo.

### Atualizando config {: #updating-config }

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## Alta Disponibilidade e Recuperação {: #high-availability-recovery }

O Provisa aplica um modelo de recuperação em duas camadas em todos os modos de implantação:

- **Camada 1 — erros transitórios.** Operações de leitura tentam novamente por até 30 segundos em erros transitórios usando backoff exponencial com jitter completo. Ajuste o orçamento com `PROVISA_RETRY_BUDGET_SECS`. Operações de escrita nunca são retentadas internamente, e erros de memória nunca são retentáveis.
- **Camada 2 — falha de componente.** Um observador interno do motor detecta e reinicia componentes de software falhados em 2–3 minutos.

Falhas de máquina e de cluster continuam sendo responsabilidade do operador. Para tolerar a perda de um nó, provisione nós redundantes e um balanceador de carga (os caminhos Terraform e Helm acima).

### Tabela de telemetria cujos metadados estão ausentes {: #telemetry-table-whose-metadata-is-missing }

No motor Trino, as tabelas de telemetria (`otel.signals.traces`, `metrics`, `logs`) são tabelas Iceberg. O registro de cada uma é uma linha no banco de dados do plano de controle, e seus metadados e dados são arquivos no bucket de telemetria. Quando o bucket é servido sem esses arquivos enquanto o banco de dados ainda mantém os registros, toda instrução sobre a tabela falha com `ICEBERG_MISSING_METADATA`. Isso acontece quando o disco do bucket ainda não estava montado quando o armazenamento de objetos iniciou, ou quando o bucket foi esvaziado ou substituído.

O Provisa não altera nada nesse caso. Cada execução de compactação registra um `ERROR` que nomeia a tabela e o local de metadados que o Trino não conseguiu ler (`otel.signals.traces is registered but its metadata cannot be read …`), ignora esse sinal e mantém seus arquivos brutos no bucket para uma execução posterior. Os demais sinais são compactados normalmente. A inicialização deixa a tabela fora de suas verificações de esquema da mesma forma.

Recupere-a manualmente, nesta ordem:

1. **Descubra se os metadados ainda existem.** O `ERROR` nomeia o local. Em um coordenador Terraform (GCP), verifique se o disco do armazenamento de objetos está montado (`findmnt /var/lib/provisa-object-store`) e se o contêiner do MinIO iniciou depois da montagem. Um MinIO iniciado antes serve o diretório vazio sob o ponto de montagem; reinicie-o quando o disco estiver montado e a tabela volta a ser lida com seu histórico intacto.
2. **Se o arquivo de metadados existe mas o registro aponta para outro lugar**, registre a tabela novamente no seu local:

   ```sql
   CALL otel.system.register_table(schema_name => 'signals', table_name => 'traces', table_location => 's3://<bucket>/warehouse/signals/<table directory>')
   ```

   Cancele primeiro o registro obsoleto se o nome estiver em uso (`CALL otel.system.unregister_table(schema_name => 'signals', table_name => 'traces')`).
3. **Se os arquivos do bucket foram realmente perdidos**, cancele o registro da tabela. A próxima execução de compactação a cria novamente, vazia. Os sinais que ela continha não podem ser lidos novamente.

Não execute `remove_orphan_files` em uma tabela que está sendo recuperada. Ele exclui arquivos que os metadados atuais não referenciam, e esses podem ser o histórico que você está restaurando.

## Dependências do Motor de Federação {: #federation-engine-dependencies }

Os motores de federação de warehouse precisam de pacotes Python e de componentes de sistema. Os pacotes Python estão declarados em `pyproject.toml` e vêm com o `pip install provisa` ou `pip install -e .` padrão, então nenhum motor de warehouse precisa de um extra opcional.

Os itens de sistema (o driver ODBC, as CLIs de nuvem, as chaves de conta de serviço) precisam ser instalados à parte.

### Pacotes Python (já nas dependências core) {: #python-packages-already-in-core-dependencies }

| Pacote | Motor | Propósito |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | Conexão de SQL warehouse; Arrow Cloud Fetch |
| `snowflake-connector-python[pandas]` | Snowflake | Conexão + `fetch_arrow_table` Arrow-nativo |
| `google-cloud-bigquery` | BigQuery | Execução de consulta |
| `google-cloud-bigquery-storage` | BigQuery | Storage Read API para leituras Arrow-nativas |
| `google-cloud-storage` | BigQuery | Staging GCS para links de tabela externa |
| `pyodbc` | Fabric, Synapse | Conexão ODBC para endpoints T-SQL |
| `azure-identity` | Fabric, Synapse | Token Azure AD via `DefaultAzureCredential` |
| `clickhouse-connect` | ClickHouse | Leituras colunares HTTP |
| `protobuf>=6.33.5,<7` | BigQuery, gRPC | Pin de compatibilidade — `google-cloud-*` e OTel compartilham um runtime protobuf; `<7` os mantém alinhados |
| `grpcio-status<1.82` | gRPC | Alinha com o pin `protobuf<7` |

### Requisitos de nível de sistema {: #system-level-requirements }

Não são pacotes Python. Instale-os no host ou contêiner que roda o Provisa.

**Microsoft Fabric e Azure Synapse (ODBC)**

O `pyodbc` se conecta pelo Microsoft ODBC Driver for SQL Server (`msodbcsql18`). Instale o driver no próprio host; o pip não consegue instalá-lo.

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

Ambos os motores autenticam via `azure.identity.DefaultAzureCredential`. `DefaultAzureCredential` verifica fontes de credencial em ordem: variáveis de ambiente, identidade de carga de trabalho, identidade gerenciada, VS Code, `az login`, e outras.

Para desenvolvimento local, `az login` é o caminho mais simples:

```bash
az login
```

Em produção, use identidade gerenciada (em VMs do Azure ou AKS), que dispensa gerenciar credenciais. Para autenticação por service principal, defina:

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

Em produção no GCP (Cloud Run, GKE com Workload Identity, Compute Engine), a biblioteca pega sozinha a conta de serviço anexada, sem variável de ambiente.

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

O conector do Databricks herda isso do `requests`, então não precisa de variável de ambiente específica do Databricks.

### Lista de verificação por motor {: #per-engine-checklist }

**Databricks**

- [ ] `databricks-sql-connector` instalado (padrão)
- [ ] URL de motor com `http_path`: `databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] Token de acesso pessoal ou token de service principal
- [ ] `REQUESTS_CA_BUNDLE` definido se atrás de proxy que intercepta TLS

**Snowflake**

- [ ] `snowflake-connector-python[pandas]` instalado (padrão)
- [ ] URL de motor: `snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `account` em `PROVISA_ENGINE_URL` ou `federation_hints`

**BigQuery**

- [ ] `google-cloud-bigquery`, `google-cloud-bigquery-storage`, `google-cloud-storage` instalados (padrão)
- [ ] `GOOGLE_APPLICATION_CREDENTIALS` definido (dev) ou identidade de carga de trabalho configurada (prod)
- [ ] `GOOGLE_CLOUD_PROJECT` definido se o projeto não puder ser inferido da conta de serviço
- [ ] Conta de serviço tem funções BigQuery Data Viewer + Job User

**Microsoft Fabric**

- [ ] `pyodbc` + `azure-identity` instalados (padrão)
- [ ] Driver de sistema `msodbcsql18` instalado
- [ ] `FABRIC_SQL_SERVER` e `FABRIC_DATABASE` definidos
- [ ] Autenticação Azure AD: `az login` (dev) ou identidade gerenciada / service principal (prod)
- [ ] `FABRIC_WORKSPACE_ID` definido se usando links de armazenamento de objeto externo

**Azure Synapse**

- [ ] Mesmos requisitos Python + sistema que Fabric
- [ ] `SYNAPSE_SQL_SERVER` e `SYNAPSE_DATABASE` definidos
- [ ] Mesma configuração de autenticação Azure AD que Fabric

**ClickHouse**

- [ ] `clickhouse-connect` instalado (padrão)
- [ ] URL de motor: `clickhouse+http://user:pass@host:8123/database`
- [ ] `secure: "true"` em `federation_hints` para TLS (porta 8443)

---

## Variáveis de Ambiente {: #environment-variables }

| Variável | Padrão | Propósito |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | Senha PostgreSQL |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Caminho para o arquivo de config |
| `PROVISA_REDIRECT_ENABLED` | `false` | Habilitar redirecionamento de resultado grande para S3 |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Limite de contagem de linha para redirecionamento |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Bucket S3 |
| `PROVISA_REDIRECT_ENDPOINT` | | URL de endpoint compatível com S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL de URL pré-assinada (segundos) |
| `REDIS_HOST` | `localhost` | Host Redis |
| `REDIS_PORT` | `6379` | Porta Redis |
| `REDIS_PASSWORD` | | Senha Redis |
| `REDIS_TLS` | `false` | Habilitar TLS para Redis |
| `TRINO_HOST` | `localhost` | Host do coordenador do motor de federação Trino |
| `TRINO_PORT` | `8080` | Porta HTTP do coordenador do motor de federação Trino |
| `PROVISA_ENGINE` | `duckdb` | Chave do motor de federação ativo; sobrepõe a config persistida |
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

## Comandos CLI {: #cli-commands }

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

### Fluxo de promoção de config (dev → test → prod) {: #config-promotion-workflow-dev-test-prod }

Toda configuração específica de ambiente (strings de conexão, segredos, portas) vai em variáveis de ambiente ou num gerenciador de segredos, fora da configuração exportada. O YAML exportado captura seu modelo semântico: fontes, domínios, papéis e views.

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
