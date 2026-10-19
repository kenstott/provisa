# Regiões {: #regions }

O Provisa oferece duas primitivas para posicionar dados: uma **região** e a **atribuição de região**, ou seja, criar uma região e atribuir uma tabela a ela. Nenhuma das duas é, por si só, uma política. O uso que você faz delas se divide em três grupos:

- **Desempenho.** A latência até quem lê, quão recente é a cópia, e a adequação do motor: onde fica o seu data center, que tipo de armazenamento ele lê, quanta CPU e memória ele tem.
- **Custo.** Onde o trabalho roda e sobre o quê: hardware próprio ou nuvem, um motor grande ou um pequeno, e o que custa mover dados entre lugares (egress).
- **Soberania.** Quem controla os dados e para onde eles podem ir: as regras de residência da lei (onde dados pessoais podem ser processados e onde podem ser armazenados) e as linhas de controle próprias de uma organização, como uma unidade de negócio ou uma subsidiária que mantém seus dados e sua infraestrutura sob o próprio controle.

A mecânica é a mesma para os três. Só mudam as regras que você escreve por cima.

## Uma região é um motor com seus armazenamentos em um lugar {: #a-region-is-an-engine-and-its-stores-in-a-place }

Uma região é um lugar onde o Provisa trabalha e guarda o que o trabalho deixa. Cada região tem seu próprio motor e seus próprios armazenamentos.

| Armazenamento | O que guarda |
| --- | --- |
| `engine` | O motor de federação que executa instruções nesta região |
| `replicas` | Réplicas das tabelas atribuídas aqui e das tabelas sem região que esta região copia |
| `views` | As cópias de visões materializadas desta região |
| `cache` | Respostas em cache |
| `state` | Estado de construção e de atualização |
| `record` | O registro de cada requisição atendida aqui |

Um único modelo governa todas as regiões. Fontes, tabelas, relacionamentos, funções, regras de linha e de coluna, visões e configurações de frescor são declaradas uma vez, em um armazenamento de modelo, e todas as regiões servem esse mesmo modelo, de modo que as definições não podem divergir entre regiões. Uma mudança chega a cada região pelo mesmo carimbo, e cada região então converge para ela.

A plataforma declara as regiões físicas em que seus nós rodam, com o endereço em que cada região responde. O modelo de cada organização escolhe quais dessas regiões usa e nomeia seu próprio armazenamento para cada papel em cada região. Um nó inicia em uma região com `provisa run --region <id>` e atende essa região para cada organização que a escolhe. Com regiões declaradas, um nó iniciado sem `--region` se recusa a iniciar e lista as regiões que poderia atender.

```yaml
platform:
  regions:
    - { id: eu, address: "https://eu.provisa.example" }
    - { id: us, address: "https://us.provisa.example" }

stores:
  - { id: eu_engine, url: "${secret:eu-trino-url}", kind: trino }
  - { id: us_engine, url: "${secret:us-trino-url}", kind: trino }
  - { id: eu_data,   url: "${secret:eu-pg-url}" }
  - { id: us_data,   url: "${secret:us-pg-url}" }
  - { id: eu_cache,  url: "${secret:eu-redis-url}" }
  - { id: us_cache,  url: "${secret:us-redis-url}" }

regions:
  - { id: eu, engine: eu_engine, replicas: eu_data, views: eu_data, cache: eu_cache, state: eu_data, record: eu_data }
  - { id: us, engine: us_engine, replicas: us_data, views: us_data, cache: us_cache, state: us_data, record: us_data }
```

Ao ser carregada, a configuração é recusada, com o motivo nomeado, se as regiões não se encaixam. Uma região precisa nomear um armazenamento declarado para cada papel. Seus `replicas` e `views` precisam ser o mesmo armazenamento, e seu armazenamento de motor precisa nomear um `kind` de motor. Com mais de uma região de plataforma, um arquivo de motor embutido é recusado, porque as outras regiões não conseguem alcançá-lo.

Várias regiões podem compartilhar uma instância de banco de dados, um Redis ou um bucket de armazenamento de objetos. Tudo o que uma região guarda leva a região no nome, então seus armazenamentos não colidem.

Uma implantação que não declara regiões tem uma região implícita e não mostra região em lugar nenhum. Não há campo de região nem opção `--region`, e uma configuração que nomeia uma região é recusada.

## Atribuição de região: uma tabela nomeia sua região de origem {: #region-homing-a-table-names-its-home }

Uma tabela pode nomear uma das regiões da sua organização como região de origem. Essa é a única região que constrói e atualiza sua réplica, e a única que pode mantê-la quente. Todas as outras regiões leem a tabela a partir da réplica da região de origem, no local, no endereço que o modelo dá. As regras de linha e de coluna de quem lê entram nessa instrução, então a filtragem acontece na região de origem e só atravessa o que quem lê pode ver.

```yaml
tables:
  - source_id: eu-shop
    schema_name: public
    table_name: customers
    region: eu
```
Algumas consequências:

- Nenhuma outra região lê a fonte da tabela; elas leem só a réplica da região de origem. Uma fonte que você replica para protegê-la de carga continua protegida.
- Se a réplica não foi construída ou a região de origem não está acessível, a requisição é recusada, nomeando a região de origem. A fonte não é lida ao vivo em seu lugar.
- Uma tabela atribuída que não é replicada, com `replicate` Never ou uma configuração que ainda não pediu uma cópia, é lida ao vivo da sua fonte por todas as regiões, como qualquer tabela não replicada.
- A contagem quente de uma tabela é só o tráfego da sua região de origem.
- Uma visão materializada também pode nomear uma região. Ela é então construída só ali, e todas as outras regiões leem essa cópia, exatamente como para uma tabela atribuída.

Uma região que lê a réplica de outra precisa de um motor capaz de lê-la: `duckdb`, `motherduck`, `pg`, `trino` ou `trino-byo`. O armazenamento `replicas` da região de origem precisa ser um servidor PostgreSQL. A configuração nomeia ambos quando não se encaixam.

Uma fonte também pode nomear uma região, mas essa região só preenche o formulário quando você registra uma nova tabela da fonte. Ela não decide nada sobre onde os dados ficam, e mudá-la depois não move nenhuma tabela.

## Tabelas sem região são copiadas onde são lidas {: #unhomed-tables-are-copied-where-they-are-read }

Uma tabela que não nomeia região pode ser copiada em todas as regiões. Quando sua configuração `replicate` pede uma cópia, a região a que quem lê se conectou constrói uma no próprio armazenamento. Uma visão materializada que não nomeia região é construída em todas as regiões, cada cópia no armazenamento de visões daquela região.

Acrescentar uma região significa, portanto, mais cópias e mais trabalho de atualização. Nunca significa mais governança. Um modelo governa todas as cópias, e as mesmas regras de linha e de coluna valem em todas as regiões. A construção de uma visão em uma região é governada como o administrador da organização agindo naquela região, então a cópia de cada região contém o que pode ser guardado ali.

## Três motivos para usar regiões {: #three-reasons-to-use-regions }

### Desempenho {: #performance }

Regiões usadas só por desempenho não declaram regras entre regiões. As tabelas, as funções e as regras são as que você escreveria para uma única região, e a atribuição de região decide onde o trabalho acontece:

- **Latência.** Quem lê se conecta à região próxima, cujo motor, cache e cópias são locais para essa pessoa.
- **Frescor.** Uma tabela atribuída onde ela muda é atualizada ali, perto da sua fonte.
- **Adequação do motor.** Cada região roda seu próprio motor, então você pode ajustar um motor a um lugar: o data center em que ele fica, o armazenamento que ele lê, e a CPU e a memória que ele tem.

**Exemplo: duas regiões idênticas.** Declare `east` e `west` em dois data centers, cada uma com o mesmo tipo de motor e os mesmos tipos de armazenamento, e não atribua nenhuma tabela. Cada região atende quem lê mais perto dela: constrói as próprias cópias do que leem e guarda as respostas em cache. As duas podem ficar atrás de um balanceador de carga e dividir a carga. Há um único cronograma de atualização, o do modelo, e as duas regiões começam a atualizar suas cópias por ele, mas seus motores podem não terminar no mesmo tempo. Em um dado momento, então, não há garantia de que as duas estejam no mesmo estado, e quem lê e passa de uma região para a outra pode ver dados de frescor diferente. O Provisa não move um cliente para a outra região quando uma para; para onde um cliente vai é decisão do balanceador de carga.

### Custo {: #cost }

Regiões de custo também não declaram regras entre regiões. Elas levam o trabalho para onde é mais barato rodá-lo:

- **Hardware próprio e nuvem.** Mantenha uma região no local, em hardware que você já tem, para as tabelas e os leitores que estão ali, e rode outra região na nuvem para o resto.
- **Tamanho do motor.** Rode um motor grande em uma região para trabalho pesado ocasional e um pequeno em outra para as leituras do dia a dia. Quem lê escolhe a região pelo endereço.
- **Egress.** Quem lê em outras regiões recebe só as linhas filtradas da réplica de uma tabela atribuída, e uma tabela grande atribuída a uma região não é copiada em todas as regiões.

### Soberania {: #sovereignty }

Regiões usadas por soberania declaram, sim, regras entre regiões, seja o motivo a lei ou a organização: uma subsidiária que atribui suas tabelas à própria região as faz processar e armazenar na própria infraestrutura, e decide por meio dessas regras o que o resto do grupo lê. São regras comuns de linha e de coluna, escritas sobre atributos que dizem onde está quem lê, ou o nó que faz o trabalho:

- `current_setting('provisa.region')` é a região do nó que responde à requisição, quando a plataforma declara regiões. Um filtro de linha escrito sobre ele mantém as linhas de uma região naquela região.
- O atributo `region` de um membro faz com que ele detenha as funções derivadas `region` e `region__<value>`, então uma entrada de máscara ou de concessão pode nomear `analyst+region__eu`. O trabalho em segundo plano do próprio nó carrega sua região da mesma forma. Veja [Funções: concedidas, derivadas e meta](security.md#roles-granted-derived-and-meta).

Junto com a atribuição, essas regras decidem o que atravessa uma fronteira. Os dados pessoais de uma tabela atribuída são armazenados e processados na sua região de origem, e quem lê em outro lugar recebe só o que suas regras permitem, filtrado na origem. Uma visão sem região construída em cada região contém o que um administrador daquela região pode ver, então a cópia de uma região pode incluir linhas que a cópia de outra região deixa de fora.

## Quem pode atribuir uma tabela {: #who-may-home-a-table }

Definir, mudar ou remover a região de uma tabela, de uma fonte ou de uma visão materializada move dados através de uma fronteira. Exige o direito `data_residency` além de ser dono do objeto pelo seu domínio. A concessão nomeia os valores que cobre: as regiões da organização e "sem região". Mover um objeto entre duas regiões exige uma concessão que cubra as duas. Um salvamento fora da concessão é recusado, nomeando o valor não coberto.

O direito existe só quando a plataforma declara regiões. A propriedade do domínio continua decidindo quem pode editar todo o resto do objeto. Uma região não confere direito próprio; uma organização que quer que as tabelas de uma região sejam administradas pelas pessoas daquela região dá a elas domínios próprios.

Uma tabela ou visão também pode ser posta em **rascunho**: fora de serviço e oferecida a nenhum leitor. Uma tabela registrada pela administração começa como rascunho. Seu responsável a pré-visualiza só nos nós da região que ela nomeia (uma que não nomeia nenhuma, onde o responsável está), e a cópia que essa pré-visualização grava é mantida nessa região para a liberação da tabela. Uma cópia que outra região tenha dela, de antes de sua região ser definida ou mudada, é retirada e removida como numa tabela publicada. Uma mudança entre regiões pode passar pelo rascunho, de modo que a tabela não é servida em lugar nenhum no intervalo: o lado da primeira região põe em rascunho, e o titular da segunda região assume o objeto definindo a sua região.

Veja também a [demo de duas regiões](regions-demo.md).
