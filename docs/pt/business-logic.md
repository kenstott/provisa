# Lógica de negócio no Provisa

## O problema {: #the-problem }

Toda organização tem padrões para a lógica de negócio, e a maioria das equipes os segue. Mas, com os anos, à medida que sistemas são construídos e substituídos e as equipes mudam, a lógica se espalha por jobs de ETL, dashboards, APIs e procedimentos armazenados. Cada peça faz sentido sozinha. Muitas vezes ninguém planejou o todo, e mais vezes ainda ninguém planejou o todo ao longo do tempo. O resultado é conhecido: a “receita líquida” é calculada de três jeitos em três lugares, e ninguém sabe dizer qual está certo.

Existem respostas do setor para partes disso. Uma camada de métricas, muitas vezes vendida como camada semântica, define a “receita líquida” uma vez para toda ferramenta ler. O [Open Data Product Standard](https://github.com/bitol-io/open-data-product-standard) (ODPS) descreve produtos de dados e o [Open Data Contract Standard](https://github.com/bitol-io/open-data-contract-standard) (ODCS) descreve contratos de dados. O Provisa [exporta os dois](metadata-export.md#data-products-as-odps-and-odcs). Todas são boas ideias e todas estão ganhando adoção, mas cada uma cobre uma parte. A pergunta maior é como elas se encaixam em dados controlados e governados como um único modelo coerente e componível. A lógica que deriva ou altera dados precisa da mesma governança que os próprios dados: quem pode usá-la, o que ela lê e escreve, e para onde vão seus resultados.

## O que é lógica de negócio {: #definition }

Lógica de negócio são as regras que criam, atualizam, excluem ou transformam dados.

A terminologia de negócio é outra coisa. Os nomes e descrições de domínios, tabelas e colunas, e os termos do [glossário](glossary.md), dizem o que os dados significam. A lógica de negócio diz o que se faz com eles.

## Três papéis {: #roles }

Toda regra cumpre um de três papéis, e cada papel pode ser escrito de três formas:

| Papel | SQL | Chamada externa | Código |
|---|---|---|---|
| **Conjunto de dados** | view, comando de instrução | API ou procedimento que devolve linhas | resolvedor de código `@table`, `@stream`; comando `python` ou `script` |
| **Valor** | expressão de view, métrica | — | comando `python` ou `script` |
| **Mudança** | `INSERT`, `UPDATE`, `DELETE`, comando de instrução | escrita em API, mutation, procedimento | `@command`; comando `python` ou `script` |

- **Chamada externa.** A API GraphQL, REST ou gRPC de outro sistema, ou o procedimento armazenado de um banco de dados, registrado como comando.
- **Tabelas como argumentos.** Um comando pode receber tabelas como argumentos, então um conjunto de dados pode ser construído a partir de outros.
- **Métrica.** Um agregado com nome, definido uma vez, como `net_revenue` dado por `SUM(orders.amount) - SUM(orders.refunds)`. Cada consulta o detalha pelas dimensões que pede, então o warehouse, o dashboard e a API leem o mesmo número.

## Efeitos colaterais ocultos {: #side-effects }

Um efeito colateral oculto é aquele que não se vê na definição da regra.

Toda regra tem nome e tipo, e é registrada, auditada e protegida. Um comando também pode exigir aprovação antes de rodar.

| | Efeitos colaterais possíveis | Limitados por |
|---|---|---|
| **SQL** | Nenhum | — |
| **Chamada externa** | Os próprios fluxos de trabalho, notificações e cascatas do outro sistema | Lista de saída permitida (HTTP, gRPC); credenciais guardadas como segredos (procedimentos, operações de fonte) |
| **Resolvedor de código** | Chamadas a serviços e redes externos; resultados que variam; custo | Código fixado, impressão digital do host fixada, sandbox, medição |
| **Código no host** | Lê tudo o que o servidor pode ler (memória, credenciais, arquivos, rede); pode esgotar o servidor ou derrubá-lo | `commands.allow_host_code`, desligado por padrão |

Uma chamada externa é tipada pelo esquema do próprio sistema, ou pelos argumentos e colunas de retorno que você declara ao registrar o comando.

O código de um resolvedor de código é fixado por versão de pacote, commit do Git ou hash de arquivo, então é promovido com o modelo.

Código no host é um comando `python` ou `script`. Ele roda dentro do servidor com todo o acesso do servidor, então um código malicioso, com bugs ou guloso pode ler tudo o que o servidor pode, esgotá-lo ou derrubá-lo. É conveniente, e seguro quando você confia no código. O modelo guarda só uma referência (`module:attr` ou uma linha de comando), então o código não viaja com a promoção. Ele só roda onde `commands.allow_host_code` está ligado.

Esses limites contêm os efeitos colaterais, mas não podem eliminá-los. O que um sistema externo ou o seu próprio código faz depende, no fim, dele.

## O que o modelo oferece {: #what-you-get }

- Toda regra é registrada em um domínio, com permissões.
- Toda regra é governada do mesmo jeito, seja qual for a forma de escrita.
- Toda regra aparece na [linhagem](lineage.md).

## Leia em seguida {: #read-next }

- [Views](configuration.md#views-governed-computed-datasets)
- [Métricas](modeling.md#metrics)
- [Comandos](commands.md)
- [Resolvers de código](code-sources.md)
- [Verificações de qualidade de dados](sources.md#data-quality-checkers)
