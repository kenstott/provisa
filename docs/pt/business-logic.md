# Lógica de negócio no Provisa

## O problema {: #the-problem }

A lógica de negócio acaba em todo lugar: em jobs de ETL, dashboards e APIs, nas mãos de equipes diferentes e catalogada por ninguém. A “receita líquida” é calculada de um jeito no job do warehouse, de outro no dashboard financeiro e de um terceiro na API de cobrança, e ninguém sabe dizer qual está certo.

Não é culpa de ninguém. Não havia um modelo comum e simples para colocar as regras.

## O que é lógica de negócio {: #definition }

Lógica de negócio são as regras que criam, atualizam, excluem ou transformam dados.

A terminologia de negócio é outra coisa. Os nomes e descrições de domínios, tabelas e colunas, e os termos do [glossário](glossary.md), dizem o que os dados significam. A lógica de negócio diz o que se faz com eles.

## Três papéis {: #roles }

Toda regra cumpre um de três papéis, e cada papel pode ser escrito de três formas:

| Papel | SQL | Chamada externa | Resolver de código |
|---|---|---|---|
| **Conjunto de dados** | view, comando de instrução | API ou procedimento que devolve linhas | `@table`, `@stream` |
| **Valor** | expressão de view, métrica | — | — |
| **Mudança** | `INSERT`, `UPDATE`, `DELETE`, comando de instrução | escrita em API, mutation, procedimento | `@command` |

- **Chamada externa.** A API GraphQL, REST ou gRPC de outro sistema, ou o procedimento armazenado de um banco de dados, registrado como comando.
- **Métrica.** Um agregado com nome, definido uma vez, como `net_revenue` dado por `SUM(orders.amount) - SUM(orders.refunds)`. Cada consulta o detalha pelas dimensões que pede, então o warehouse, o dashboard e a API leem o mesmo número.

## Efeitos colaterais ocultos {: #side-effects }

Um efeito colateral oculto é aquele que não se vê na definição da regra.

Toda regra tem nome e tipo, e é registrada, auditada e protegida.

| | Efeitos colaterais possíveis | Limitados por |
|---|---|---|
| **SQL** | Nenhum | — |
| **Chamada externa** | Os próprios fluxos de trabalho, notificações e cascatas do outro sistema | Aprovação |
| **Resolver de código** | Chamadas a serviços e redes externos; resultados que variam; custo | Sandbox, medição |

Uma chamada externa é tipada pelo esquema do próprio sistema, ou pelos argumentos e colunas de retorno que você declara ao registrar o comando.

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
