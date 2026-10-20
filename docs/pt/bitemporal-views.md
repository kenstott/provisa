# Criar uma exibição bitemporal {: #create-a-bitemporal-view }

Uma exibição bitemporal guarda as duas coisas: quando algo aconteceu e quando foi registrado. Assim você pode lê-la em uma data, ou como estava registrada em uma data, e uma correção feita depois nunca esconde o que se sabia antes dela.

Uma exibição se torna bitemporal quando você mapeia três de suas colunas:

- **Chave de negócio**: a coluna, ou colunas, que identificam o mesmo registro entre versões, como `transaction_id`. Um pagamento corrigido continua sendo o mesmo pagamento, então as duas versões têm a mesma chave.
- **Tempo válido**: a coluna com a data que seu negócio dá ao registro, como quando foi acordado, lançado ou entrou em vigor.
- **Tempo de registro**: a coluna com o momento em que esta versão foi inserida ou alterada, como `updated_at`.

O tempo de registro é chamado de tempo de transação na literatura de bancos de dados temporais e de tempo do sistema no SQL:2011; o tempo válido é chamado de tempo de aplicação no SQL:2011, ou data efetiva.

Os três valores mapeiam colunas que sua fonte já tem. O Provisa decide como as versões são guardadas; não há modo de armazenamento a escolher.

## Três exemplos {: #three-examples }

| Exemplo | Tempo válido | Tempo de registro |
| ------- | ------------ | ----------------- |
| Um pagamento de 100 para 3 de out. é corrigido para 120 em 5 de out. | 3 de out., mantido pela correção. | 3 de out. para a primeira versão; 5 de out. para a correção. |
| Uma medição de campo feita em 3 de out. às 14:10 é digitada em 6 de out. Um erro de digitação nela é corrigido em 7 de out. | 3 de out. 14:10, mantido pela correção. | 6 de out.; a correção recebe 7 de out. |
| Uma operação é lançada na execução, data da operação 3 de out., às 10:02. Em 4 de out. a quantidade se mostra errada e é retificada. | Data da operação 3 de out., mantida pela retificação. | 3 de out. 10:02; a retificação recebe 4 de out. |

Lida como informada em 3 de out., a operação mostra a quantidade errada. Lida conforme se sabe hoje, mostra a corrigida.

## Criar a exibição {: #create-the-view }

Uma exibição que guarda histórico é materializada, para que suas versões sejam armazenadas.

1. Abra **Views** e clique em **+ View**.
2. Defina a exibição em qualquer uma das duas abas:
    - **Metrics**: dê um nome à exibição, escolha o domínio, as métricas e as dimensões e clique em **Continue**.
    - **SQL**: abra o editor SQL, escreva e execute o `SELECT`, clique em **+ View** no editor, dê um nome à exibição e clique em **Continue**.
3. As configurações da exibição se abrem nas mesmas seções do seu editor. Em **Load and timeliness**, marque **Materialized View**. A seção **Time Travel** aparece.
4. Defina **Keep history** como **Bitemporal**.
5. Escolha as colunas de **Business key**, **Valid time** e **Recorded time**.
6. Ajuste o restante nas outras seções e clique em **Save**. Nada é registrado antes disso.

As três são declaradas juntas ou nenhuma. O Provisa recusa uma exibição que nomeia só algumas e diz quais faltam.

Uma tabela recebe as mesmas seções quando você a registra: escolha-a em **+ Table**, clique em **Continue**, e a seção Time Travel dela fica em **Load and timeliness**. Um modelo recebe a seção quando você o cria (**+ Model**). Em um modelo, a chave dele é a chave de negócio, então a seção pede apenas os dois tempos.

## Lê-la {: #read-it }

A exibição aparece na lista **Views** com o selo **HISTORY**. O painel de detalhes dela diz que guarda histórico e leva ao explorador SQL com a exibição selecionada. Ajuste os seletores **As of** e **As recorded at** do explorador para os momentos desejados e execute a consulta.

Os mesmos tempos chegam a todas as outras superfícies. Uma sessão pgwire os define uma vez para cada instrução que executa:

```sql
SET provisa.as_of = '2026-10-03T12:00:00';
SET provisa.as_recorded = '2026-10-03T23:59:59';
```

Os cabeçalhos HTTP, os argumentos de `resolve` e os parâmetros REST estão em [Exibições com histórico](configuration.md#views-with-history-time-travel).

## Alterá-la depois {: #change-it-later }

O editor mostra a seção Time Travel na aba **Load and timeliness** da exibição. Depois que a exibição é construída, suas versões ficam registradas sob a chave de negócio e o tempo válido dela, então o editor não oferece mais esses dois, nem o interruptor que desliga o Time Travel. O tempo de registro ainda pode mudar. Para organizar o histórico com outra chave, crie uma nova exibição.
