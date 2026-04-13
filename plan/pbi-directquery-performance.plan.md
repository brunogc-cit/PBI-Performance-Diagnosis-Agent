---
name: PBI DirectQuery Performance Plan
overview: Plan com estratégias para melhorar a performance dos reports Power BI que usam DirectQuery contra Databricks, incluindo a hipótese de parametrização da função _fn_GetDataFromDBX para reduzir o volume de dados consultados.
todos:
  - id: validate-folding
    content: Validar query folding do conector Databricks PBI com Table.SelectRows e filtros de data na funcao _fn_GetDataFromDBX
    status: pending
  - id: prototype-incremental
    content: Prototipar Incremental Refresh + DirectQuery hybrid no modelo ADE - OrderLine (fact_order_line_v1)
    status: pending
  - id: prototype-parameterized-fn
    content: Criar _fn_GetDataFromDBXFiltered com parametros de data e testar query folding
    status: pending
  - id: evaluate-materialized-views
    content: Avaliar viabilidade de converter serve views para materialized views no dbt-databricks
    status: pending
  - id: audit-dq-dims
    content: Auditar dimensoes em DirectQuery que poderiam ser convertidas para dual/import
    status: pending
isProject: false
---

# Plano de Otimização de Performance - Power BI DirectQuery sobre Databricks

## Contexto Atual

- **13 modelos semânticos** em `asos-data-ade-powerbi/powerbi/models`, organizados por domínio (sales, customer, product, supplychain, sourcing and buying)
- As **fact tables** usam **DirectQuery** contra Databricks (ex: `fact_order_line_v1` com 6B+ registros)
- A camada **serve** no dbt (`asos-data-ade-dbt`) é composta inteiramente de **views** sobre tabelas Delta curated
- A função [`_fn_GetDataFromDBX`](asos-data-ade-powerbi/powerbi/models/sales/ADE - OrderLine/expressions/_fn_GetDataFromDBX.json) retorna a **tabela inteira** sem nenhum filtro M aplicado:

```m
func = (_DbxCatalog, _DbxDatabase, _DbxTable) =>
    let Source = Databricks.Catalogs(_DbxServer, _DbxEndpoint, []),
        catalog_Database = Source{[Name=_fn_ConvertCatalog(_DbxCatalog),Kind="Database"]}[Data],
        default_Schema = catalog_Database{[Name=_DbxDatabase,Kind="Schema"]}[Data],
        Table = default_Schema{[Name=_DbxTable,Kind="Table"]}[Data]
    in Table
```

- As **partitions DirectQuery** dos fatos são simples pass-through (ex: `let DatabricksTable = _fn_GetDataFromDBX("sales", "serve", "fact_order_line_v1") in DatabricksTable`)
- As tabelas curated usam **liquid clustering** (ex: `fact_order_line_v1` clustered por `dim_fx_rate_type_sk`, `dim_billed_transaction_date_sk`, etc.)
- Modelos como **ADE - OrderLine** (73 tabelas, 14 DQ), **ADE - Sales** (52 tabelas), **ADE - Purchase/Commitment/Stock** (78 tabelas) são os mais complexos

---

## Estratégia 1: Parametrizar `_fn_GetDataFromDBX` com Filtros (hipótese do usuário)

**Viabilidade: SIM, com ressalvas importantes sobre query folding.**

### Abordagem

Criar uma variante da função, ex: `_fn_GetDataFromDBXFiltered`, que receba parâmetros adicionais (data inicial, data final, nome da coluna de filtro) e aplique `Table.SelectRows` dentro do M:

```m
func = (_DbxCatalog as text, _DbxDatabase as text, _DbxTable as text,
        _FilterColumn as text, _StartDate as date, _EndDate as date) =>
    let
        Source = Databricks.Catalogs(_DbxServer, _DbxEndpoint, []),
        catalog_Database = Source{[Name=_fn_ConvertCatalog(_DbxCatalog),Kind="Database"]}[Data],
        default_Schema = catalog_Database{[Name=_DbxDatabase,Kind="Schema"]}[Data],
        FullTable = default_Schema{[Name=_DbxTable,Kind="Table"]}[Data],
        Filtered = Table.SelectRows(FullTable, each Record.Field(_, _FilterColumn) >= _StartDate
                                                and Record.Field(_, _FilterColumn) <= _EndDate)
    in
        Filtered
```

A partição do `Order Line` passaria a ser:

```m
let DatabricksTable = _fn_GetDataFromDBXFiltered("sales", "serve", "fact_order_line_v1",
    "billed_transaction_date", _StartDate, _EndDate)
in DatabricksTable
```

Onde `_StartDate` e `_EndDate` seriam novos parâmetros M no modelo (em `expressions/`).

### Riscos e Considerações

- **Query Folding**: O filtro `Table.SelectRows` precisa "fold" para SQL no Databricks. O conector Databricks do Power BI geralmente suporta folding de filtros simples de data, **mas usar `Record.Field` dinâmico pode quebrar o folding**. Uma alternativa mais segura é criar funções específicas por tipo de filtro em vez de genérica.
- **Limitação do DirectQuery**: Em DirectQuery, o Power BI já gera SQL com filtros baseados nos slicers/filtros do report. Adicionar filtro M fixo **é redundante com os filtros do report**, mas funciona como um **"guard rail"** para garantir que nunca se consulte dados além de um período.
- **Manutenção**: Cada modelo/tabela pode precisar de parâmetros diferentes (coluna de data diferente para cada fact table).
- **Impacto nos reports existentes**: Reports que mostram dados históricos além do range dos parâmetros **deixarão de funcionar**.

### Alternativa Mais Robusta: `Value.NativeQuery`

Em vez de depender do folding do M, usar SQL nativo que garante execução no Databricks:

```m
func = (_DbxCatalog as text, _DbxDatabase as text, _DbxTable as text,
        _FilterColumn as text, _StartDate as text, _EndDate as text) =>
    let
        Source = Databricks.Catalogs(_DbxServer, _DbxEndpoint, []),
        catalog_Database = Source{[Name=_fn_ConvertCatalog(_DbxCatalog),Kind="Database"]}[Data],
        Result = Value.NativeQuery(catalog_Database,
            "SELECT * FROM " & _DbxDatabase & "." & _DbxTable &
            " WHERE " & _FilterColumn & " >= '" & _StartDate & "'" &
            " AND " & _FilterColumn & " <= '" & _EndDate & "'")
    in
        Result
```

**Importante**: `Value.NativeQuery` pode desabilitar query folding downstream, então os filtros adicionais do report precisariam ser incluídos manualmente no SQL ou via parâmetros.

---

## Estratégia 2: Incremental Refresh + Real-Time (Hybrid Mode)

**Impacto: ALTO. Recomendação principal.**

O Power BI suporta **Incremental Refresh com DirectQuery** (disponível com Premium/PPU):
- Dados **históricos** são importados (cache VertiPaq) com refresh incremental
- Dados **recentes** (ex: últimos 3 dias) ficam em DirectQuery
- A tabela precisa ter uma coluna de data/datetime usada como `RangeStart`/`RangeEnd`

### Como implementar

1. Nos modelos semânticos, criar os parâmetros M `RangeStart` e `RangeEnd` (tipo DateTime)
2. Modificar as partições dos fatos para incluir filtro de data usando esses parâmetros:
   ```m
   let
     DatabricksTable = _fn_GetDataFromDBX("sales", "serve", "fact_order_line_v1"),
     Filtered = Table.SelectRows(DatabricksTable,
       each [billed_transaction_date] >= RangeStart and [billed_transaction_date] < RangeEnd)
   in Filtered
   ```
3. Configurar a política de Incremental Refresh no Power BI Service (ex: importar últimos 3 anos, DQ para últimos 3 dias)

### Vantagens

- Dados históricos ficam em cache (rápido)
- Dados recentes vêm do Databricks (sempre atualizados)
- Refresh diário só processa o "delta" (rápido e barato)

### Riscos

- Requer **Power BI Premium / PPU / Fabric**
- A coluna de filtro precisa ser uma coluna de data na tabela fato
- O filtro **precisa** fazer query folding (testar com o conector Databricks)

---

## Estratégia 3: Criar Tabelas Agregadas na Camada Serve do Databricks

**Impacto: ALTO para reports de alto nível.**

### Abordagem

Criar novos modelos dbt na camada **serve** que pré-agreguem os dados por granularidade comum dos reports:

- `serve.fact_order_line_daily_agg_v1` (agregado por dia + dimensões principais)
- `serve.fact_order_line_weekly_agg_v1` (agregado por semana)
- `serve.fact_order_line_monthly_agg_v1` (agregado por mês)

No Power BI, criar um modelo **composite**:
- Tabelas agregadas importadas (ou DQ, mas muito menores)
- Tabela de detalhe em DQ (para drill-through)
- Usar **aggregation awareness** do Power BI para rotear queries automaticamente

### Vantagens

- Redução drástica do volume de dados (de bilhões para milhões)
- Reports de alto nível ficam instantâneos
- Drill-through mantém acesso ao detalhe quando necessário

---

## Estratégia 4: Materializar Views da Camada Serve no Databricks

**Impacto: MÉDIO-ALTO.**

Atualmente todas as serve tables são **views** (`materialized: view` no dbt). Quando o Power BI faz DirectQuery, o Databricks precisa:
1. Resolver a view
2. Ler a tabela curated
3. Aplicar transformações da view
4. Retornar os resultados

### Abordagem

Converter as serve tables mais pesadas para **materialized views** ou **tables** no dbt:

```yaml
# Em serve/_contracts/serve_fact_order_line_v1.yml
config:
  alias: fact_order_line_v1
  materialized: materialized_view  # era: view
```

Ou criar uma camada intermediária "serve_pbi" com tabelas materializadas específicas para Power BI.

### Vantagens

- Elimina o custo de resolução da view em cada query
- O Databricks mantém a materialized view atualizada automaticamente

### Riscos

- Custo de storage adicional
- Latência de atualização da materialized view (pode não ser real-time)
- Verificar suporte do dbt-databricks a `materialized_view`

---

## Estratégia 5: Otimizações no Lado do Databricks

**Impacto: MÉDIO.**

### 5a. Liquid Clustering otimizado

A `fact_order_line_v1` já usa liquid clustering em `dim_fx_rate_type_sk`, `dim_billed_transaction_date_sk`, `dim_return_transaction_date_sk`, `dim_shipped_transaction_date_sk`. Validar se as colunas de clustering estão alinhadas com os filtros mais comuns dos reports PBI.

### 5b. SQL Warehouse sizing e caching

- Usar **Serverless SQL Warehouse** para melhor auto-scaling
- Habilitar **query result caching** no warehouse
- Considerar **warehouse dedicado** para PBI (evitar contenção)

### 5c. Predictive I/O e Photon

- Garantir que Photon está habilitado no SQL Warehouse
- Habilitar **Predictive I/O** se disponível

---

## Estratégia 6: Otimizações no Modelo Semântico Power BI

**Impacto: MÉDIO.**

### 6a. Reduzir cardinalidade

- Remover colunas não usadas das tabelas DQ (projeção explícita no M ou no SQL)
- Evitar colunas de texto de alta cardinalidade em DQ

### 6b. Converter dimensões de DQ para Import/Dual

- Tabelas como `dim_product_v1`, `dim_customer_account_v1` que estão em DQ podem ser movidas para **dual** ou **import** (se o tamanho permitir)
- Isso reduz o número de joins que precisam ser resolvidos no Databricks

### 6c. Otimizar DAX

- Revisar medidas DAX que podem gerar queries DQ ineficientes
- Usar `SUMMARIZECOLUMNS` em vez de `ADDCOLUMNS` + `VALUES` quando possível
- Evitar iteradores (SUMX, FILTER) sobre tabelas DQ grandes

---

## Resumo Comparativo das Estratégias

- **Estratégia 1** (Parametrizar fn_GetDataFromDBX): Esforço baixo, impacto médio, risco médio (query folding). Bom como "guard rail" para limitar escopo máximo.
- **Estratégia 2** (Incremental Refresh + Hybrid): Esforço médio, impacto alto. Melhor relação custo-benefício. Requer Premium/Fabric.
- **Estratégia 3** (Tabelas Agregadas): Esforço alto, impacto alto. Ideal para reports executivos/alto nível.
- **Estratégia 4** (Materializar Views): Esforço médio, impacto médio-alto. Quick win se o dbt suportar materialized views.
- **Estratégia 5** (Otimizações Databricks): Esforço baixo-médio, impacto médio. Complementar às outras.
- **Estratégia 6** (Otimizações Modelo PBI): Esforço médio, impacto médio. Deve ser feito em paralelo.

## Recomendação de Prioridade

1. **Começar pela Estratégia 2** (Incremental Refresh Hybrid) para as fact tables mais pesadas (`fact_order_line_v1`, `fact_billed_sale_v1`)
2. **Em paralelo, Estratégia 5** (otimizações Databricks) - quick wins sem mudança no PBI
3. **Estratégia 1** como complemento da 2 - parametrizar `_fn_GetDataFromDBX` para limitar o escopo máximo
4. **Estratégia 4** (materializar views serve) para ganho imediato sem mudanças no PBI
5. **Estratégia 3** para reports específicos de alto volume de acesso
6. **Estratégia 6** como otimização contínua
