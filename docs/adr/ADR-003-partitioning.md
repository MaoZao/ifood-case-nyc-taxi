# ADR-003 — Estratégia de particionamento

**Status:** Aceito · **Data:** 2026-06

## Contexto
As consultas do case filtram/agrupam por tempo (média mensal de receita; média
horária de passageiros em maio). O layout físico deve favorecer *partition
pruning* sem gerar *small files* excessivos.

## Decisão
Particionar a camada Silver por **`trip_month`** (coluna derivada de
`tpep_pickup_datetime`), de baixa cardinalidade (5 valores no período) e
diretamente alinhada às consultas. A hora (`pickup_hour`) é mantida como coluna,
não como partição, para evitar fragmentação (24× mais diretórios).

## Alternativas consideradas
- **Particionar por dia/hora:** *pruning* mais fino, porém explode o nº de
  arquivos e degrada leitura — anti-padrão nesta volumetria.
- **Sem partição:** simples, mas força *full scan* a cada consulta mensal.

## Consequências e evolução
- (+) *Pruning* eficiente nas consultas mensais; nº de partições saudável.
- (→) **Em escala de produção** (~16M linhas/mês): aplicar `OPTIMIZE` + **Z-Order**
  por `tpep_pickup_datetime`, ou preferir **Liquid Clustering** (recomendação
  atual do Databricks para tabelas novas), que dispensa a escolha rígida de
  coluna de partição e reclusteriza incrementalmente.

> Refs.: Databricks/Delta docs — *When to partition tables*, *OPTIMIZE & Z-Order*,
> *Liquid Clustering*.
