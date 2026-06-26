# ADR-001 — Delta Lake como formato de tabela do Lakehouse

**Status:** Aceito · **Data:** 2026-06 · **Decisão de:** Data Architect

## Contexto
O case pede uma camada de consumo confiável, consultável via SQL, sobre dados
de táxi que podem chegar com lotes tardios e exigem reprocessamento. A escolha
do formato físico das tabelas impacta consistência, performance e governança.

## Decisão
Adotar **Delta Lake** como formato padrão das camadas Bronze/Silver/Gold, com
*fallback* automático para Parquet quando `delta-spark` não estiver disponível
(ambientes mínimos de teste/CI).

## Alternativas consideradas
- **Parquet puro:** simples e universal, mas sem ACID nem *time travel*; leituras
  podem pegar escrita parcial; difícil reprocessar com segurança.
- **Apache Iceberg / Hudi:** excelentes, porém Delta tem integração nativa com
  Databricks Community (ambiente recomendado pelo case) e menor atrito local.

## Consequências
- (+) Transações ACID evitam leituras de dados pela metade durante ingestão.
- (+) *Time Travel* permite auditoria e recuperação pontual após jobs ruidosos.
- (+) `MERGE`/*schema evolution* facilitam upserts e mudanças de contrato.
- (−) Pequeno *overhead* de `_delta_log`; mitigado por `OPTIMIZE`/compactação.
