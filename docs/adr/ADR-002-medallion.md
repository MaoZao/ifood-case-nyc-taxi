# ADR-002 — Topologia Medallion (Bronze/Silver/Gold)

**Status:** Aceito · **Data:** 2026-06

## Contexto
O case exige uma *landing zone* para arquivos originais e uma *camada de
consumo* estruturada, deixando a organização intermediária a critério do
candidato. É preciso uma estrutura que separe responsabilidades e permita
reprocessamento seguro.

## Decisão
Organizar o Lakehouse em três camadas: **Bronze** (raw imutável), **Silver**
(trusted/limpo) e **Gold** (consumo/agregações).

## Alternativas consideradas
- **Duas camadas (raw + consumo):** menos código, mas mistura limpeza e
  agregação na mesma etapa, dificultando teste, reuso e *backfill* seletivo.
- **Star schema clássico em DW:** rígido demais para dados semiestruturados e
  evolução rápida de schema.

## Consequências
- (+) Cada camada é testável e tem contrato próprio; falhas ficam isoladas.
- (+) Bronze intocada habilita reprocessamento total quando regras mudam.
- (+) Gold pré-agregada acelera o consumo de BI/SQL.
- (−) Mais artefatos de storage; aceitável e padrão de mercado.
