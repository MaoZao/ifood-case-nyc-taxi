-- ============================================================================
-- Q1: Qual a média de total_amount recebido em um mês, considerando todos os
--     yellow táxis da frota?
--
-- Lê a camada de consumo (Gold/Silver) registrada como tabela/view SQL.
-- Interpretação: média do valor por corrida em cada mês (Jan-Mai/2023), além
-- da média global do período. Ordenado cronologicamente.
-- ============================================================================
SELECT
    trip_month                              AS mes,
    COUNT(*)                                AS qtd_corridas,
    ROUND(AVG(total_amount), 2)             AS receita_media_usd,
    ROUND(SUM(total_amount), 2)             AS receita_total_usd
FROM ifood.silver_trips
GROUP BY trip_month
ORDER BY trip_month;

-- Média global do período (resposta direta "média em um mês"):
-- SELECT ROUND(AVG(total_amount), 2) AS media_global_usd FROM ifood.silver_trips;
