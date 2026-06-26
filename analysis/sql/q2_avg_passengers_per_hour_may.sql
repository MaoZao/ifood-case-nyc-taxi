-- ============================================================================
-- Q2: Qual a média de passageiros (passenger_count) por cada hora do dia que
--     pegaram táxi no mês de MAIO, considerando todos os táxis da frota?
--
-- Agrupa por hora de embarque (0-23) apenas as corridas de maio (mês 5) e
-- calcula a média de passageiros por corrida em cada faixa horária.
-- ============================================================================
SELECT
    HOUR(tpep_pickup_datetime)              AS hora_do_dia,
    COUNT(*)                                AS qtd_corridas,
    ROUND(AVG(passenger_count), 3)          AS media_passageiros
FROM ifood.silver_trips
WHERE trip_month = 5
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY hora_do_dia;
