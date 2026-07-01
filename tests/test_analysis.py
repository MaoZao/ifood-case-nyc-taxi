"""Testes das respostas do case (Gold/analytics)."""

from __future__ import annotations

from ifood_case.pipeline.gold import passageiros_por_hora_maio, receita_mensal
from ifood_case.transformations import to_silver


def test_q1_receita_mensal(spark, raw_df):
    silver = to_silver(raw_df)
    out = {r["trip_month"]: r for r in receita_mensal(silver).collect()}
    # Após limpeza sobram: jan (20.5) e maio (55.0).
    assert out[1]["receita_media_usd"] == 20.5
    assert out[5]["receita_media_usd"] == 55.0
    assert out[1]["qtd_corridas"] == 1


def test_q2_passageiros_hora_maio(spark, raw_df):
    silver = to_silver(raw_df)
    out = passageiros_por_hora_maio(silver).collect()
    # Apenas a corrida boa de maio às 18h sobrevive.
    assert len(out) == 1
    assert out[0]["pickup_hour"] == 18
    assert out[0]["media_passageiros"] == 4.0


def test_q2_only_may(spark, raw_df):
    silver = to_silver(raw_df)
    out = passageiros_por_hora_maio(silver).collect()
    # Garante que nenhum mês != 5 vazou: a corrida boa de JANEIRO é às 8h —
    # se vazasse, a hora 8 apareceria no resultado.
    hours = {r["pickup_hour"] for r in out}
    assert hours == {18}
