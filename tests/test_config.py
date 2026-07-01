"""Testes da configuração — em especial a janela de datas derivada de months."""

from __future__ import annotations

from ifood_case.config import Config, Paths


def _cfg(months: list[str]) -> Config:
    return Config(
        months=months,
        base_url="http://example.com",
        paths=Paths(landing="l", bronze="b", silver="s", gold="g"),
    )


def test_window_jan_to_may():
    assert _cfg(["2023-01", "2023-02", "2023-03", "2023-04", "2023-05"]).window == (
        "2023-01-01",
        "2023-06-01",
    )


def test_window_single_month():
    assert _cfg(["2023-05"]).window == ("2023-05-01", "2023-06-01")


def test_window_crosses_year_boundary():
    # Dezembro: o fim da janela precisa virar o ano.
    assert _cfg(["2023-11", "2023-12"]).window == ("2023-11-01", "2024-01-01")


def test_window_ignores_month_order():
    assert _cfg(["2023-03", "2023-01", "2023-02"]).window == ("2023-01-01", "2023-04-01")
