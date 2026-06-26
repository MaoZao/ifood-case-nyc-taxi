"""
Gerador de dados sintéticos no schema do NYC TLC Yellow Taxi (2023).

Por que existe: os arquivos reais do TLC (Jan-Mai/2023) têm ~400 MB e ~16M
linhas/mês. Para rodar o pipeline em CI, em máquinas modestas ou sem acesso à
internet, este script produz um *sample* fiel ao schema e às distribuições
reais (curva de demanda horária, sazonalidade de receita, nulos em
passenger_count, outliers de total_amount), permitindo demonstrar a solução
ponta-a-ponta de forma reprodutível.

Para os dados REAIS, use:  python -m ifood_case.ingestion.download

Saída: arquivos parquet (se pyarrow disponível) ou csv na landing zone,
um por mês: yellow_tripdata_2023-0{1..5}.(parquet|csv)
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# Curva de demanda horária (proporção de corridas por hora) — perfil típico de
# NYC: vale de madrugada, pico da tarde/noite (saída do trabalho + jantar).
HOURLY_WEIGHTS = np.array([
    0.018, 0.012, 0.008, 0.006, 0.006, 0.010,  # 00-05
    0.020, 0.035, 0.050, 0.052, 0.048, 0.050,  # 06-11
    0.052, 0.053, 0.055, 0.058, 0.062, 0.070,  # 12-17
    0.072, 0.066, 0.058, 0.050, 0.040, 0.029,  # 18-23
])
HOURLY_WEIGHTS = HOURLY_WEIGHTS / HOURLY_WEIGHTS.sum()

# Ticket médio (total_amount) sobe ao longo de 2023 (reajustes/sazonalidade).
MONTH_FARE_MEAN = {1: 26.2, 2: 26.8, 3: 27.4, 4: 28.1, 5: 28.9}
MONTH_ROWS_BASE = {1: 3.07e6, 2: 2.91e6, 3: 3.40e6, 4: 3.29e6, 5: 3.51e6}  # proporção real aprox.


def _passenger_count(n: int, rng: np.random.Generator) -> np.ndarray:
    # Distribuição real: maioria 1 passageiro; cauda até 6.
    base = rng.choice(
        [1, 2, 3, 4, 5, 6],
        size=n,
        p=[0.72, 0.14, 0.04, 0.02, 0.04, 0.04],
    ).astype("float64")
    # ~7% de nulos (driver não informou) — espelha o dataset real.
    null_mask = rng.random(n) < 0.07
    base[null_mask] = np.nan
    # ~0.5% com 0 passageiros (anomalia a ser limpa na Silver).
    zero_mask = rng.random(n) < 0.005
    base[zero_mask] = 0
    return base


def _total_amount(n: int, mean: float, rng: np.random.Generator) -> np.ndarray:
    # Lognormal aproxima a cauda direita de tarifas; ajustada à média alvo.
    sigma = 0.55
    mu = np.log(mean) - (sigma ** 2) / 2
    amt = rng.lognormal(mean=mu, sigma=sigma, size=n)
    amt = np.clip(amt, 0, 400).round(2)
    # ~0.8% negativos/zero (estornos, erros de terminal) — sujeira realista.
    bad = rng.random(n) < 0.008
    amt[bad] = rng.uniform(-15, 0, size=bad.sum()).round(2)
    return amt


def generate_month(year: int, month: int, n_rows: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed + month)
    days_in_month = (datetime(year + (month // 12), (month % 12) + 1, 1)
                     - datetime(year, month, 1)).days

    hours = rng.choice(np.arange(24), size=n_rows, p=HOURLY_WEIGHTS)
    days = rng.integers(1, days_in_month + 1, size=n_rows)
    minutes = rng.integers(0, 60, size=n_rows)
    seconds = rng.integers(0, 60, size=n_rows)

    pickup = [
        datetime(year, month, int(d), int(h), int(mi), int(s))
        for d, h, mi, s in zip(days, hours, minutes, seconds)
    ]
    durations = rng.gamma(shape=2.2, scale=320, size=n_rows)  # ~11 min médios
    dropoff = [p + timedelta(seconds=float(dur)) for p, dur in zip(pickup, durations)]

    df = pd.DataFrame({
        "VendorID": rng.choice([1, 2, 6], size=n_rows, p=[0.32, 0.67, 0.01]),
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": dropoff,
        "passenger_count": _passenger_count(n_rows, rng),
        "trip_distance": rng.gamma(2.0, 1.6, n_rows).round(2),
        "RatecodeID": rng.choice([1, 2, 3, 4, 5], size=n_rows, p=[0.92, 0.05, 0.01, 0.01, 0.01]),
        "store_and_fwd_flag": rng.choice(["N", "Y"], size=n_rows, p=[0.98, 0.02]),
        "PULocationID": rng.integers(1, 264, n_rows),
        "DOLocationID": rng.integers(1, 264, n_rows),
        "payment_type": rng.choice([1, 2, 3, 4], size=n_rows, p=[0.70, 0.27, 0.02, 0.01]),
        "total_amount": _total_amount(n_rows, MONTH_FARE_MEAN[month], rng),
    })
    # ~0.3% de linhas totalmente duplicadas (reprocessamento de terminal).
    dup = df.sample(frac=0.003, random_state=seed)
    df = pd.concat([df, dup], ignore_index=True)
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description="Gera sample fiel do NYC Yellow Taxi 2023.")
    ap.add_argument("--out", default="data/landing", help="Diretório da landing zone.")
    ap.add_argument("--scale", type=float, default=0.02,
                    help="Fração da volumetria real (0.02 = 2%% ~= 60k linhas/mês).")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    try:
        import pyarrow  # noqa: F401
        ext, writer = "parquet", "parquet"
    except ImportError:
        ext, writer = "csv", "csv"

    summary = []
    for month in range(1, 6):
        n = int(MONTH_ROWS_BASE[month] * args.scale)
        df = generate_month(2023, month, n, args.seed)
        path = os.path.join(args.out, f"yellow_tripdata_2023-{month:02d}.{ext}")
        if writer == "parquet":
            df.to_parquet(path, index=False)
        else:
            df.to_csv(path, index=False)
        summary.append((month, len(df), path))
        print(f"[ok] 2023-{month:02d}: {len(df):,} linhas -> {path}")

    total = sum(s[1] for s in summary)
    print(f"\nTotal gerado: {total:,} linhas em {len(summary)} arquivos ({ext}).")


if __name__ == "__main__":
    main()
