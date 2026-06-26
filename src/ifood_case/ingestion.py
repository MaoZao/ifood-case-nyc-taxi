"""
Ingestão dos arquivos originais do NYC TLC para a Landing Zone.

Baixa os Parquet oficiais (Jan-Mai/2023) do CloudFront do TLC. Idempotente:
pula arquivos já presentes. Sem dependência de SDK de cloud — `urllib` resolve.
Para storage em nuvem, basta apontar `landing` para s3://... (o Spark grava lá
nativamente; aqui mantemos o download simples e portável).
"""
from __future__ import annotations

import argparse
import logging
import os
import urllib.request
from pathlib import Path

from .config import load_config

logger = logging.getLogger(__name__)


def download_month(base_url: str, month: str, dest_dir: str) -> str:
    """Baixa um mês (ex.: '2023-01'); retorna o caminho local."""
    fname = f"yellow_tripdata_{month}.parquet"
    url = f"{base_url.rstrip('/')}/{fname}"
    dest = Path(dest_dir) / fname
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and dest.stat().st_size > 0:
        logger.info("[skip] %s já existe (%d bytes)", fname, dest.stat().st_size)
        return str(dest)

    logger.info("[download] %s -> %s", url, dest)
    tmp = dest.with_suffix(".parquet.part")
    urllib.request.urlretrieve(url, tmp)  # noqa: S310 (URL oficial confiável)
    os.replace(tmp, dest)
    logger.info("[ok] %s (%d bytes)", fname, dest.stat().st_size)
    return str(dest)


def ingest(config_path: str | None = None) -> None:
    cfg = load_config(config_path)
    logger.info("Baixando %d meses para %s", len(cfg.months), cfg.paths.landing)
    for month in cfg.months:
        download_month(cfg.base_url, month, cfg.paths.landing)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Baixa NYC TLC Yellow Taxi para a landing zone.")
    ap.add_argument("--config", default=None)
    ingest(ap.parse_args().config)


if __name__ == "__main__":
    main()
