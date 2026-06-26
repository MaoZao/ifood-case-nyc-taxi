"""
Configuração central do pipeline.

Tudo que varia entre ambientes (caminhos, meses processados, schema exigido)
vive aqui, carregado de `conf/pipeline.yaml` com override por variáveis de
ambiente. Assim o mesmo código roda em local, Docker (MinIO/S3) e Databricks
sem alteração — só muda a configuração.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List

import yaml

# Colunas exigidas pelo case na camada de consumo (não negociável).
REQUIRED_COLUMNS: List[str] = [
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "conf" / "pipeline.yaml"


@dataclass
class Paths:
    landing: str
    bronze: str
    silver: str
    gold: str


@dataclass
class Config:
    months: List[str]
    base_url: str
    paths: Paths
    storage_format: str = "delta"          # delta | parquet
    partition_column: str = "trip_month"
    env: str = "local"

    @property
    def months_int(self) -> List[int]:
        return [int(m.split("-")[1]) for m in self.months]


def _expand(value: str) -> str:
    """Permite ${VAR} e ~ nos caminhos do YAML."""
    return os.path.expandvars(os.path.expanduser(value))


def load_config(path: str | os.PathLike | None = None) -> Config:
    cfg_path = Path(path) if path else Path(os.getenv("IFOOD_CONFIG", DEFAULT_CONFIG_PATH))
    with open(cfg_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    paths = raw["paths"]
    cfg = Config(
        months=raw["months"],
        base_url=os.getenv("IFOOD_BASE_URL", raw["base_url"]),
        paths=Paths(
            landing=_expand(os.getenv("IFOOD_LANDING", paths["landing"])),
            bronze=_expand(os.getenv("IFOOD_BRONZE", paths["bronze"])),
            silver=_expand(os.getenv("IFOOD_SILVER", paths["silver"])),
            gold=_expand(os.getenv("IFOOD_GOLD", paths["gold"])),
        ),
        storage_format=os.getenv("IFOOD_FORMAT", raw.get("storage_format", "delta")),
        partition_column=raw.get("partition_column", "trip_month"),
        env=os.getenv("IFOOD_ENV", raw.get("env", "local")),
    )
    return cfg
