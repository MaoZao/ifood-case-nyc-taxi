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

CONF_DIR = Path(__file__).resolve().parents[2] / "conf"
DEFAULT_CONFIG_PATH = CONF_DIR / "pipeline.yaml"
_REPO_ROOT = CONF_DIR.parent  # raiz do repositório


def resolve_config_path() -> Path:
    """Resolve qual YAML usar, nesta ordem de precedência:

    1. IFOOD_CONFIG (caminho explícito);
    2. conf/pipeline.<IFOOD_ENV>.yaml  (ex.: dev/hom/prd) — overlay por ambiente;
    3. conf/pipeline.yaml              (base/local).

    Isso é o que liga o pipeline à estratégia de 3 ambientes: o mesmo código,
    selecionando a config certa via a variável IFOOD_ENV injetada pelo CI/CD.
    """
    explicit = os.getenv("IFOOD_CONFIG")
    if explicit:
        return Path(explicit)
    env = os.getenv("IFOOD_ENV")
    if env:
        candidate = CONF_DIR / f"pipeline.{env}.yaml"
        if candidate.exists():
            return candidate
    return DEFAULT_CONFIG_PATH


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
    storage_format: str = "delta"  # delta | parquet
    partition_column: str = "trip_month"
    env: str = "local"
    database: str = "ifood"  # nome do schema/db no Hive Metastore
    warehouse_dir: str = "data/_warehouse"

    @property
    def months_int(self) -> List[int]:
        return [int(m.split("-")[1]) for m in self.months]

    @property
    def window(self) -> tuple[str, str]:
        """Janela [start, end) de datas válidas derivada de ``months``.

        Ex.: months=[2023-01 … 2023-05] -> ("2023-01-01", "2023-06-01").
        Fonte única de verdade: mudar os meses no YAML ajusta a limpeza da
        Silver automaticamente (nada de janelas hardcoded no código).
        """
        first = min(self.months)
        last = max(self.months)
        start = f"{first}-01"
        y, m = (int(p) for p in last.split("-"))
        end = f"{y + 1}-01-01" if m == 12 else f"{y}-{m + 1:02d}-01"
        return start, end


def _expand(value: str) -> str:
    """Permite ${VAR} e ~ nos caminhos do YAML."""
    return os.path.expandvars(os.path.expanduser(value))


def _resolve_fs_path(value: str) -> str:
    """Como ``_expand``, mas ancora caminhos de filesystem LOCAIS relativos na
    raiz do repo. URIs (``s3a://``, ``file://``, ``dbfs:/`` …) e caminhos já
    absolutos passam intactos. Garante que o pipeline e os catálogos apontem
    sempre para o MESMO lugar, independentemente do ``cwd`` (ex.: notebooks)."""
    v = _expand(value)
    if "://" in v or v.startswith("dbfs:") or Path(v).is_absolute():
        return v
    return (_REPO_ROOT / v).as_posix()


def load_config(path: str | os.PathLike | None = None) -> Config:
    cfg_path = Path(path) if path else resolve_config_path()
    with open(cfg_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    paths = raw["paths"]
    cfg = Config(
        months=raw["months"],
        base_url=os.getenv("IFOOD_BASE_URL", raw["base_url"]),
        paths=Paths(
            landing=_resolve_fs_path(os.getenv("IFOOD_LANDING", paths["landing"])),
            bronze=_resolve_fs_path(os.getenv("IFOOD_BRONZE", paths["bronze"])),
            silver=_resolve_fs_path(os.getenv("IFOOD_SILVER", paths["silver"])),
            gold=_resolve_fs_path(os.getenv("IFOOD_GOLD", paths["gold"])),
        ),
        storage_format=os.getenv("IFOOD_FORMAT", raw.get("storage_format", "delta")),
        partition_column=raw.get("partition_column", "trip_month"),
        env=os.getenv("IFOOD_ENV", raw.get("env", "local")),
        database=os.getenv("IFOOD_DATABASE", raw.get("database", "ifood")),
        warehouse_dir=_expand(
            os.getenv("IFOOD_WAREHOUSE", raw.get("warehouse_dir", "data/_warehouse"))
        ),
    )
    return cfg
