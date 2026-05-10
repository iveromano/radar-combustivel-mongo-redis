"""
Shared configuration for the Radar Combustivel pipeline.

All runtime knobs come from environment variables (loaded from .env when
present). Keys ARE NOT hard-coded inside business logic so that the same
codebase can be aimed at a dev, lab or production cluster.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Load .env if it exists (idempotent)
load_dotenv()


# ---------------------------------------------------------------------------
# Mongo
# ---------------------------------------------------------------------------
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true",
)
MONGO_DB = os.getenv("MONGO_DB", "radar_combustivel")

COL_POSTOS = os.getenv("MONGO_COL_POSTOS", "postos")
COL_LOCALIZACOES = os.getenv("MONGO_COL_LOCALIZACOES", "localizacoes_postos")
COL_PRECOS = os.getenv("MONGO_COL_PRECOS", "eventos_preco")
COL_BUSCAS = os.getenv("MONGO_COL_BUSCAS", "buscas_usuarios")
COL_AVALIACOES = os.getenv("MONGO_COL_AVALIACOES", "avaliacoes_interacoes")

ALL_COLLECTIONS = [
    COL_POSTOS,
    COL_LOCALIZACOES,
    COL_PRECOS,
    COL_BUSCAS,
    COL_AVALIACOES,
]


# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None
REDIS_DB = int(os.getenv("REDIS_DB", "0"))


# ---------------------------------------------------------------------------
# Pipeline tuning (defaults adjusted for ~100k documents)
# ---------------------------------------------------------------------------
BACKFILL_BATCH_SIZE = int(os.getenv("BACKFILL_BATCH_SIZE", "2000"))
BACKFILL_WORKERS = int(os.getenv("BACKFILL_WORKERS", "4"))
TS_RETENTION_MS = int(os.getenv("TS_RETENTION_MS", str(30 * 24 * 60 * 60 * 1000)))
RANKING_WINDOW_HOURS = int(os.getenv("RANKING_WINDOW_HOURS", "24"))
METRICS_INTERVAL_SECONDS = int(os.getenv("METRICS_INTERVAL_SECONDS", "15"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


# ---------------------------------------------------------------------------
# Streamlit
# ---------------------------------------------------------------------------
STREAMLIT_REFRESH_SECONDS = int(os.getenv("STREAMLIT_REFRESH_SECONDS", "10"))


# ---------------------------------------------------------------------------
# Redis key catalog
# ---------------------------------------------------------------------------
# Centralizing key naming avoids typos and makes the data model auditable.
# All readers and writers go through these helpers.

@dataclass(frozen=True)
class RedisKeys:
    # ----- Cadastro / metadados -----
    @staticmethod
    def posto(posto_id: str) -> str:
        return f"posto:{posto_id}"

    @staticmethod
    def posto_precos(posto_id: str) -> str:
        return f"posto:{posto_id}:precos"

    @staticmethod
    def posto_rating(posto_id: str) -> str:
        return f"posto:{posto_id}:rating"

    # ----- Geo -----
    GEO_POSTOS = "geo:postos"

    @staticmethod
    def geo_postos_combustivel(combustivel: str) -> str:
        return f"geo:postos:{combustivel.lower()}"

    # ----- RediSearch -----
    IDX_POSTOS = "idx:postos"
    POSTO_DOC_PREFIX = "doc:posto:"

    @staticmethod
    def posto_doc(posto_id: str) -> str:
        return f"doc:posto:{posto_id}"

    # ----- Rankings de preco -----
    @staticmethod
    def rank_preco_uf(combustivel: str, uf: str) -> str:
        return f"rank:preco:{combustivel.lower()}:{uf.upper()}"

    @staticmethod
    def rank_preco_cidade(combustivel: str, cidade: str) -> str:
        return f"rank:preco:{combustivel.lower()}:cidade:{cidade}"

    @staticmethod
    def rank_preco_global(combustivel: str) -> str:
        return f"rank:preco:{combustivel.lower()}:global"

    @staticmethod
    def rank_variacao(combustivel: str, window: str = "24h") -> str:
        # score = variacao_pct mais recente acumulada na janela
        return f"rank:variacao:{combustivel.lower()}:{window}"

    # ----- TimeSeries -----
    @staticmethod
    def ts_posto_combustivel(posto_id: str, combustivel: str) -> str:
        return f"ts:posto:{posto_id}:{combustivel.lower()}"

    @staticmethod
    def ts_avg_uf(combustivel: str, uf: str) -> str:
        return f"ts:avg:{combustivel.lower()}:{uf.upper()}"

    TS_BUSCAS_TOTAL = "ts:buscas:total"
    TS_BUSCAS_LATENCIA = "ts:buscas:latencia"

    # ----- Buscas -----
    RANK_BUSCAS_BAIRRO = "rank:buscas:bairro"
    RANK_BUSCAS_CIDADE = "rank:buscas:cidade"
    RANK_BUSCAS_UF = "rank:buscas:uf"
    RANK_BUSCAS_COMBUSTIVEL = "rank:buscas:combustivel"

    # Rankings de buscas com granularidade adicional
    @staticmethod
    def rank_buscas_bairro_fuel(combustivel: str) -> str:
        """Top bairros para um combustivel especifico (membros 'UF|Cidade|Bairro')."""
        return f"rank:buscas:bairro:{combustivel.lower()}"

    @staticmethod
    def rank_buscas_cidade_fuel(combustivel: str) -> str:
        """Top cidades para um combustivel especifico (membros 'UF|Cidade')."""
        return f"rank:buscas:cidade:{combustivel.lower()}"

    @staticmethod
    def rank_buscas_fuel_uf(uf: str) -> str:
        """Combustiveis mais buscados em uma UF."""
        return f"rank:buscas:fuel:{uf.upper()}"

    @staticmethod
    def rank_buscas_fuel_uf_cidade(uf: str, cidade: str) -> str:
        """Combustiveis mais buscados em uma cidade."""
        return f"rank:buscas:fuel:{uf.upper()}|{cidade}"

    # ----- Avaliacoes / interacoes -----
    RANK_POSTOS_RATING = "rank:postos:rating"
    RANK_POSTOS_UTIL = "rank:postos:util"
    RANK_POSTOS_CHECKIN = "rank:postos:checkin"
    RANK_POSTOS_COMPARTILHAMENTO = "rank:postos:compartilhamento"

    # ----- Observabilidade do pipeline -----
    METRICS_HASH = "pipeline:metrics"
    METRICS_LAST_EVENT = "pipeline:metrics:last_event"
    METRICS_LAST_RUN = "pipeline:metrics:last_run"


def configure_logging(name: str = "radar") -> logging.Logger:
    """Standard logger for every script in the pipeline."""
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(name)
