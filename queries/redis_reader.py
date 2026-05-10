"""
Demonstration queries against the Radar Combustivel serving layer.

Run with::

    python queries/redis_reader.py

Each function prints a small report to stdout so the queries can be
captured as evidence in the PDF deliverable. They are also reused by the
Streamlit dashboard.
"""
from __future__ import annotations

import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import redis

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import (  # noqa: E402
    REDIS_DB,
    REDIS_HOST,
    REDIS_PASSWORD,
    REDIS_PORT,
    RedisKeys,
    configure_logging,
)


log = configure_logging("reader")


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=REDIS_DB,
        decode_responses=True,
    )


# ---------------------------------------------------------------------------
# Postos lookup helpers
# ---------------------------------------------------------------------------
def get_posto(r: redis.Redis, posto_id: str) -> Dict[str, str]:
    return r.hgetall(RedisKeys.posto(posto_id))


def hydrate_postos(r: redis.Redis, posto_ids: List[str]) -> Dict[str, Dict[str, str]]:
    """Multi-get of posto hashes via pipeline."""
    if not posto_ids:
        return {}
    pipe = r.pipeline(transaction=False)
    for pid in posto_ids:
        pipe.hgetall(RedisKeys.posto(pid))
    out = pipe.execute()
    return {pid: doc for pid, doc in zip(posto_ids, out) if doc}


# ---------------------------------------------------------------------------
# Pricing queries
# ---------------------------------------------------------------------------
def cheapest_postos(
    r: redis.Redis,
    combustivel: str,
    uf: Optional[str] = None,
    cidade: Optional[str] = None,
    limit: int = 10,
) -> List[Tuple[str, float]]:
    """
    Lowest price first.

    Falls back from cidade -> UF -> global so even sparse cities give
    a useful result.
    """
    if cidade and uf:
        key = RedisKeys.rank_preco_cidade(combustivel, cidade)
        if r.exists(key):
            return [(m, float(s)) for m, s in r.zrange(key, 0, limit - 1, withscores=True)]
    if uf:
        key = RedisKeys.rank_preco_uf(combustivel, uf)
        if r.exists(key):
            return [(m, float(s)) for m, s in r.zrange(key, 0, limit - 1, withscores=True)]
    key = RedisKeys.rank_preco_global(combustivel)
    return [(m, float(s)) for m, s in r.zrange(key, 0, limit - 1, withscores=True)]


def biggest_price_swings(
    r: redis.Redis, combustivel: str, direction: str = "up", limit: int = 10
) -> List[Tuple[str, float]]:
    """
    direction='up'   -> biggest positive variation
    direction='down' -> biggest drops
    """
    key = RedisKeys.rank_variacao(combustivel, "24h")
    if direction == "down":
        return [(m, float(s)) for m, s in r.zrange(key, 0, limit - 1, withscores=True)]
    return [(m, float(s)) for m, s in r.zrevrange(key, 0, limit - 1, withscores=True)]


def price_history(
    r: redis.Redis, posto_id: str, combustivel: str, bucket_min: int = 60
) -> List[Tuple[int, float]]:
    """TimeSeries of a single station/fuel. ``bucket_min`` aggregates by minutes."""
    key = RedisKeys.ts_posto_combustivel(posto_id, combustivel)
    try:
        rows = r.execute_command(
            "TS.RANGE",
            key,
            "-",
            "+",
            "AGGREGATION",
            "AVG",
            bucket_min * 60_000,
        )
    except redis.ResponseError:
        return []
    return [(int(t), float(v)) for t, v in rows]


def avg_price_uf(
    r: redis.Redis, combustivel: str, uf: str, bucket_min: int = 60
) -> List[Tuple[int, float]]:
    key = RedisKeys.ts_avg_uf(combustivel, uf)
    try:
        rows = r.execute_command(
            "TS.RANGE",
            key,
            "-",
            "+",
            "AGGREGATION",
            "AVG",
            bucket_min * 60_000,
        )
    except redis.ResponseError:
        return []
    return [(int(t), float(v)) for t, v in rows]


# ---------------------------------------------------------------------------
# Geo queries
# ---------------------------------------------------------------------------
def nearest_postos(
    r: redis.Redis,
    lat: float,
    lon: float,
    radius_km: float = 5,
    combustivel: Optional[str] = None,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """``GEOSEARCH`` para encontrar postos perto de um ponto."""
    key = RedisKeys.GEO_POSTOS if not combustivel else RedisKeys.geo_postos_combustivel(combustivel)
    try:
        results = r.execute_command(
            "GEOSEARCH",
            key,
            "FROMLONLAT", lon, lat,
            "BYRADIUS", radius_km, "km",
            "ASC",
            "COUNT", limit,
            "WITHCOORD", "WITHDIST",
        )
    except redis.ResponseError:
        return []
    out = []
    for row in results:
        # row = [member, distance_km, [lon, lat]]
        member, dist, (rlon, rlat) = row[0], float(row[1]), (float(row[2][0]), float(row[2][1]))
        meta = r.hgetall(RedisKeys.posto(member))
        if combustivel:
            preco = r.hget(RedisKeys.posto_precos(member), combustivel)
            if preco is not None:
                meta["preco"] = float(preco)
        out.append({"posto_id": member, "dist_km": dist, "lat": rlat, "lon": rlon, **meta})
    return out


# ---------------------------------------------------------------------------
# Searches & user behaviour
# ---------------------------------------------------------------------------
def top_buscas_bairros(r: redis.Redis, limit: int = 10) -> List[Tuple[str, float]]:
    rows = r.zrevrange(RedisKeys.RANK_BUSCAS_BAIRRO, 0, limit - 1, withscores=True)
    return [(m, float(s)) for m, s in rows]


def top_buscas_cidades(r: redis.Redis, limit: int = 10) -> List[Tuple[str, float]]:
    rows = r.zrevrange(RedisKeys.RANK_BUSCAS_CIDADE, 0, limit - 1, withscores=True)
    return [(m, float(s)) for m, s in rows]


def top_buscas_combustivel(r: redis.Redis, limit: int = 10) -> List[Tuple[str, float]]:
    rows = r.zrevrange(RedisKeys.RANK_BUSCAS_COMBUSTIVEL, 0, limit - 1, withscores=True)
    return [(m, float(s)) for m, s in rows]


def buscas_volume(r: redis.Redis, bucket_min: int = 60) -> List[Tuple[int, float]]:
    try:
        rows = r.execute_command(
            "TS.RANGE",
            RedisKeys.TS_BUSCAS_TOTAL,
            "-",
            "+",
            "AGGREGATION",
            "SUM",
            bucket_min * 60_000,
        )
    except redis.ResponseError:
        return []
    return [(int(t), float(v)) for t, v in rows]


def buscas_latencia(r: redis.Redis, bucket_min: int = 60) -> List[Tuple[int, float]]:
    try:
        rows = r.execute_command(
            "TS.RANGE",
            RedisKeys.TS_BUSCAS_LATENCIA,
            "-",
            "+",
            "AGGREGATION",
            "AVG",
            bucket_min * 60_000,
        )
    except redis.ResponseError:
        return []
    return [(int(t), float(v)) for t, v in rows]


# ---------------------------------------------------------------------------
# Avaliacoes & interacoes
# ---------------------------------------------------------------------------
def top_postos_rating(r: redis.Redis, limit: int = 10) -> List[Tuple[str, float]]:
    rows = r.zrevrange(RedisKeys.RANK_POSTOS_RATING, 0, limit - 1, withscores=True)
    return [(m, float(s)) for m, s in rows]


def top_postos_checkins(r: redis.Redis, limit: int = 10) -> List[Tuple[str, float]]:
    rows = r.zrevrange(RedisKeys.RANK_POSTOS_CHECKIN, 0, limit - 1, withscores=True)
    return [(m, float(s)) for m, s in rows]


# ---------------------------------------------------------------------------
# RediSearch: full-text + filters
# ---------------------------------------------------------------------------
def search_postos(
    r: redis.Redis,
    query: str = "*",
    uf: Optional[str] = None,
    bandeira: Optional[str] = None,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    parts = [query if query else "*"]
    if uf:
        parts.append(f"@uf:{{{uf}}}")
    if bandeira:
        parts.append(f"@bandeira:{{{bandeira}}}")
    q = " ".join(parts)
    try:
        raw = r.execute_command(
            "FT.SEARCH",
            RedisKeys.IDX_POSTOS,
            q,
            "LIMIT", 0, limit,
        )
    except redis.ResponseError:
        return []
    # FT.SEARCH returns: total, key1, [field, val, field, val, ...], key2, [...]
    out: List[Dict[str, Any]] = []
    for i in range(1, len(raw), 2):
        fields = raw[i + 1] if i + 1 < len(raw) else []
        d = {fields[j]: fields[j + 1] for j in range(0, len(fields), 2)}
        d["_key"] = raw[i]
        out.append(d)
    return out


# ---------------------------------------------------------------------------
# Pipeline metrics
# ---------------------------------------------------------------------------
def pipeline_metrics(r: redis.Redis) -> Dict[str, str]:
    return r.hgetall(RedisKeys.METRICS_HASH)


# ---------------------------------------------------------------------------
# CLI demo
# ---------------------------------------------------------------------------
def _print_table(title: str, rows, fmt=None) -> None:
    print()
    print(f"== {title} ==")
    if not rows:
        print("  (sem dados ainda)")
        return
    for row in rows:
        if fmt:
            print(" ", fmt(row))
        else:
            print(" ", row)


def main() -> None:
    r = get_redis()
    log.info("Conectado em redis://%s:%s/%s", REDIS_HOST, REDIS_PORT, REDIS_DB)

    metrics = pipeline_metrics(r)
    _print_table("Pipeline metrics", list(metrics.items()), lambda kv: f"{kv[0]:35s} {kv[1]}")

    for combustivel in ("GASOLINA_COMUM", "DIESEL_S10", "ETANOL", "GASOLINA_ADITIVADA"):
        rows = cheapest_postos(r, combustivel, limit=5)
        _print_table(f"Top 5 postos mais baratos - {combustivel}", rows, lambda kv: f"{kv[0]:30s} R$ {kv[1]:.3f}")

    rows = top_buscas_bairros(r, 10)
    _print_table("Top 10 bairros mais buscados (UF|Cidade|Bairro)", rows, lambda kv: f"{kv[0]:60s} {int(kv[1])}")

    rows = top_buscas_combustivel(r, 10)
    _print_table("Combustiveis mais buscados", rows, lambda kv: f"{kv[0]:25s} {int(kv[1])}")

    rows = top_postos_rating(r, 5)
    _print_table("Top 5 postos por avaliacao media", rows, lambda kv: f"{kv[0]:30s} {kv[1]:.2f}")

    rows = biggest_price_swings(r, "GASOLINA_COMUM", "up", 5)
    _print_table("Top 5 maiores ALTAS GASOLINA_COMUM (24h)", rows, lambda kv: f"{kv[0]:30s} {kv[1]:+.2f}%")

    rows = biggest_price_swings(r, "GASOLINA_COMUM", "down", 5)
    _print_table("Top 5 maiores QUEDAS GASOLINA_COMUM (24h)", rows, lambda kv: f"{kv[0]:30s} {kv[1]:+.2f}%")


if __name__ == "__main__":
    main()
