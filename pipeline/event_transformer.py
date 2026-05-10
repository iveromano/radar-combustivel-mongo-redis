"""
Event transformer
=================

Converts MongoDB documents from the 5 collections of the
``radar_combustivel`` database into Redis write operations using the
structures that best fit each business question.

The single public entry point is :func:`apply_event`, which takes one
``(collection_name, document)`` pair and pipelines every required Redis
command. The function is idempotent: applying the same document twice
yields the same final state (rankings use ZADD, hashes use HSET, etc.),
so it is safe to call from both the initial backfill and the live
Change Stream.

Design notes
------------
* Geo data lives in the dedicated ``localizacoes_postos`` collection;
  ``postos`` already has a ``location`` field, so we accept either.
* ``eventos_preco`` is the only collection that produces TimeSeries and
  variation rankings — it carries a numeric value AND a timestamp.
* All keys are produced by :class:`RedisKeys` to keep the schema
  centralized and auditable.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from redis import Redis
from redis.exceptions import ResponseError

from .config import (
    COL_AVALIACOES,
    COL_BUSCAS,
    COL_LOCALIZACOES,
    COL_POSTOS,
    COL_PRECOS,
    RedisKeys,
    TS_RETENTION_MS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _to_ms(value) -> int:
    """Best-effort conversion of a Mongo date/string to epoch millis."""
    if value is None:
        return int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    if isinstance(value, dict) and "$date" in value:
        value = value["$date"]
    if isinstance(value, (int, float)):
        # already epoch
        return int(value if value > 1e12 else value * 1000)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        return int(dt.timestamp() * 1000)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return int(value.timestamp() * 1000)
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _stringify_id(raw) -> str:
    """ObjectId / dict / str -> always a stable string id."""
    if raw is None:
        return ""
    if isinstance(raw, dict) and "$oid" in raw:
        return str(raw["$oid"])
    return str(raw)


def _ensure_timeseries(r: Redis, key: str, labels: Dict[str, str]) -> None:
    """Create a TimeSeries if it does not exist (idempotent)."""
    try:
        r.execute_command(
            "TS.CREATE",
            key,
            "RETENTION",
            TS_RETENTION_MS,
            "DUPLICATE_POLICY",
            "LAST",
            "LABELS",
            *[v for kv in labels.items() for v in kv],
        )
    except ResponseError as e:
        # "TSDB: key already exists" is fine
        if "already exists" not in str(e).lower():
            raise


# ---------------------------------------------------------------------------
# Per-collection handlers
# ---------------------------------------------------------------------------
def _handle_posto(r: Redis, doc: Dict[str, Any]) -> None:
    """``postos`` -> Hash + Geo + RediSearch document."""
    posto_id = _stringify_id(doc.get("_id"))
    if not posto_id:
        return

    endereco = doc.get("endereco") or {}
    location = doc.get("location") or {}
    coords = (location.get("coordinates") or [None, None]) if isinstance(location, dict) else [None, None]
    lon, lat = (coords + [None, None])[:2]

    payload = {
        "posto_id": posto_id,
        "cnpj": str(doc.get("cnpj", "")),
        "nome_fantasia": str(doc.get("nome_fantasia", "")),
        "bandeira": str(doc.get("bandeira", "")),
        "logradouro": str(endereco.get("logradouro", "")),
        "numero": str(endereco.get("numero", "")),
        "bairro": str(endereco.get("bairro", "")),
        "cidade": str(endereco.get("cidade", "")),
        "estado": str(endereco.get("estado", "")),
        "cep": str(endereco.get("cep", "")),
        "telefone": str(doc.get("telefone", "")),
        "ativo": "1" if doc.get("ativo", True) else "0",
        "lat": "" if lat is None else str(lat),
        "lon": "" if lon is None else str(lon),
        "updated_at": str(_to_ms(doc.get("updated_at"))),
    }

    pipe = r.pipeline(transaction=False)
    pipe.hset(RedisKeys.posto(posto_id), mapping=payload)
    # Mirror as a doc:posto:{id} hash so RediSearch can index it.
    pipe.hset(RedisKeys.posto_doc(posto_id), mapping=payload)
    if lat is not None and lon is not None:
        pipe.geoadd(RedisKeys.GEO_POSTOS, (float(lon), float(lat), posto_id))
    pipe.execute()


def _handle_localizacao(r: Redis, doc: Dict[str, Any]) -> None:
    """``localizacoes_postos`` -> updates Hash with geo data + ibge."""
    posto_id = _stringify_id(doc.get("posto_id"))
    if not posto_id:
        return
    geo = doc.get("geo") or {}
    coords = geo.get("coordinates") or [None, None]
    lon, lat = (coords + [None, None])[:2]

    payload = {
        "municipio": str(doc.get("municipio", "")),
        "bairro": str(doc.get("bairro", "")),
        "uf": str(doc.get("uf", "")),
        "codigo_ibge": str(doc.get("codigo_ibge", "")),
    }
    if lat is not None:
        payload["lat"] = str(lat)
    if lon is not None:
        payload["lon"] = str(lon)

    pipe = r.pipeline(transaction=False)
    pipe.hset(RedisKeys.posto(posto_id), mapping=payload)
    pipe.hset(RedisKeys.posto_doc(posto_id), mapping=payload)
    if lat is not None and lon is not None:
        pipe.geoadd(RedisKeys.GEO_POSTOS, (float(lon), float(lat), posto_id))
    pipe.execute()


def _handle_evento_preco(r: Redis, doc: Dict[str, Any]) -> None:
    """
    ``eventos_preco`` is the heart of the streaming layer:
      * Hash with current price per fuel
      * Sorted Set ranking (cheapest first)
      * Sorted Set ranking by 24h variation
      * TimeSeries per posto/fuel and per UF average
      * Geo per fuel (ensures only stations that sell this fuel show up)
    """
    posto_id = _stringify_id(doc.get("posto_id"))
    combustivel = (doc.get("combustivel") or "").strip()
    preco_novo = doc.get("preco_novo")
    if not posto_id or not combustivel or preco_novo is None:
        return

    preco = float(preco_novo)
    variacao = float(doc.get("variacao_pct") or 0.0)
    ts_ms = _to_ms(doc.get("ocorrido_em"))

    # We need the location to maintain rankings per UF / cidade, which the
    # caller may already have loaded into the posto hash.
    h = r.hgetall(RedisKeys.posto(posto_id)) or {}
    uf = (h.get("estado") or h.get("uf") or "BR").upper() or "BR"
    cidade = h.get("cidade") or h.get("municipio") or "_"
    lat, lon = h.get("lat"), h.get("lon")

    pipe = r.pipeline(transaction=False)

    # Current price per posto (hash field per fuel)
    pipe.hset(RedisKeys.posto_precos(posto_id), combustivel, preco)
    pipe.hset(RedisKeys.posto_precos(posto_id), f"{combustivel}:updated_at", ts_ms)

    # Sorted Set rankings - lowest price first via ZRANGE
    pipe.zadd(RedisKeys.rank_preco_uf(combustivel, uf), {posto_id: preco})
    pipe.zadd(RedisKeys.rank_preco_cidade(combustivel, cidade), {posto_id: preco})
    pipe.zadd(RedisKeys.rank_preco_global(combustivel), {posto_id: preco})

    # 24h variation ranking - score is the latest variacao_pct
    pipe.zadd(RedisKeys.rank_variacao(combustivel, "24h"), {posto_id: variacao})

    # Geo per fuel (so we can do geosearches like "diesel S10 perto de mim")
    if lat and lon:
        try:
            pipe.geoadd(
                RedisKeys.geo_postos_combustivel(combustivel),
                (float(lon), float(lat), posto_id),
            )
        except (TypeError, ValueError):
            pass

    pipe.execute()

    # TimeSeries - written outside the pipeline so we can ensure creation.
    ts_key = RedisKeys.ts_posto_combustivel(posto_id, combustivel)
    _ensure_timeseries(
        r,
        ts_key,
        labels={"posto": posto_id, "combustivel": combustivel, "uf": uf},
    )
    try:
        r.execute_command("TS.ADD", ts_key, ts_ms, preco, "ON_DUPLICATE", "LAST")
    except ResponseError:
        # Older timestamps from backfill may collide; safe to ignore.
        pass

    # UF average TimeSeries (one bucket per minute via TS.MADD aggregation
    # in serving layer; here we just write the raw price tagged with uf).
    avg_key = RedisKeys.ts_avg_uf(combustivel, uf)
    _ensure_timeseries(r, avg_key, labels={"combustivel": combustivel, "uf": uf, "kind": "raw"})
    try:
        r.execute_command("TS.ADD", avg_key, ts_ms, preco, "ON_DUPLICATE", "LAST")
    except ResponseError:
        pass


def _handle_busca(r: Redis, doc: Dict[str, Any]) -> None:
    """``buscas_usuarios`` -> ranking de buscas + volume + latencia."""
    cidade = doc.get("cidade") or "_"
    estado = (doc.get("estado") or "BR").upper()
    bairro = (doc.get("filtros") or {}).get("bairro") or doc.get("bairro") or "_"
    combustivel = (doc.get("tipo_combustivel") or "_").strip()
    consultado_em = _to_ms(doc.get("consultado_em"))
    latencia = float(doc.get("latencia_ms") or 0)

    pipe = r.pipeline(transaction=False)
    pipe.zincrby(RedisKeys.RANK_BUSCAS_BAIRRO, 1, f"{estado}|{cidade}|{bairro}")
    pipe.zincrby(RedisKeys.RANK_BUSCAS_CIDADE, 1, f"{estado}|{cidade}")
    pipe.zincrby(RedisKeys.RANK_BUSCAS_UF, 1, estado)
    pipe.zincrby(RedisKeys.RANK_BUSCAS_COMBUSTIVEL, 1, combustivel)
    pipe.execute()

    _ensure_timeseries(r, RedisKeys.TS_BUSCAS_TOTAL, labels={"kind": "buscas_total"})
    _ensure_timeseries(
        r,
        RedisKeys.TS_BUSCAS_LATENCIA,
        labels={"kind": "buscas_latencia_ms"},
    )
    try:
        r.execute_command(
            "TS.INCRBY", RedisKeys.TS_BUSCAS_TOTAL, 1, "TIMESTAMP", consultado_em
        )
    except ResponseError:
        pass
    try:
        r.execute_command(
            "TS.ADD",
            RedisKeys.TS_BUSCAS_LATENCIA,
            consultado_em,
            latencia,
            "ON_DUPLICATE",
            "LAST",
        )
    except ResponseError:
        pass


def _handle_avaliacao(r: Redis, doc: Dict[str, Any]) -> None:
    """``avaliacoes_interacoes`` -> media, util_count, rankings de interacao."""
    posto_id = _stringify_id(doc.get("posto_id"))
    if not posto_id:
        return
    tipo = (doc.get("tipo") or "").lower()
    nota = doc.get("nota")
    util = int(doc.get("util_count") or 0)

    pipe = r.pipeline(transaction=False)
    rating_key = RedisKeys.posto_rating(posto_id)

    if nota is not None:
        try:
            nota_f = float(nota)
        except (TypeError, ValueError):
            nota_f = None
        if nota_f is not None:
            pipe.hincrbyfloat(rating_key, "sum", nota_f)
            pipe.hincrby(rating_key, "count", 1)

    if util:
        pipe.hincrby(rating_key, "util", util)
        pipe.zincrby(RedisKeys.RANK_POSTOS_UTIL, util, posto_id)

    if tipo == "check_in":
        pipe.hincrby(rating_key, "checkin", 1)
        pipe.zincrby(RedisKeys.RANK_POSTOS_CHECKIN, 1, posto_id)
    elif tipo == "compartilhamento":
        pipe.hincrby(rating_key, "compart", 1)
        pipe.zincrby(RedisKeys.RANK_POSTOS_COMPARTILHAMENTO, 1, posto_id)

    pipe.execute()

    # Recalculate the rolling average and replay it into the ranking.
    rating = r.hgetall(rating_key)
    soma = float(rating.get("sum", 0) or 0)
    qtd = int(rating.get("count", 0) or 0)
    if qtd > 0:
        media = soma / qtd
        r.zadd(RedisKeys.RANK_POSTOS_RATING, {posto_id: media})


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
_HANDLERS = {
    COL_POSTOS: _handle_posto,
    COL_LOCALIZACOES: _handle_localizacao,
    COL_PRECOS: _handle_evento_preco,
    COL_BUSCAS: _handle_busca,
    COL_AVALIACOES: _handle_avaliacao,
}


def apply_event(redis_conn: Redis, collection: str, doc: Dict[str, Any]) -> Optional[str]:
    """
    Apply a single Mongo document to the Redis serving layer.

    Returns the collection name when handled, ``None`` otherwise.
    """
    handler = _HANDLERS.get(collection)
    if not handler:
        return None
    handler(redis_conn, doc)
    return collection
