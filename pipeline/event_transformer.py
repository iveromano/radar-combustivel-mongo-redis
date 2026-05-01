from typing import Any, Dict


# =========================================================
# NORMALIZAÇÃO DE EVENTOS
# =========================================================

def normalize_event(
    raw: Dict[str, Any],
    source: str,
) -> Dict[str, Any]:

    """
    Normaliza eventos vindos das collections:

    - eventos_preco
    - avaliacoes_interacoes
    - buscas_usuarios
    """

    # =====================================================
    # EVENTOS DE PREÇO
    # =====================================================

    if source == "eventos_preco":

        posto_id = str(raw.get("posto_id", "")).strip()

        ocorrido_em = raw.get("ocorrido_em")

        ts = (
            int(ocorrido_em.timestamp() * 1000)
            if ocorrido_em
            else 0
        )

        if ts <= 0:
            raise ValueError(
                "Evento de preço sem timestamp válido."
            )

        return {
            "type": "price_update",
            "ts": ts,

            "posto_id": posto_id,

            "combustivel": str(
                raw.get("combustivel", "")
            ).upper(),

            "preco_anterior": float(
                raw.get("preco_anterior", 0.0)
            ),

            "preco_novo": float(
                raw.get("preco_novo", 0.0)
            ),

            "variacao_pct": float(
                raw.get("variacao_pct", 0.0)
            ),

            "fonte": str(
                raw.get("fonte", "")
            ),

            "revisado": bool(
                raw.get("revisado", False)
            ),
        }

    # =====================================================
    # AVALIAÇÕES / INTERAÇÕES
    # =====================================================

    if source == "avaliacoes_interacoes":

        posto_id = str(raw.get("posto_id", "")).strip()

        created_at = raw.get("created_at")

        ts = (
            int(created_at.timestamp() * 1000)
            if created_at
            else 0
        )

        if ts <= 0:
            raise ValueError(
                "Evento de avaliação sem timestamp válido."
            )

        interaction_type = str(
            raw.get("tipo", "")
        ).strip().lower()

        return {
            "type": interaction_type,
            "ts": ts,

            "posto_id": posto_id,

            "usuario_id": str(
                raw.get("usuario_id", "")
            ),

            "nota": (
                float(raw["nota"])
                if raw.get("nota") is not None
                else None
            ),

            "comentario": str(
                raw.get("comentario", "")
            ),

            "util_count": int(
                raw.get("util_count", 0)
            ),
        }

    # =====================================================
    # BUSCAS DE USUÁRIOS
    # =====================================================

    if source == "buscas_usuarios":

        consultado_em = raw.get("consultado_em")

        ts = (
            int(consultado_em.timestamp() * 1000)
            if consultado_em
            else 0
        )

        if ts <= 0:
            raise ValueError(
                "Evento de busca sem timestamp válido."
            )

        geo = raw.get("geo_centro", {})

        coords = geo.get(
            "coordinates",
            [0.0, 0.0],
        )

        lon = float(coords[0])
        lat = float(coords[1])

        return {
            "type": "search",
            "ts": ts,

            "usuario_id": str(
                raw.get("usuario_id", "")
            ),

            "session_id": str(
                raw.get("session_id", "")
            ),

            "tipo_combustivel": str(
                raw.get("tipo_combustivel", "")
            ).upper(),

            "cidade": str(
                raw.get("cidade", "")
            ),

            "estado": str(
                raw.get("estado", "")
            ),

            "raio_km": int(
                raw.get("raio_km", 0)
            ),

            "resultado_count": int(
                raw.get("resultado_count", 0)
            ),

            "latencia_ms": int(
                raw.get("latencia_ms", 0)
            ),

            "lat": lat,
            "lon": lon,
        }

    # =====================================================
    # SOURCE INVÁLIDO
    # =====================================================

    raise ValueError(
        f"Fonte de evento inválida: {source}"
    )


# =========================================================
# REDIS HASH KEY
# =========================================================

def hash_key(event: Dict[str, Any]) -> str:

    posto_id = event.get("posto_id")

    if not posto_id:
        raise ValueError(
            "Evento sem posto_id."
        )

    return f"posto:{posto_id}"


# =========================================================
# REDIS TIMESERIES KEY
# =========================================================

def ts_key(
    event: Dict[str, Any],
    metric: str,
) -> str:

    posto_id = event.get("posto_id")

    if not posto_id:
        raise ValueError(
            "Evento sem posto_id."
        )

    return f"ts:posto:{posto_id}:{metric}"


# =========================================================
# REDIS RANKING KEY
# =========================================================

def ranking_key(event: Dict[str, Any]) -> str:

    event_type = event.get("type")

    # -----------------------------------------------------
    # PREÇOS
    # -----------------------------------------------------

    if event_type == "price_update":

        combustivel = (
            event.get("combustivel", "")
            .lower()
        )

        return (
            f"ranking:combustivel:"
            f"{combustivel}:menor_preco"
        )

    # -----------------------------------------------------
    # BUSCAS
    # -----------------------------------------------------

    if event_type == "search":

        return (
            "ranking:combustivel:buscas"
        )

    # -----------------------------------------------------
    # AVALIAÇÕES
    # -----------------------------------------------------

    if event_type == "avaliacao":

        return (
            "ranking:postos:avaliacao"
        )

    # -----------------------------------------------------
    # COMPARTILHAMENTOS
    # -----------------------------------------------------

    if event_type == "compartilhamento":

        return (
            "ranking:postos:shares"
        )

    return ""