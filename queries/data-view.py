"""
Streamlit dashboard - Radar Combustivel
=======================================

Five tabs, each backed by Redis-only queries (no MongoDB hit):

  1. Visao geral .... pipeline health + KPIs
  2. Precos ......... rankings, variacao, evolucao temporal
  3. Mapa & geo ..... GEOSEARCH com filtro por combustivel
  4. Comportamento .. top buscas por bairro / cidade / combustivel
  5. Avaliacoes ..... rankings de rating, check-ins, compartilhamento

Auto-refresh is controlled by the env var ``STREAMLIT_REFRESH_SECONDS``
(default 10s).

Run::

    streamlit run queries/data-view.py
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Dict, List

import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# Allow direct execution via `streamlit run queries/data-view.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import (  # noqa: E402
    REDIS_DB,
    REDIS_HOST,
    REDIS_PORT,
    STREAMLIT_REFRESH_SECONDS,
    RedisKeys,
)
from queries import redis_reader as rr  # noqa: E402


# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Radar Combustivel | Serving Layer",
    page_icon="⛽",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_redis_conn():
    return rr.get_redis()


r = get_redis_conn()


# ---------------------------------------------------------------------------
# Sidebar - global filters and controls
# ---------------------------------------------------------------------------
st.sidebar.title("Radar Combustivel")
st.sidebar.caption(f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")

auto_refresh = st.sidebar.toggle("Auto-refresh", value=True)
refresh_every = st.sidebar.slider(
    "Atualizar a cada (s)", 5, 60, STREAMLIT_REFRESH_SECONDS, step=5
)
if auto_refresh:
    st_autorefresh(interval=refresh_every * 1000, key="autorefresh")

combustiveis = ["GASOLINA_COMUM", "GASOLINA_ADITIVADA", "ETANOL", "DIESEL_S10", "DIESEL_S500", "GNV"]
combustivel = st.sidebar.selectbox("Combustivel", combustiveis, index=0)

ufs = ["", "SP", "RJ", "MG", "PR", "RS", "BA", "PE", "CE", "SC", "GO", "DF"]
uf = st.sidebar.selectbox("UF (filtro)", ufs, index=0) or None
cidade = st.sidebar.text_input("Cidade (opcional)") or None

st.sidebar.markdown("---")
st.sidebar.caption(
    "Pipeline streaming MongoDB -> Redis | "
    f"refresh = {refresh_every}s"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _ts_to_df(rows, value_col: str = "valor") -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["ts", value_col])
    df = pd.DataFrame(rows, columns=["ts", value_col])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    return df


def _hydrate_postos(ids: List[str]) -> Dict[str, Dict[str, str]]:
    return rr.hydrate_postos(r, ids)


def _format_posto_label(meta: Dict[str, str]) -> str:
    nome = meta.get("nome_fantasia") or meta.get("bandeira") or "Posto"
    cidade = meta.get("cidade") or meta.get("municipio") or ""
    uf_ = meta.get("estado") or meta.get("uf") or ""
    return f"{nome} ({cidade}/{uf_})" if cidade else nome


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_overview, tab_precos, tab_geo, tab_comp, tab_aval = st.tabs(
    ["Visao geral", "Precos", "Mapa & Geo", "Comportamento", "Avaliacoes"]
)


# ===========================================================================
# 1) Overview
# ===========================================================================
with tab_overview:
    st.subheader("Saude do pipeline")
    metrics = rr.pipeline_metrics(r)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Eventos processados", int(metrics.get("processed_total", 0) or 0))
    c2.metric("Erros", int(metrics.get("errors", 0) or 0))
    last_ts = int(metrics.get("last_event_ts", 0) or 0)
    last_human = (
        datetime.fromtimestamp(last_ts / 1000).strftime("%H:%M:%S") if last_ts else "--"
    )
    c3.metric("Ultimo evento (hora)", last_human)
    c4.metric("Ultima colecao", metrics.get("last_event_collection") or "--")

    st.markdown("**Eventos por colecao**")
    cols_data = [(k.replace("col:", ""), int(v)) for k, v in metrics.items() if k.startswith("col:")]
    if cols_data:
        df_cols = pd.DataFrame(cols_data, columns=["colecao", "eventos"])
        st.bar_chart(df_cols, x="colecao", y="eventos", height=240)
    else:
        st.info("Aguardando primeiros eventos do pipeline...")

    st.markdown("---")
    st.subheader("Volume de buscas (TimeSeries)")
    df_buscas = _ts_to_df(rr.buscas_volume(r, bucket_min=60), "buscas")
    if not df_buscas.empty:
        fig = px.area(df_buscas, x="ts", y="buscas", title="Buscas por hora")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem TimeSeries de buscas disponivel (rode o consumer).")

    st.subheader("Latencia media das buscas (ms)")
    df_lat = _ts_to_df(rr.buscas_latencia(r, bucket_min=60), "latencia_ms")
    if not df_lat.empty:
        fig = px.line(df_lat, x="ts", y="latencia_ms", title="Latencia media das buscas")
        st.plotly_chart(fig, use_container_width=True)


# ===========================================================================
# 2) Precos
# ===========================================================================
with tab_precos:
    st.subheader(f"Postos mais baratos - {combustivel}")

    rows = rr.cheapest_postos(r, combustivel, uf=uf, cidade=cidade, limit=20)
    posto_ids = [pid for pid, _ in rows]
    metas = _hydrate_postos(posto_ids)
    df = pd.DataFrame(
        [
            {
                "posto": _format_posto_label(metas.get(pid, {})),
                "bandeira": metas.get(pid, {}).get("bandeira", ""),
                "uf": metas.get(pid, {}).get("estado") or metas.get(pid, {}).get("uf"),
                "cidade": metas.get(pid, {}).get("cidade") or metas.get(pid, {}).get("municipio"),
                "preco": preco,
            }
            for pid, preco in rows
        ]
    )
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
        fig = px.bar(
            df.head(15),
            x="preco",
            y="posto",
            orientation="h",
            color="bandeira",
            title=f"Top 15 - menor preco | {combustivel}"
            + (f" | {uf}" if uf else "")
            + (f" / {cidade}" if cidade else ""),
        )
        fig.update_layout(yaxis={"categoryorder": "total descending"})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem dados ainda. Rode o consumer.")

    st.markdown("---")
    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Maiores ALTAS (24h)")
        rows = rr.biggest_price_swings(r, combustivel, "up", limit=10)
        metas = _hydrate_postos([p for p, _ in rows])
        df_up = pd.DataFrame(
            [{"posto": _format_posto_label(metas.get(p, {})), "variacao_pct": v} for p, v in rows]
        )
        if not df_up.empty:
            st.dataframe(df_up, use_container_width=True, hide_index=True)
        else:
            st.info("Sem variacoes registradas.")
    with col_b:
        st.subheader("Maiores QUEDAS (24h)")
        rows = rr.biggest_price_swings(r, combustivel, "down", limit=10)
        metas = _hydrate_postos([p for p, _ in rows])
        df_dw = pd.DataFrame(
            [{"posto": _format_posto_label(metas.get(p, {})), "variacao_pct": v} for p, v in rows]
        )
        if not df_dw.empty:
            st.dataframe(df_dw, use_container_width=True, hide_index=True)
        else:
            st.info("Sem variacoes registradas.")

    st.markdown("---")
    st.subheader("Evolucao do preco")
    rows = rr.cheapest_postos(r, combustivel, uf=uf, cidade=cidade, limit=10)
    if rows:
        metas = _hydrate_postos([p for p, _ in rows])
        labels = {p: _format_posto_label(metas.get(p, {})) for p, _ in rows}
        chosen = st.selectbox(
            "Posto",
            options=[p for p, _ in rows],
            format_func=lambda pid: labels.get(pid, pid),
        )
        bucket_min = st.slider("Granularidade (min)", 5, 240, 60, step=5, key="ts_bucket")
        history = rr.price_history(r, chosen, combustivel, bucket_min=bucket_min)
        df_hist = _ts_to_df(history, "preco")
        if not df_hist.empty:
            fig = px.line(df_hist, x="ts", y="preco", markers=True, title=f"{labels[chosen]} - {combustivel}")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem TimeSeries para este posto/combustivel.")

    if uf:
        st.subheader(f"Preco medio em {uf}")
        df_avg = _ts_to_df(rr.avg_price_uf(r, combustivel, uf, bucket_min=60), "preco_medio")
        if not df_avg.empty:
            fig = px.line(df_avg, x="ts", y="preco_medio", title=f"Media {combustivel} | {uf}")
            st.plotly_chart(fig, use_container_width=True)


# ===========================================================================
# 3) Mapa & Geo
# ===========================================================================
with tab_geo:
    st.subheader("Postos proximos (GEOSEARCH)")

    # Auto-center on the cidade filter when it is set: pick any posto in
    # the city and use its coordinates as the default. Falls back to
    # Sao Paulo - Praca da Se when no city is set or none has coords.
    default_lat, default_lon = -23.5505, -46.6333
    auto_msg = ""
    if cidade:
        candidates = r.zrange(
            RedisKeys.rank_preco_cidade(combustivel, cidade), 0, 9
        ) or []
        for pid in candidates:
            meta = r.hgetall(RedisKeys.posto(pid))
            try:
                default_lat = float(meta["lat"])
                default_lon = float(meta["lon"])
                auto_msg = f"Centralizado em {cidade} (posto {meta.get('nome_fantasia') or pid})"
                break
            except (KeyError, ValueError):
                continue
        if not auto_msg:
            auto_msg = f"Cidade '{cidade}' nao tem postos com coordenadas indexadas para {combustivel}."

    if auto_msg:
        st.caption(auto_msg)

    col1, col2, col3 = st.columns([1, 1, 1])
    # `key` muda quando o filtro muda, fazendo o Streamlit reiniciar o widget
    # com o novo `value`.
    geo_key = f"{cidade or 'sp'}|{uf or 'br'}|{combustivel}"
    lat = col1.number_input("Latitude", value=default_lat, format="%.4f", key=f"lat_{geo_key}")
    lon = col2.number_input("Longitude", value=default_lon, format="%.4f", key=f"lon_{geo_key}")
    default_radius = 25 if cidade else 5
    raio = col3.slider("Raio (km)", 1, 100, default_radius, key=f"raio_{geo_key}")

    only_with_fuel = st.checkbox(
        f"Apenas postos com {combustivel} indexado", value=False
    )

    postos = rr.nearest_postos(
        r,
        lat=lat,
        lon=lon,
        radius_km=raio,
        combustivel=combustivel if only_with_fuel else None,
        limit=200,
    )

    if not postos:
        st.info("Sem postos encontrados nesse raio.")
    else:
        df_geo = pd.DataFrame(postos)
        st.metric("Postos no raio", len(df_geo))
        layer_postos = pdk.Layer(
            "ScatterplotLayer",
            data=df_geo,
            get_position="[lon, lat]",
            get_radius=120,
            get_fill_color=[0, 153, 76, 200],
            pickable=True,
        )
        tooltip = {"text": "{nome_fantasia}\n{bandeira}\n{cidade}/{estado}\n{dist_km} km"}
        view = pdk.ViewState(latitude=lat, longitude=lon, zoom=11)
        deck = pdk.Deck(
            layers=[layer_postos],
            initial_view_state=view,
            tooltip=tooltip,
            map_style="mapbox://styles/mapbox/light-v9",
        )
        st.pydeck_chart(deck)
        st.dataframe(
            df_geo[
                [c for c in ["posto_id", "nome_fantasia", "bandeira", "cidade", "estado", "dist_km", "preco"] if c in df_geo.columns]
            ].sort_values("dist_km"),
            use_container_width=True,
            hide_index=True,
        )


# ===========================================================================
# 4) Comportamento (buscas)
# ===========================================================================
with tab_comp:
    # Os rankings de buscas guardam membros como "UF|Cidade|Bairro" e
    # "UF|Cidade", entao filtramos por prefixo.
    bairros_prefix = ""
    if uf and cidade:
        bairros_prefix = f"{uf}|{cidade}|"
    elif uf:
        bairros_prefix = f"{uf}|"

    cidades_prefix = f"{uf}|" if uf else ""

    titulo_filtro = []
    if uf:
        titulo_filtro.append(f"UF={uf}")
    if cidade:
        titulo_filtro.append(f"cidade={cidade}")
    sufixo = f" ({', '.join(titulo_filtro)})" if titulo_filtro else " (Brasil)"

    st.subheader(f"Top bairros mais buscados{sufixo}")

    # Buscamos mais para conseguir filtrar localmente sem perder o top.
    rows_all = rr.top_buscas_bairros(r, limit=500)
    if bairros_prefix:
        rows = [(m, s) for m, s in rows_all if m.startswith(bairros_prefix)][:20]
    else:
        rows = rows_all[:20]
    df_bairros = pd.DataFrame(rows, columns=["uf|cidade|bairro", "buscas"])
    if not df_bairros.empty:
        fig = px.bar(
            df_bairros.head(15),
            x="buscas",
            y="uf|cidade|bairro",
            orientation="h",
            title=f"Top 15 bairros{sufixo}",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(
            "Sem buscas para esse filtro. Tente sem cidade ou em outra UF."
            if titulo_filtro
            else "Sem dados de busca ainda."
        )

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader(f"Top cidades{sufixo}")
        rows_all = rr.top_buscas_cidades(r, limit=300)
        rows = (
            [(m, s) for m, s in rows_all if m.startswith(cidades_prefix)][:15]
            if cidades_prefix
            else rows_all[:15]
        )
        df = pd.DataFrame(rows, columns=["uf|cidade", "buscas"])
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("Sem cidades para esse filtro.")
    with col_b:
        st.subheader("Combustiveis mais buscados (global)")
        rows = rr.top_buscas_combustivel(r, limit=15)
        df = pd.DataFrame(rows, columns=["combustivel", "buscas"])
        if not df.empty:
            fig = px.pie(df, values="buscas", names="combustivel", hole=0.4)
            st.plotly_chart(fig, use_container_width=True)


# ===========================================================================
# 5) Avaliacoes & Interacoes
# ===========================================================================
with tab_aval:
    def _matches_filter(meta: Dict[str, str]) -> bool:
        """Filtra metas pelo UF/cidade da sidebar."""
        if uf:
            estado = (meta.get("estado") or meta.get("uf") or "").upper()
            if estado != uf.upper():
                return False
        if cidade:
            cid = (meta.get("cidade") or meta.get("municipio") or "").lower()
            if cidade.lower() not in cid:
                return False
        return True

    sufixo_aval = []
    if uf:
        sufixo_aval.append(f"UF={uf}")
    if cidade:
        sufixo_aval.append(f"cidade={cidade}")
    sufixo_aval = f" ({', '.join(sufixo_aval)})" if sufixo_aval else " (Brasil)"

    st.subheader(f"Top postos por avaliacao media{sufixo_aval}")

    # Pegamos o ranking global maior e filtramos pelos metadados em memoria.
    raw_rows = rr.top_postos_rating(r, limit=300)
    metas = _hydrate_postos([p for p, _ in raw_rows])
    rows = [
        (p, m)
        for p, m in raw_rows
        if _matches_filter(metas.get(p, {}))
    ][:20]

    df_rating = pd.DataFrame(
        [
            {
                "posto": _format_posto_label(metas.get(p, {})),
                "bandeira": metas.get(p, {}).get("bandeira", ""),
                "media": media,
            }
            for p, media in rows
        ]
    )
    if not df_rating.empty:
        st.dataframe(df_rating, use_container_width=True, hide_index=True)
        fig = px.bar(
            df_rating.head(15),
            x="media",
            y="posto",
            orientation="h",
            color="bandeira",
            title="Top 15 - avaliacao media",
        )
        fig.update_layout(yaxis={"categoryorder": "total descending"})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem avaliacoes ainda.")

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader(f"Top check-ins{sufixo_aval}")
        raw = rr.top_postos_checkins(r, limit=300)
        metas2 = _hydrate_postos([p for p, _ in raw])
        rows = [(p, v) for p, v in raw if _matches_filter(metas2.get(p, {}))][:15]
        df = pd.DataFrame(
            [
                {"posto": _format_posto_label(metas2.get(p, {})), "check_ins": int(v)}
                for p, v in rows
            ]
        )
        st.dataframe(df, use_container_width=True, hide_index=True)
    with col_b:
        st.subheader(f"Top util_count{sufixo_aval}")
        raw = r.zrevrange(RedisKeys.RANK_POSTOS_UTIL, 0, 299, withscores=True)
        metas3 = _hydrate_postos([m for m, _ in raw])
        rows = [(m, v) for m, v in raw if _matches_filter(metas3.get(m, {}))][:15]
        df = pd.DataFrame(
            [
                {"posto": _format_posto_label(metas3.get(m, {})), "util": int(v)}
                for m, v in rows
            ]
        )
        st.dataframe(df, use_container_width=True, hide_index=True)


st.caption(
    "Pipeline MongoDB -> Redis | RediSearch + Sorted Sets + Geo + TimeSeries | "
    f"Render em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
)
