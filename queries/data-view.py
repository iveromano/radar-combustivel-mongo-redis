import os
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from redis import Redis
from redis.commands.search.query import NumericFilter, Query

# =========================================================
# ENVIRONMENT
# =========================================================

load_dotenv(".env.local")
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# =========================================================
# REDIS CONNECTION
# =========================================================

def get_redis() -> Redis:

    return Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )


# =========================================================
# HELPERS
# =========================================================

def posto_nome(
    redis: Redis,
    posto_id: str,
) -> str:

    nome = redis.hget(
        f"posto:{posto_id}",
        "nome_fantasia",
    )

    return nome or posto_id


def posto_bandeira(
    redis: Redis,
    posto_id: str,
) -> str:

    bandeira = redis.hget(
        f"posto:{posto_id}",
        "bandeira",
    )

    return bandeira or "-"


def posto_cidade(
    redis: Redis,
    posto_id: str,
) -> str:

    cidade = redis.hget(
        f"posto:{posto_id}",
        "cidade",
    )

    return cidade or "-"


# =========================================================
# RANKINGS
# =========================================================

def top_postos_avaliados(
    redis: Redis,
    n: int = 10,
) -> List[Tuple[str, float]]:

    return redis.zrevrange(
        "ranking:postos:avaliacao",
        0,
        n - 1,
        withscores=True,
    )


def top_postos_compartilhados(
    redis: Redis,
    n: int = 10,
) -> List[Tuple[str, float]]:

    return redis.zrevrange(
        "ranking:postos:shares",
        0,
        n - 1,
        withscores=True,
    )


def combustiveis_mais_buscados(
    redis: Redis,
    n: int = 5,
) -> List[Tuple[str, float]]:

    return redis.zrevrange(
        "ranking:combustivel:buscas",
        0,
        n - 1,
        withscores=True,
    )


def gasolina_mais_barata(
    redis: Redis,
    n: int = 10,
) -> List[Tuple[str, float]]:

    return redis.zrange(
        "ranking:combustivel:gasolina_comum:menor_preco",
        0,
        n - 1,
        withscores=True,
    )


# =========================================================
# REDISEARCH
# =========================================================

def top_rated_postos(
    redis: Redis,
    n: int = 10,
):

    query = (
        Query("*")
        .sort_by(
            "media_avaliacao",
            asc=False,
        )
        .paging(0, n)
    )

    return redis.ft(
        "idx:postos"
    ).search(query)


def search_postos(
    redis: Redis,
    bandeira: str,
    estado: str,
    min_rating: float,
    max_gasolina: float,
    limit: int,
):

    query_parts = []

    if bandeira.strip():

        query_parts.append(
            f"@bandeira:{{{bandeira}}}"
        )

    if estado.strip():

        query_parts.append(
            f"@estado:{{{estado}}}"
        )

    query_text = (
        " ".join(query_parts)
        if query_parts
        else "*"
    )

    query = (
        Query(query_text)
        .add_filter(
            NumericFilter(
                "media_avaliacao",
                min_rating,
                5,
            )
        )
        .add_filter(
            NumericFilter(
                "gasolina_comum",
                0,
                max_gasolina,
            )
        )
        .sort_by(
            "gasolina_comum",
            asc=True,
        )
        .paging(0, limit)
    )

    return redis.ft(
        "idx:postos"
    ).search(query)


# =========================================================
# REDISTIMESERIES
# =========================================================

def price_series(
    redis: Redis,
    posto_id: str,
):

    key = (
        f"ts:posto:{posto_id}:price_updates"
    )

    return redis.execute_command(
        "TS.RANGE",
        key,
        "-",
        "+",
        "AGGREGATION",
        "avg",
        "60000",
    )


def shares_series(
    redis: Redis,
    posto_id: str,
):

    key = (
        f"ts:posto:{posto_id}:shares"
    )

    return redis.execute_command(
        "TS.RANGE",
        key,
        "-",
        "+",
        "AGGREGATION",
        "sum",
        "60000",
    )


# =========================================================
# STREAMLIT CONFIG
# =========================================================

st.set_page_config(
    page_title="Radar Combustível Dashboard",
    layout="wide",
)

st.title(
    "⛽ Radar Combustível — Redis Dashboard"
)

st.caption(
    "Visualização em tempo real "
    "MongoDB -> Redis"
)

auto_refresh = st.sidebar.toggle(
    "Auto-refresh",
    value=True,
)

refresh_seconds = st.sidebar.number_input(
    "Intervalo (segundos)",
    min_value=1,
    max_value=60,
    value=5,
    step=1,
)

redis = get_redis()

# =========================================================
# TOP RANKINGS
# =========================================================

col1, col2 = st.columns(2)

# ---------------------------------------------------------
# TOP AVALIADOS
# ---------------------------------------------------------

with col1:

    st.subheader(
        "⭐ Top 10 postos mais bem avaliados"
    )

    postos = top_postos_avaliados(
        redis,
        10,
    )

    df_postos = pd.DataFrame(
        postos,
        columns=[
            "posto_id",
            "rating",
        ],
    )

    if df_postos.empty:

        st.info(
            "Sem dados em ranking:postos:avaliacao"
        )

    else:

        df_postos["nome"] = (
            df_postos["posto_id"]
            .apply(
                lambda x: posto_nome(
                    redis,
                    x,
                )
            )
        )

        fig = px.bar(
            df_postos.sort_values(
                "rating",
                ascending=True,
            ),
            x="rating",
            y="nome",
            orientation="h",
            title="Top avaliações",
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

        st.dataframe(
            df_postos,
            use_container_width=True,
            hide_index=True,
        )

# ---------------------------------------------------------
# TOP COMBUSTÍVEIS BUSCADOS
# ---------------------------------------------------------

with col2:

    st.subheader(
        "🔎 Combustíveis mais buscados"
    )

    combustiveis = (
        combustiveis_mais_buscados(
            redis,
            5,
        )
    )

    df_comb = pd.DataFrame(
        combustiveis,
        columns=[
            "combustivel",
            "buscas",
        ],
    )

    if df_comb.empty:

        st.info(
            "Sem dados de buscas."
        )

    else:

        fig = px.pie(
            df_comb,
            names="combustivel",
            values="buscas",
            title="Distribuição de buscas",
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

        st.dataframe(
            df_comb,
            use_container_width=True,
            hide_index=True,
        )

# =========================================================
# GASOLINA MAIS BARATA
# =========================================================

st.subheader(
    "⛽ Top 10 gasolina comum mais barata"
)

gasolina = gasolina_mais_barata(
    redis,
    10,
)

df_gas = pd.DataFrame(
    gasolina,
    columns=[
        "posto_id",
        "preco",
    ],
)

if df_gas.empty:

    st.info(
        "Sem ranking de gasolina."
    )

else:

    df_gas["posto"] = (
        df_gas["posto_id"]
        .apply(
            lambda x: posto_nome(
                redis,
                x,
            )
        )
    )

    df_gas["cidade"] = (
        df_gas["posto_id"]
        .apply(
            lambda x: posto_cidade(
                redis,
                x,
            )
        )
    )

    fig = px.bar(
        df_gas.sort_values(
            "preco",
            ascending=False,
        ),
        x="preco",
        y="posto",
        orientation="h",
        title="Menores preços gasolina",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    st.dataframe(
        df_gas,
        use_container_width=True,
        hide_index=True,
    )

# =========================================================
# TOP POSTOS REDISEARCH
# =========================================================

st.subheader(
    "🏆 Top postos via RediSearch"
)

try:

    rated = top_rated_postos(
        redis,
        10,
    )

    rows: List[Dict[str, Any]] = []

    for doc in rated.docs:

        rows.append(
            {
                "id": doc.id,
                "nome": getattr(
                    doc,
                    "nome_fantasia",
                    "-",
                ),
                "bandeira": getattr(
                    doc,
                    "bandeira",
                    "-",
                ),
                "cidade": getattr(
                    doc,
                    "cidade",
                    "-",
                ),
                "avaliacao": float(
                    getattr(
                        doc,
                        "media_avaliacao",
                        0,
                    )
                ),
                "gasolina": float(
                    getattr(
                        doc,
                        "gasolina_comum",
                        0,
                    )
                ),
            }
        )

    df_rated = pd.DataFrame(rows)

    if df_rated.empty:

        st.info(
            "Sem dados no idx:postos"
        )

    else:

        fig = px.bar(
            df_rated.sort_values(
                "avaliacao",
                ascending=True,
            ),
            x="avaliacao",
            y="nome",
            orientation="h",
            title="Top avaliações",
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

        st.dataframe(
            df_rated,
            use_container_width=True,
            hide_index=True,
        )

except Exception as exc:

    st.error(
        f"Falha RediSearch: {exc}"
    )

# =========================================================
# BUSCA DINÂMICA
# =========================================================

st.subheader(
    "🔍 Busca dinâmica de postos"
)

f1, f2, f3, f4, f5 = st.columns(5)

with f1:

    bandeira_filter = st.text_input(
        "Bandeira",
        value="Ipiranga",
    )

with f2:

    estado_filter = st.text_input(
        "Estado",
        value="SP",
    )

with f3:

    min_rating_filter = st.slider(
        "Nota mínima",
        min_value=0.0,
        max_value=5.0,
        value=4.0,
        step=0.1,
    )

with f4:

    gasolina_filter = st.slider(
        "Gasolina máxima",
        min_value=0.0,
        max_value=10.0,
        value=6.0,
        step=0.1,
    )

with f5:

    limit_filter = st.number_input(
        "Limite",
        min_value=1,
        max_value=50,
        value=10,
        step=1,
    )

try:

    result = search_postos(
        redis,
        bandeira_filter,
        estado_filter,
        float(min_rating_filter),
        float(gasolina_filter),
        int(limit_filter),
    )

    rows = []

    for doc in result.docs:

        rows.append(
            {
                "id": doc.id,
                "nome": getattr(
                    doc,
                    "nome_fantasia",
                    "-",
                ),
                "bandeira": getattr(
                    doc,
                    "bandeira",
                    "-",
                ),
                "cidade": getattr(
                    doc,
                    "cidade",
                    "-",
                ),
                "avaliacao": getattr(
                    doc,
                    "media_avaliacao",
                    "-",
                ),
                "gasolina": getattr(
                    doc,
                    "gasolina_comum",
                    "-",
                ),
            }
        )

    df_search = pd.DataFrame(rows)

    if df_search.empty:

        st.info(
            "Nenhum resultado encontrado."
        )

    else:

        st.caption(
            f"{result.total} resultado(s)."
        )

        st.dataframe(
            df_search,
            use_container_width=True,
            hide_index=True,
        )

except Exception as exc:

    st.error(
        f"Falha na busca: {exc}"
    )

# =========================================================
# TIMESERIES
# =========================================================

st.subheader(
    "📈 Série temporal de preços"
)

posto_id = st.text_input(
    "ID do posto",
    value="1",
)

try:

    series = price_series(
        redis,
        posto_id,
    )

    if not series:

        st.info(
            f"Sem dados para "
            f"ts:posto:{posto_id}:price_updates"
        )

    else:

        df_series = pd.DataFrame(
            series,
            columns=[
                "ts",
                "preco",
            ],
        )

        df_series["datetime"] = (
            df_series["ts"]
            .apply(
                lambda v: datetime.fromtimestamp(
                    v / 1000.0
                )
            )
        )

        fig = px.line(
            df_series,
            x="datetime",
            y="preco",
            markers=True,
            title=(
                f"Posto {posto_id}"
            ),
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

        st.dataframe(
            df_series.tail(15),
            use_container_width=True,
            hide_index=True,
        )

except Exception as exc:

    st.error(
        f"Falha na TimeSeries: {exc}"
    )

# =========================================================
# AUTO REFRESH
# =========================================================

if auto_refresh:

    time.sleep(
        int(refresh_seconds)
    )

    st.rerun()