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


def posto_bairro(
    redis: Redis,
    posto_id: str,
) -> str:

    bairro = redis.hget(
        f"posto:{posto_id}",
        "bairro",
    )

    return bairro or "-"


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
# CONSULTAS PRINCIPAIS
# =========================================================

# ---------------------------------------------------------
# 1. POSTOS MAIS BARATOS POR REGIÃO
# ---------------------------------------------------------

def postos_mais_baratos(
    redis: Redis,
    combustivel: str,
    bairro: str,
    n: int = 10,
) -> List[Tuple[str, float]]:

    return redis.zrange(
        f"ranking:preco:{combustivel}:{bairro}",
        0,
        n - 1,
        withscores=True,
    )


# ---------------------------------------------------------
# 2. COMBUSTÍVEIS EM ALTA
# ---------------------------------------------------------

def combustiveis_em_alta(
    redis: Redis,
    n: int = 5,
) -> List[Tuple[str, float]]:

    return redis.zrevrange(
        "ranking:combustivel:buscas",
        0,
        n - 1,
        withscores=True,
    )


# ---------------------------------------------------------
# 3. BAIRROS COM MAIS BUSCAS
# ---------------------------------------------------------

def bairros_mais_buscados(
    redis: Redis,
    n: int = 10,
) -> List[Tuple[str, float]]:

    return redis.zrevrange(
        "ranking:buscas:bairro",
        0,
        n - 1,
        withscores=True,
    )


# ---------------------------------------------------------
# 4. MAIOR VARIAÇÃO DE PREÇO
# ---------------------------------------------------------

def maior_variacao_preco(
    redis: Redis,
    combustivel: str,
    n: int = 10,
) -> List[Tuple[str, float]]:

    return redis.zrevrange(
        f"ranking:variacao:{combustivel}",
        0,
        n - 1,
        withscores=True,
    )


# =========================================================
# REDISEARCH
# =========================================================

def search_postos(
    redis: Redis,
    bairro: str,
    combustivel_max: float,
    limit: int,
):

    query_parts = []

    if bairro.strip():

        query_parts.append(
            f"@bairro:{{{bairro}}}"
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
                "gasolina_comum",
                0,
                combustivel_max,
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
    combustivel: str,
):

    key = (
        f"ts:posto:{posto_id}:{combustivel}"
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


# =========================================================
# STREAMLIT CONFIG
# =========================================================

st.set_page_config(
    page_title="Radar Combustível Dashboard",
    layout="wide",
)

st.title(
    "⛽ Radar Combustível Dashboard"
)

st.caption(
    "Pipeline MongoDB → Redis em tempo real"
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
# FILTROS
# =========================================================

st.sidebar.header("Filtros")

bairro = st.sidebar.selectbox(
    "Bairro",
    [
        "Pinheiros",
        "Centro",
        "Moema",
        "Vila Mariana",
    ],
)

combustivel = st.sidebar.selectbox(
    "Combustível",
    [
        "gasolina_comum",
        "etanol",
        "diesel",
    ],
)

# =========================================================
# KPI
# =========================================================

col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)

try:

    total_postos = redis.dbsize()

except:

    total_postos = 0

col_kpi1.metric(
    "Objetos Redis",
    total_postos,
)

col_kpi2.metric(
    "Bairro selecionado",
    bairro,
)

col_kpi3.metric(
    "Combustível",
    combustivel,
)

col_kpi4.metric(
    "Atualização",
    "Tempo real",
)

# =========================================================
# 1. POSTOS MAIS BARATOS
# =========================================================

st.subheader(
    "⛽ Postos com menor preço por região"
)

baratos = postos_mais_baratos(
    redis,
    combustivel,
    bairro,
    10,
)

df_baratos = pd.DataFrame(
    baratos,
    columns=[
        "posto_id",
        "preco",
    ],
)

if df_baratos.empty:

    st.info(
        "Sem dados para ranking de preços."
    )

else:

    df_baratos["posto"] = (
        df_baratos["posto_id"]
        .apply(
            lambda x: posto_nome(
                redis,
                x,
            )
        )
    )

    df_baratos["bairro"] = (
        df_baratos["posto_id"]
        .apply(
            lambda x: posto_bairro(
                redis,
                x,
            )
        )
    )

    fig = px.bar(
        df_baratos.sort_values(
            "preco",
            ascending=False,
        ),
        x="preco",
        y="posto",
        orientation="h",
        title="Menores preços",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    st.dataframe(
        df_baratos,
        use_container_width=True,
        hide_index=True,
    )

# =========================================================
# 2. COMBUSTÍVEIS EM ALTA
# =========================================================

st.subheader(
    "🔥 Combustíveis em alta"
)

alta = combustiveis_em_alta(
    redis,
    5,
)

df_alta = pd.DataFrame(
    alta,
    columns=[
        "combustivel",
        "buscas",
    ],
)

if df_alta.empty:

    st.info(
        "Sem dados de buscas."
    )

else:

    fig = px.pie(
        df_alta,
        names="combustivel",
        values="buscas",
        title="Volume de buscas",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    st.dataframe(
        df_alta,
        use_container_width=True,
        hide_index=True,
    )

# =========================================================
# 3. BAIRROS MAIS BUSCADOS
# =========================================================

st.subheader(
    "📍 Bairros com maior volume de buscas"
)

bairros = bairros_mais_buscados(
    redis,
    10,
)

df_bairros = pd.DataFrame(
    bairros,
    columns=[
        "bairro",
        "buscas",
    ],
)

if df_bairros.empty:

    st.info(
        "Sem dados de bairros."
    )

else:

    fig = px.bar(
        df_bairros.sort_values(
            "buscas",
            ascending=True,
        ),
        x="buscas",
        y="bairro",
        orientation="h",
        title="Volume de buscas por bairro",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    st.dataframe(
        df_bairros,
        use_container_width=True,
        hide_index=True,
    )

# =========================================================
# 4. MAIOR VARIAÇÃO DE PREÇO
# =========================================================

st.subheader(
    "📈 Postos com maior variação recente"
)

variacoes = maior_variacao_preco(
    redis,
    combustivel,
    10,
)

df_var = pd.DataFrame(
    variacoes,
    columns=[
        "posto_id",
        "variacao",
    ],
)

if df_var.empty:

    st.info(
        "Sem dados de variação."
    )

else:

    df_var["posto"] = (
        df_var["posto_id"]
        .apply(
            lambda x: posto_nome(
                redis,
                x,
            )
        )
    )

    fig = px.bar(
        df_var.sort_values(
            "variacao",
            ascending=True,
        ),
        x="variacao",
        y="posto",
        orientation="h",
        title="Maior variação recente",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    st.dataframe(
        df_var,
        use_container_width=True,
        hide_index=True,
    )

# =========================================================
# 5. CONSULTAS EM TEMPO REAL
# =========================================================

st.subheader(
    "⚡ Consulta dinâmica em tempo real"
)

f1, f2, f3 = st.columns(3)

with f1:

    bairro_filter = st.text_input(
        "Bairro",
        value=bairro,
    )

with f2:

    preco_filter = st.slider(
        "Preço máximo",
        min_value=0.0,
        max_value=10.0,
        value=6.0,
        step=0.1,
    )

with f3:

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
        bairro_filter,
        float(preco_filter),
        int(limit_filter),
    )

    rows: List[Dict[str, Any]] = []

    for doc in result.docs:

        rows.append(
            {
                "id": doc.id,
                "nome": getattr(
                    doc,
                    "nome_fantasia",
                    "-",
                ),
                "bairro": getattr(
                    doc,
                    "bairro",
                    "-",
                ),
                "cidade": getattr(
                    doc,
                    "cidade",
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
            f"{result.total} resultado(s)"
        )

        st.dataframe(
            df_search,
            use_container_width=True,
            hide_index=True,
        )

except Exception as exc:

    st.error(
        f"Falha na busca dinâmica: {exc}"
    )

# =========================================================
# 6. SÉRIE TEMPORAL
# =========================================================

st.subheader(
    "📊 Evolução temporal de preços"
)

posto_id = st.text_input(
    "ID do posto",
    value="1",
)

try:

    series = price_series(
        redis,
        posto_id,
        combustivel,
    )

    if not series:

        st.info(
            "Sem dados de série temporal."
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
                f"Evolução de preços "
                f"{posto_id}"
            ),
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

        st.dataframe(
            df_series.tail(20),
            use_container_width=True,
            hide_index=True,
        )

except Exception as exc:

    st.error(
        f"Falha na série temporal: {exc}"
    )

# =========================================================
# AUTO REFRESH
# =========================================================

if auto_refresh:

    time.sleep(
        int(refresh_seconds)
    )

    st.rerun()
