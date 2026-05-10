import pandas as pd
import streamlit as st

from redis_reader import (
    cidades_mais_buscadas,
    combustiveis_em_alta,
    get_redis,
    maior_variacao_preco,
    posto_bairro,
    posto_cidade,
    posto_nome,
    postos_mais_baratos_cidade,
    postos_mais_baratos_uf,
    postos_mais_engajados,
    postos_proximos,
)

st.set_page_config(page_title='Radar Combustível', layout='wide')
st.title('Radar Combustível')
st.caption('Dashboard de consultas servidas pelo Redis.')

redis = get_redis()
combustivel = st.sidebar.selectbox('Combustível', ['gasolina_comum', 'etanol', 'diesel_s10', 'gnv'])
uf = st.sidebar.text_input('UF', 'sp').strip().lower().replace(' ', '_')
cidade = st.sidebar.text_input('Cidade', 'alves').strip().lower().replace(' ', '_')
limite = st.sidebar.slider('Top N', 3, 20, 10)

k1, k2, k3, k4 = st.columns(4)
k1.metric('Postos indexados', len(redis.keys('posto:*')))
k2.metric('Eventos de preço', redis.dbsize())
k3.metric('Chave GEO', 'geo:postos')
k4.metric('Índice search', 'idx:postos')

aba1, aba2, aba3, aba4, aba5 = st.tabs([
    'Menor preço por UF',
    'Menor preço por cidade',
    'Variação e buscas',
    'Engajamento',
    'Proximidade',
])

with aba1:
    rows = postos_mais_baratos_uf(redis, combustivel, uf, limite)
    df = pd.DataFrame([{'posto_id': pid, 'nome': posto_nome(redis, pid), 'bairro': posto_bairro(redis, pid), 'cidade': posto_cidade(redis, pid), 'preco': score} for pid, score in rows])
    st.dataframe(df, use_container_width=True)

with aba2:
    rows = postos_mais_baratos_cidade(redis, combustivel, cidade, limite)
    df = pd.DataFrame([{'posto_id': pid, 'nome': posto_nome(redis, pid), 'bairro': posto_bairro(redis, pid), 'cidade': posto_cidade(redis, pid), 'preco': score} for pid, score in rows])
    st.dataframe(df, use_container_width=True)

with aba3:
    rows = maior_variacao_preco(redis, combustivel, limite)
    df = pd.DataFrame([{'posto_id': pid, 'nome': posto_nome(redis, pid), 'variacao_score': score} for pid, score in rows])
    st.subheader('Maior variação recente')
    st.dataframe(df, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        alta = combustiveis_em_alta(redis, limite)
        df_alta = pd.DataFrame([{'combustivel': c, 'buscas': score} for c, score in alta])
        st.subheader('Combustíveis em alta')
        if not df_alta.empty:
            st.bar_chart(df_alta.set_index('combustivel'))
    with col2:
        buscadas = cidades_mais_buscadas(redis, limite)
        df_busca = pd.DataFrame([{'cidade_estado': item, 'buscas': score} for item, score in buscadas])
        st.subheader('Cidades mais buscadas')
        if not df_busca.empty:
            st.bar_chart(df_busca.set_index('cidade_estado'))

with aba4:
    rows = postos_mais_engajados(redis, limite)
    df = pd.DataFrame([{'posto_id': pid, 'nome': posto_nome(redis, pid), 'engajamento': score} for pid, score in rows])
    st.dataframe(df, use_container_width=True)

with aba5:
    lat = st.number_input('Latitude', value=-23.550520, format='%.6f')
    lon = st.number_input('Longitude', value=-46.633308, format='%.6f')
    raio = st.slider('Raio (km)', 1, 50, 5)
    rows = postos_proximos(redis, lon, lat, raio, limite)
    df = pd.DataFrame([{'posto_id': pid, 'nome': posto_nome(redis, pid), 'bairro': posto_bairro(redis, pid), 'cidade': posto_cidade(redis, pid)} for pid in rows])
    st.dataframe(df, use_container_width=True)
