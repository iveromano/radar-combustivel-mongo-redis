# mongodb_consumer.py

import time
from bson import ObjectId
from pymongo import MongoClient
from redis import Redis
from redis.exceptions import ResponseError

from config import BATCH_SIZE, MONGO_DB, MONGO_URI, POLL_INTERVAL_SECONDS, REDIS_HOST, REDIS_PORT
from event_transformer import (
    hash_key,
    normalize_event,
    ranking_preco_cidade_key,
    ranking_preco_uf_key,
    ranking_variacao_key,
    ts_key,
)

COL_EVENTOS_PRECO = 'eventos_preco'
COL_AVALIACOES = 'avaliacoes_interacoes'
COL_BUSCAS = 'buscas_usuario'
COL_POSTOS = 'postos'
COL_LOCALIZACOES = 'localizacoes_postos'


def ensure_ts_add(redis: Redis, key: str, ts: int, value: float, labels: dict) -> None:
    try:
        redis.execute_command('TS.ADD', key, ts, value, 'ON_DUPLICATE', 'LAST')
    except ResponseError:
        redis.execute_command('TS.CREATE', key, 'RETENTION', 604800000, 'DUPLICATE_POLICY', 'LAST', 'LABELS', *sum(([k, v] for k, v in labels.items()), []))
        redis.execute_command('TS.ADD', key, ts, value, 'ON_DUPLICATE', 'LAST')


def resolve_posto_context(mongo, posto_id: str) -> dict:
    oid = ObjectId(posto_id)
    posto = mongo[MONGO_DB][COL_POSTOS].find_one({'_id': oid})
    local = mongo[MONGO_DB][COL_LOCALIZACOES].find_one({'posto_id': oid})
    endereco = (posto or {}).get('endereco', {}) or {}
    return {
        'nome_fantasia': (posto or {}).get('nome_fantasia', posto_id),
        'bandeira': str((posto or {}).get('bandeira', '')).strip().lower().replace(' ', '_'),
        'bairro': str((local or {}).get('bairro') or endereco.get('bairro', '')).strip().lower().replace(' ', '_'),
        'cidade': str((local or {}).get('municipio') or endereco.get('cidade', '')).strip().lower().replace(' ', '_'),
        'estado': str((local or {}).get('uf') or endereco.get('estado', '')).strip().lower().replace(' ', '_'),
        'codigo_ibge': str((local or {}).get('codigo_ibge', '')),
    }


def apply_price_event(redis: Redis, mongo, event: dict) -> None:
    ctx = resolve_posto_context(mongo, event['posto_id'])
    posto_id = event['posto_id']
    combustivel = event['combustivel']
    preco_novo = event['preco_novo']

    redis.hset(hash_key(posto_id), mapping={
        'posto_id': posto_id,
        'nome_fantasia': ctx['nome_fantasia'],
        'bandeira': ctx['bandeira'],
        'bairro': ctx['bairro'],
        'cidade': ctx['cidade'],
        'estado': ctx['estado'],
        'codigo_ibge': ctx['codigo_ibge'],
        combustivel: preco_novo,
        f'{combustivel}_variacao_pct': event['variacao_pct'],
        f'{combustivel}_fonte': event['fonte'],
        f'{combustivel}_revisado': int(event['revisado']),
        f'{combustivel}_updated_at': event['ocorrido_em'].isoformat(),
    })

    redis.zadd(ranking_preco_uf_key(combustivel, ctx['estado']), {posto_id: preco_novo})
    redis.zadd(ranking_preco_cidade_key(combustivel, ctx['cidade']), {posto_id: preco_novo})
    redis.zadd(ranking_variacao_key(combustivel), {posto_id: abs(event['variacao_pct'])})

    ensure_ts_add(redis, ts_key(posto_id, combustivel), event['ts'], preco_novo, {
        'posto_id': posto_id,
        'combustivel': combustivel,
        'metric': 'preco',
    })


def apply_interaction_event(redis: Redis, event: dict) -> None:
    incremento = 1 + int(event.get('util_count', 0) or 0)
    redis.zincrby('ranking:interacoes:postos', incremento, event['posto_id'])
    redis.hincrby(hash_key(event['posto_id']), 'engajamento', incremento)
    if event.get('nota') is not None:
        redis.hincrbyfloat(hash_key(event['posto_id']), 'nota_sum', float(event['nota']))
        redis.hincrby(hash_key(event['posto_id']), 'nota_count', 1)


def ensure_global_ts(redis: Redis, key: str, labels: dict) -> None:
    try:
        redis.execute_command('TS.CREATE', key, 'RETENTION', 604800000, 'DUPLICATE_POLICY', 'LAST', 'LABELS', *sum(([k, v] for k, v in labels.items()), []))
    except ResponseError:
        pass


def apply_search_event(redis: Redis, event: dict) -> None:
    redis.zincrby('ranking:combustivel:buscas', 1, event['combustivel'])
    redis.zincrby('ranking:buscas:cidade', 1, f"{event['estado']}:{event['cidade']}")

    ensure_global_ts(redis, 'ts:buscas:latencia_ms', {'metric': 'latencia_ms', 'scope': 'buscas'})
    ensure_global_ts(redis, 'ts:buscas:resultado_count', {'metric': 'resultado_count', 'scope': 'buscas'})
    redis.execute_command('TS.ADD', 'ts:buscas:latencia_ms', event['ts'], event['latencia_ms'], 'ON_DUPLICATE', 'LAST')
    redis.execute_command('TS.ADD', 'ts:buscas:resultado_count', event['ts'], event['resultado_count'], 'ON_DUPLICATE', 'LAST')


def main() -> None:
    mongo = MongoClient(MONGO_URI)
    redis = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    col_precos = mongo[MONGO_DB][COL_EVENTOS_PRECO]
    col_avaliacoes = mongo[MONGO_DB][COL_AVALIACOES]
    col_buscas = mongo[MONGO_DB][COL_BUSCAS]

    last_preco_ts = None
    last_avaliacao_ts = None
    last_busca_ts = None

    print('[CONSUMER] Radar Combustível iniciado.')

    while True:
        q_preco = {'ocorrido_em': {'$gt': last_preco_ts}} if last_preco_ts else {}
        for raw in col_precos.find(q_preco).sort('ocorrido_em', 1).limit(BATCH_SIZE):
            event = normalize_event(raw, 'eventos_preco')
            apply_price_event(redis, mongo, event)
            last_preco_ts = raw['ocorrido_em']

        q_aval = {'created_at': {'$gt': last_avaliacao_ts}} if last_avaliacao_ts else {}
        for raw in col_avaliacoes.find(q_aval).sort('created_at', 1).limit(BATCH_SIZE):
            event = normalize_event(raw, 'avaliacoes_interacoes')
            apply_interaction_event(redis, event)
            last_avaliacao_ts = raw['created_at']

        q_busca = {'consultado_em': {'$gt': last_busca_ts}} if last_busca_ts else {}
        for raw in col_buscas.find(q_busca).sort('consultado_em', 1).limit(BATCH_SIZE):
            event = normalize_event(raw, 'buscas_usuario')
            apply_search_event(redis, event)
            last_busca_ts = raw['consultado_em']

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == '__main__':
    main()
