# event_transformer.py

from typing import Any, Dict


def normalize_text(value: Any) -> str:
    return str(value or '').strip().lower().replace(' ', '_')


def normalize_event(raw: Dict[str, Any], source: str) -> Dict[str, Any]:
    if source == 'eventos_preco':
        return {
            'type': 'price_update',
            'posto_id': str(raw['posto_id']),
            'combustivel': normalize_text(raw.get('combustivel', '')),
            'preco_anterior': float(raw.get('preco_anterior', 0) or 0),
            'preco_novo': float(raw.get('preco_novo', 0) or 0),
            'variacao_pct': float(raw.get('variacao_pct', 0) or 0),
            'unidade': raw.get('unidade', ''),
            'fonte': raw.get('fonte', ''),
            'revisado': bool(raw.get('revisado', False)),
            'ts': int(raw['ocorrido_em'].timestamp() * 1000),
            'ocorrido_em': raw['ocorrido_em'],
        }

    if source == 'avaliacoes_interacoes':
        return {
            'type': normalize_text(raw.get('tipo', '')),
            'posto_id': str(raw['posto_id']),
            'usuario_id': str(raw.get('usuario_id', '')),
            'nota': raw.get('nota'),
            'comentario': raw.get('comentario', ''),
            'util_count': int(raw.get('util_count', 0) or 0),
            'ts': int(raw['created_at'].timestamp() * 1000),
            'created_at': raw['created_at'],
        }

    if source == 'buscas_usuario':
        return {
            'type': 'search',
            'usuario_id': str(raw.get('usuario_id', '')),
            'session_id': str(raw.get('session_id', '')),
            'cidade': normalize_text(raw.get('cidade', '')),
            'estado': normalize_text(raw.get('estado', '')),
            'combustivel': normalize_text(raw.get('tipo_combustivel', '')),
            'raio_km': int(raw.get('raio_km', 0) or 0),
            'resultado_count': int(raw.get('resultado_count', 0) or 0),
            'latencia_ms': int(raw.get('latencia_ms', 0) or 0),
            'ts': int(raw['consultado_em'].timestamp() * 1000),
            'consultado_em': raw['consultado_em'],
        }

    raise ValueError(f'Fonte inválida: {source}')


def hash_key(posto_id: str) -> str:
    return f'posto:{posto_id}'


def ts_key(posto_id: str, combustivel: str) -> str:
    return f'ts:posto:{posto_id}:{combustivel}'


def ranking_preco_uf_key(combustivel: str, uf: str) -> str:
    return f'ranking:preco:{normalize_text(combustivel)}:{normalize_text(uf)}'


def ranking_preco_cidade_key(combustivel: str, cidade: str) -> str:
    return f'ranking:preco:{normalize_text(combustivel)}:{normalize_text(cidade)}'


def ranking_variacao_key(combustivel: str) -> str:
    return f'ranking:variacao:{normalize_text(combustivel)}'
