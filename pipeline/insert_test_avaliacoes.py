"""
Insere documentos sinteticos em avaliacoes_interacoes para testar o
Change Stream do pipeline.

O posto-alvo (Posto Bom Vista EIRELI - Itajuba/MG) eh deixado entre os
top do rank:postos:rating, mas com media abaixo de 5 (mistura de notas
3, 4 e 5). Tambem dispara alguns check-ins e um compartilhamento para
exercitar os outros rankings de engajamento.

Uso:
    python init/insert_test_avaliacoes.py
    python init/insert_test_avaliacoes.py --posto 6a00d413119f233162969e04
    python init/insert_test_avaliacoes.py --avaliacoes 20 --checkins 5

Pre-requisito: consumer rodando em outro terminal para ver o Change
Stream reagindo aos novos documentos.
"""
from __future__ import annotations

import argparse
import os
import random
import sys
import uuid
from datetime import datetime, timezone

from bson import ObjectId
from pymongo import MongoClient

# Permite rodar como script ou modulo
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import (  # noqa: E402
    COL_AVALIACOES,
    MONGO_DB,
    MONGO_URI,
    configure_logging,
)

log = configure_logging("insert-test")


COMENTARIOS_NOTA_3 = [
    "Atendimento mediano, podia melhorar.",
    "Posto OK, mas o banheiro estava sujo.",
    "Preco bom, atendimento devagar.",
    "Esperei 15 minutos para abastecer.",
]
COMENTARIOS_NOTA_4 = [
    "Posto limpo e atendimento simpatico.",
    "Bom posto, sempre paro aqui.",
    "Recomendo, mas o cafe da loja podia ser melhor.",
    "Bem localizado.",
]
COMENTARIOS_NOTA_5 = [
    "Excelente atendimento, super recomendo!",
    "Sempre paro aqui, melhor posto da regiao.",
    "Atendentes super atenciosos.",
]


def _comentario_para_nota(nota: int) -> str:
    pool = {3: COMENTARIOS_NOTA_3, 4: COMENTARIOS_NOTA_4, 5: COMENTARIOS_NOTA_5}.get(nota)
    return random.choice(pool) if pool else None


def gerar_documentos(posto_id: str, n_avaliacoes: int, n_checkins: int,
                     n_compart: int) -> list[dict]:
    docs = []
    oid = ObjectId(posto_id)
    now = datetime.now(tz=timezone.utc)

    # ---------------- Avaliacoes com nota ----------------
    # Notas com peso para 3 e 4 (puxam a media abaixo de 5)
    pesos_nota = [3, 3, 3, 4, 4, 4, 4, 5]
    for i in range(n_avaliacoes):
        nota = random.choice(pesos_nota)
        docs.append({
            "posto_id": oid,
            "usuario_id": str(uuid.uuid4()),
            "tipo": "avaliacao",
            "nota": nota,
            "comentario": _comentario_para_nota(nota) if random.random() > 0.4 else None,
            "created_at": now,
            "util_count": random.randint(0, 12),
        })

    # ---------------- Check-ins ----------------
    for _ in range(n_checkins):
        docs.append({
            "posto_id": oid,
            "usuario_id": str(uuid.uuid4()),
            "tipo": "check_in",
            "nota": None,
            "comentario": None,
            "created_at": now,
            "util_count": random.randint(0, 3),
        })

    # ---------------- Compartilhamentos ----------------
    for _ in range(n_compart):
        docs.append({
            "posto_id": oid,
            "usuario_id": str(uuid.uuid4()),
            "tipo": "compartilhamento",
            "nota": None,
            "comentario": None,
            "created_at": now,
            "util_count": 0,
        })

    random.shuffle(docs)
    return docs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--posto", default="6a00d413119f233162969e04",
                        help="ObjectId do posto-alvo")
    parser.add_argument("--avaliacoes", type=int, default=10,
                        help="quantas avaliacoes com nota inserir")
    parser.add_argument("--checkins", type=int, default=3,
                        help="quantos check-ins inserir")
    parser.add_argument("--compartilhamentos", type=int, default=1,
                        help="quantos compartilhamentos inserir")
    parser.add_argument("--seed", type=int, default=None,
                        help="seed para gerar sempre os mesmos documentos")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    log.info("Conectando em %s / %s", MONGO_URI, MONGO_DB)
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # Confirma que o posto existe (UX friendly)
    try:
        posto = db["postos"].find_one({"_id": ObjectId(args.posto)},
                                       {"nome_fantasia": 1, "endereco.cidade": 1,
                                        "endereco.estado": 1})
    except Exception as exc:
        log.error("ObjectId invalido: %s", exc)
        sys.exit(1)
    if not posto:
        log.error("Posto %s nao encontrado em radar_combustivel.postos",
                  args.posto)
        sys.exit(1)

    log.info("Alvo: %s (%s/%s)",
             posto.get("nome_fantasia"),
             (posto.get("endereco") or {}).get("cidade"),
             (posto.get("endereco") or {}).get("estado"))

    docs = gerar_documentos(args.posto, args.avaliacoes, args.checkins,
                            args.compartilhamentos)
    log.info("Gerados %d documentos (%d aval, %d check-in, %d compart.)",
             len(docs), args.avaliacoes, args.checkins, args.compartilhamentos)

    # Inserts UM por UM para que o consumer veja eventos individuais
    # (insertMany dispararia um unico Change Stream event por batch,
    # menos visivel na demo).
    for i, d in enumerate(docs, 1):
        result = db[COL_AVALIACOES].insert_one(d)
        log.info("  [%2d/%d] tipo=%-18s nota=%s util=%-2d _id=%s",
                 i, len(docs), d["tipo"], d["nota"], d["util_count"],
                 result.inserted_id)

    # Sumario das notas para previsao da nova media
    notas = [d["nota"] for d in docs if d["nota"] is not None]
    if notas:
        log.info("Media das %d novas notas inseridas: %.2f (min=%d, max=%d)",
                 len(notas), sum(notas) / len(notas), min(notas), max(notas))

    log.info("Pronto. Observe o terminal do consumer para ver o Change")
    log.info("Stream processando os %d eventos novos, e atualize o dashboard.", len(docs))


if __name__ == "__main__":
    main()
