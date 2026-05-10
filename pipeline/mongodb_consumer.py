"""
MongoDB -> Redis consumer (streaming pipeline)
==============================================

Two phases:

1. **Backfill** -- on first run there are already ~100k documents in the
   five collections of ``radar_combustivel``. We stream them in batches
   (configurable) and apply :func:`event_transformer.apply_event` so the
   Redis serving layer reflects the historical state.

2. **Change Stream** -- once the backfill is done we open
   ``col.watch(...)`` on every collection and react to ``insert``,
   ``update`` and ``replace`` events as they happen.

The script publishes lightweight metrics to ``pipeline:metrics`` so the
Streamlit dashboard can show pipeline health without querying Mongo.

Run with:

    python -m pipeline.mongodb_consumer
or simply:
    python pipeline/mongodb_consumer.py
"""
from __future__ import annotations

import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable

import redis as redis_lib
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError
from rich.console import Console

# Allow running this file as a script (python pipeline/mongodb_consumer.py)
# or as a module (python -m pipeline.mongodb_consumer).
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from pipeline.config import (  # type: ignore
        ALL_COLLECTIONS,
        BACKFILL_BATCH_SIZE,
        BACKFILL_WORKERS,
        METRICS_INTERVAL_SECONDS,
        MONGO_DB,
        MONGO_URI,
        REDIS_DB,
        REDIS_HOST,
        REDIS_PASSWORD,
        REDIS_PORT,
        RedisKeys,
        configure_logging,
    )
    from pipeline.event_transformer import apply_event  # type: ignore
else:
    from .config import (
        ALL_COLLECTIONS,
        BACKFILL_BATCH_SIZE,
        BACKFILL_WORKERS,
        METRICS_INTERVAL_SECONDS,
        MONGO_DB,
        MONGO_URI,
        REDIS_DB,
        REDIS_HOST,
        REDIS_PASSWORD,
        REDIS_PORT,
        RedisKeys,
        configure_logging,
    )
    from .event_transformer import apply_event


log = configure_logging("consumer")
console = Console()
shutdown_event = threading.Event()


# ---------------------------------------------------------------------------
# Counters (process-local, periodically flushed to Redis)
# ---------------------------------------------------------------------------
class Counters:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.processed_total = 0
        self.processed_by_collection = {c: 0 for c in ALL_COLLECTIONS}
        self.last_event_collection = ""
        self.last_event_ts = 0
        self.errors = 0

    def incr(self, collection: str) -> None:
        with self._lock:
            self.processed_total += 1
            self.processed_by_collection[collection] = (
                self.processed_by_collection.get(collection, 0) + 1
            )
            self.last_event_collection = collection
            self.last_event_ts = int(time.time() * 1000)

    def incr_error(self) -> None:
        with self._lock:
            self.errors += 1

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "processed_total": self.processed_total,
                "errors": self.errors,
                "last_event_collection": self.last_event_collection,
                "last_event_ts": self.last_event_ts,
                **{f"col:{k}": v for k, v in self.processed_by_collection.items()},
            }


counters = Counters()


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------
def make_redis() -> redis_lib.Redis:
    return redis_lib.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=REDIS_DB,
        decode_responses=True,
        socket_keepalive=True,
    )


def make_mongo() -> MongoClient:
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)


# ---------------------------------------------------------------------------
# Backfill phase
# ---------------------------------------------------------------------------
def _process_batch(collection_name: str, docs: list) -> int:
    """Worker function used by the thread pool."""
    r = make_redis()
    ok = 0
    for d in docs:
        try:
            apply_event(r, collection_name, d)
            counters.incr(collection_name)
            ok += 1
        except Exception as exc:  # noqa: BLE001
            counters.incr_error()
            log.warning("Backfill error in %s: %s", collection_name, exc)
    return ok


def backfill_collection(db, collection_name: str) -> int:
    """Sequential cursor + parallel batch processing."""
    col = db[collection_name]
    total = col.estimated_document_count()
    log.info("[backfill] %s -> %s documentos estimados", collection_name, total)
    if total == 0:
        return 0

    cursor = col.find({}, no_cursor_timeout=True).batch_size(BACKFILL_BATCH_SIZE)
    processed = 0
    batch: list = []
    started = time.time()

    with ThreadPoolExecutor(max_workers=BACKFILL_WORKERS) as pool:
        futures = []
        try:
            for doc in cursor:
                batch.append(doc)
                if len(batch) >= BACKFILL_BATCH_SIZE:
                    futures.append(pool.submit(_process_batch, collection_name, batch))
                    batch = []
                    # Throttle to avoid blowing memory: drain whenever the
                    # pool already has BACKFILL_WORKERS * 2 batches in flight.
                    if len(futures) >= BACKFILL_WORKERS * 2:
                        for f in futures:
                            processed += f.result()
                        futures = []
                        elapsed = max(time.time() - started, 1)
                        log.info(
                            "[backfill] %s: %d/%d (%.0f docs/s)",
                            collection_name,
                            processed,
                            total,
                            processed / elapsed,
                        )
            if batch:
                futures.append(pool.submit(_process_batch, collection_name, batch))
            for f in futures:
                processed += f.result()
        finally:
            cursor.close()

    elapsed = time.time() - started
    log.info(
        "[backfill] %s concluido: %d docs em %.1fs (%.0f docs/s)",
        collection_name,
        processed,
        elapsed,
        processed / max(elapsed, 1),
    )
    return processed


def run_backfill() -> None:
    log.info("Iniciando backfill em %d colecoes", len(ALL_COLLECTIONS))
    client = make_mongo()
    try:
        db = client[MONGO_DB]
        # Order matters: postos and localizacoes first so eventos_preco can
        # read uf/cidade/coords from the hash when computing rankings.
        ordered = sorted(
            ALL_COLLECTIONS,
            key=lambda c: 0 if c in ("postos", "localizacoes_postos") else 1,
        )
        for col in ordered:
            if shutdown_event.is_set():
                break
            backfill_collection(db, col)
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Change Stream phase
# ---------------------------------------------------------------------------
def _watch_collection(collection_name: str) -> None:
    """One thread per watched collection."""
    log.info("[watch] abrindo Change Stream em %s", collection_name)
    backoff = 1
    while not shutdown_event.is_set():
        client = make_mongo()
        r = make_redis()
        try:
            col = client[MONGO_DB][collection_name]
            pipeline = [
                {"$match": {"operationType": {"$in": ["insert", "update", "replace"]}}}
            ]
            with col.watch(pipeline, full_document="updateLookup") as stream:
                backoff = 1  # reset after a successful open
                for change in stream:
                    if shutdown_event.is_set():
                        break
                    doc = change.get("fullDocument")
                    if not doc:
                        continue
                    try:
                        apply_event(r, collection_name, doc)
                        counters.incr(collection_name)
                    except Exception as exc:  # noqa: BLE001
                        counters.incr_error()
                        log.warning(
                            "[watch] erro processando %s/%s: %s",
                            collection_name,
                            doc.get("_id"),
                            exc,
                        )
        except OperationFailure as e:
            log.error(
                "[watch] OperationFailure em %s (replica set ativo?): %s",
                collection_name,
                e,
            )
        except PyMongoError as e:
            log.error("[watch] erro Mongo em %s: %s", collection_name, e)
        finally:
            client.close()

        if shutdown_event.is_set():
            return
        sleep_for = min(backoff, 30)
        log.warning(
            "[watch] reconectando %s em %ds...", collection_name, sleep_for
        )
        time.sleep(sleep_for)
        backoff = min(backoff * 2, 30)


def run_change_streams() -> None:
    threads = []
    for col in ALL_COLLECTIONS:
        t = threading.Thread(
            target=_watch_collection, args=(col,), name=f"watch-{col}", daemon=True
        )
        t.start()
        threads.append(t)
    log.info("Change Streams ativos em %d colecoes", len(threads))
    return threads


# ---------------------------------------------------------------------------
# Metrics flush
# ---------------------------------------------------------------------------
def metrics_loop() -> None:
    r = make_redis()
    while not shutdown_event.is_set():
        try:
            snap = counters.snapshot()
            r.hset(RedisKeys.METRICS_HASH, mapping={k: str(v) for k, v in snap.items()})
            r.set(RedisKeys.METRICS_LAST_RUN, int(time.time() * 1000))
            log.info(
                "metrics: total=%d erros=%d ultimo=%s",
                snap["processed_total"],
                snap["errors"],
                snap["last_event_collection"] or "-",
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("metrics flush error: %s", exc)
        shutdown_event.wait(METRICS_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def _install_signal_handlers() -> None:
    def _stop(signum, _frame):  # noqa: ANN001
        log.info("Sinal %s recebido, encerrando...", signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, _stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _stop)


def main() -> None:
    _install_signal_handlers()
    console.rule("[bold green]Radar Combustivel - Pipeline MongoDB -> Redis")

    metrics_thread = threading.Thread(target=metrics_loop, name="metrics", daemon=True)
    metrics_thread.start()

    skip_backfill = os.getenv("SKIP_BACKFILL", "0") in ("1", "true", "True")
    if skip_backfill:
        log.info("SKIP_BACKFILL=1 -> pulando backfill, indo direto pro Change Stream")
    else:
        run_backfill()

    if shutdown_event.is_set():
        return

    threads = run_change_streams()
    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    finally:
        log.info("Encerrado. Aguardando threads...")
        for t in threads:
            t.join(timeout=5)
        log.info("Bye.")


if __name__ == "__main__":
    main()
