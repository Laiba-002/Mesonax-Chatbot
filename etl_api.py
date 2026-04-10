"""
MESONEX - ETL API
=================
Single FastAPI application that:
  1. On startup  -> runs a FULL migration once (background thread).
  2. After full  -> auto-starts delta sync timer every DELTA_SYNC_INTERVAL_SECONDS.
  3. Swagger UI  -> http://localhost:8001/docs

Run:
    uvicorn etl_api:app --host 0.0.0.0 --port 8001
"""

import logging
import threading
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from config import DELTA_SYNC_INTERVAL_SECONDS
from migrator import DataMigrator

# ================================
# Logging  (ASCII-only messages - safe on Windows CP1252)
# ================================
logger = logging.getLogger("mesonex_etl_api")
logger.setLevel(logging.INFO)

if not logger.handlers:
    fh = RotatingFileHandler(
        "etl_api.log", maxBytes=5 * 1024 * 1024, backupCount=3)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    if hasattr(sh.stream, "reconfigure"):
        sh.stream.reconfigure(encoding="utf-8", errors="replace")
    logger.addHandler(sh)


# ================================
# FastAPI App
# ================================
app = FastAPI(
    title="MESONEX ETL API",
    description=(
        "Manages the SQL Server to Neo4j ETL pipeline.\n\n"
        "**Startup behaviour:** Runs a full migration once when the server starts, "
        "then automatically triggers a delta sync every "
        f"`{DELTA_SYNC_INTERVAL_SECONDS}` seconds "
        "(change `DELTA_SYNC_INTERVAL_SECONDS` in `config.py`).\n\n"
        "Use the endpoints below to monitor status, view history, "
        "or trigger manual syncs at any time."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ================================
# Shared ETL State
# ================================

class ETLState:
    """
    Thread-safe container for the ETL pipeline runtime state.
    One global instance shared across all requests and the background thread.
    """

    MAX_HISTORY = 50

    def __init__(self):
        self._lock = threading.Lock()

        # Scheduler internals
        self.scheduler_running: bool = False
        self.is_syncing: bool = False
        self._stop_event = threading.Event()
        self._trigger_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Initial full migration status
        self.initial_full_done: bool = False
        self.initial_full_started_at: Optional[str] = None
        self.initial_full_completed_at: Optional[str] = None
        self.initial_full_error: Optional[str] = None

        # Run statistics
        self.total_runs: int = 0
        self.total_failures: int = 0
        self.consecutive_failures: int = 0
        self.last_run_at: Optional[str] = None
        self.last_run_status: Optional[str] = None
        self.last_run_mode: Optional[str] = None
        self.last_run_duration: Optional[float] = None
        self.last_error: Optional[str] = None
        self.next_delta_at: Optional[str] = None

        # Run history
        self.history: List[Dict[str, Any]] = []

    def record_run(self, mode: str, success: bool, duration: float, error: Optional[str]):
        with self._lock:
            self.total_runs += 1
            self.last_run_at = datetime.now(tz=timezone.utc).isoformat()
            self.last_run_mode = mode
            self.last_run_duration = round(duration, 2)
            if success:
                self.last_run_status = "success"
                self.last_error = None
                self.consecutive_failures = 0
            else:
                self.last_run_status = "failed"
                self.last_error = error
                self.total_failures += 1
                self.consecutive_failures += 1
            self.history.append({
                "timestamp": self.last_run_at,
                "mode": mode,
                "success": success,
                "duration_seconds": round(duration, 2),
                "error": error,
            })
            if len(self.history) > self.MAX_HISTORY:
                self.history = self.history[-self.MAX_HISTORY:]

    def set_next_delta(self):
        with self._lock:
            self.next_delta_at = (
                datetime.now(tz=timezone.utc) +
                timedelta(seconds=DELTA_SYNC_INTERVAL_SECONDS)
            ).isoformat()

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "scheduler_running": self.scheduler_running,
                "is_syncing": self.is_syncing,
                "initial_full_done": self.initial_full_done,
                "initial_full_started_at": self.initial_full_started_at,
                "initial_full_completed_at": self.initial_full_completed_at,
                "initial_full_error": self.initial_full_error,
                "delta_sync_interval_seconds": DELTA_SYNC_INTERVAL_SECONDS,
                "total_runs": self.total_runs,
                "total_failures": self.total_failures,
                "consecutive_failures": self.consecutive_failures,
                "last_run_at": self.last_run_at,
                "last_run_mode": self.last_run_mode,
                "last_run_status": self.last_run_status,
                "last_run_duration_seconds": self.last_run_duration,
                "last_error": self.last_error,
                "next_delta_at": self.next_delta_at,
            }


# Single global state instance
state = ETLState()


# ================================
# ETL Execution Helpers
# ================================

def _run_full(clear_existing: bool = False) -> Dict[str, Any]:
    migrator = DataMigrator()
    try:
        return migrator.run_full(clear_existing=clear_existing)
    finally:
        migrator.close()


def _run_delta() -> Dict[str, Any]:
    migrator = DataMigrator()
    try:
        return migrator.run_delta()
    finally:
        migrator.close()


# ================================
# Background Delta-Sync Scheduler
# ================================

def _scheduler_loop():
    """
    Background thread: waits DELTA_SYNC_INTERVAL_SECONDS between delta syncs.
    Can be woken early by _trigger_event for manual on-demand runs.
    Stops when _stop_event is set.
    """
    logger.info(
        f"Scheduler started - delta sync every {DELTA_SYNC_INTERVAL_SECONDS}s")

    while not state._stop_event.is_set():
        state.set_next_delta()

        # Block until interval elapses OR a manual trigger fires
        triggered = state._trigger_event.wait(
            timeout=DELTA_SYNC_INTERVAL_SECONDS)
        state._trigger_event.clear()

        if state._stop_event.is_set():
            break

        with state._lock:
            if state.is_syncing:
                logger.warning(
                    "Scheduler skipping - a sync is already running")
                continue
            state.is_syncing = True

        start = datetime.now(tz=timezone.utc)
        reason = "triggered manually" if triggered else "scheduled"
        logger.info(f"=== Delta sync starting ({reason}) ===")

        try:
            _run_delta()
            duration = (datetime.now(tz=timezone.utc) - start).total_seconds()
            state.record_run("DELTA", True, duration, None)
            logger.info(f"=== Delta sync completed in {duration:.2f}s ===")
        except Exception as exc:
            duration = (datetime.now(tz=timezone.utc) - start).total_seconds()
            logger.error(f"=== Delta sync FAILED: {exc} ===")
            state.record_run("DELTA", False, duration, str(exc))
        finally:
            with state._lock:
                state.is_syncing = False

    logger.info("Scheduler thread stopped")


def _start_scheduler():
    with state._lock:
        if state.scheduler_running:
            return
        state._stop_event.clear()
        state._trigger_event.clear()
        state._thread = threading.Thread(
            target=_scheduler_loop,
            name="ETLDeltaScheduler",
            daemon=True,
        )
        state._thread.start()
        state.scheduler_running = True
    logger.info("[OK] Background delta-sync scheduler started")


def _stop_scheduler():
    state._stop_event.set()
    state._trigger_event.set()
    if state._thread:
        state._thread.join(timeout=30)
    with state._lock:
        state.scheduler_running = False
        state.next_delta_at = None
    logger.info("Background scheduler stopped")


# ================================
# Startup - Full Migration then Scheduler
# ================================

def _initial_full_migration():
    """
    Runs at startup in a background thread:
      1. Execute a full migration.
      2. On success  -> start the auto delta-sync scheduler.
      3. On failure  -> log error, scheduler is NOT started (data incomplete).
    """
    with state._lock:
        state.is_syncing = True
        state.initial_full_started_at = datetime.now(
            tz=timezone.utc).isoformat()

    start = datetime.now(tz=timezone.utc)
    logger.info("=== Initial FULL migration starting ===")

    try:
        _run_full(clear_existing=False)
        duration = (datetime.now(tz=timezone.utc) - start).total_seconds()

        with state._lock:
            state.initial_full_done = True
            state.initial_full_completed_at = datetime.now(
                tz=timezone.utc).isoformat()
            state.is_syncing = False

        state.record_run("FULL", True, duration, None)
        logger.info(
            f"=== Initial FULL migration completed in {duration:.2f}s ===")

        # Start auto delta-sync ONLY after a successful full migration
        _start_scheduler()

    except Exception as exc:
        duration = (datetime.now(tz=timezone.utc) - start).total_seconds()
        error_str = str(exc)
        logger.error(f"=== Initial FULL migration FAILED: {error_str} ===")

        with state._lock:
            state.initial_full_error = error_str
            state.is_syncing = False

        state.record_run("FULL", False, duration, error_str)
        # Scheduler intentionally NOT started - data may be incomplete


@app.on_event("startup")
def on_startup():
    """Launch the initial full migration in a background thread.
    FastAPI and Swagger UI are immediately available while migration runs."""
    logger.info("ETL API starting...")
    t = threading.Thread(
        target=_initial_full_migration,
        name="InitialFullMigration",
        daemon=True,
    )
    t.start()


@app.on_event("shutdown")
def on_shutdown():
    _stop_scheduler()
    logger.info("ETL API shut down")


# ================================
# Pydantic Models
# ================================

class StatusResponse(BaseModel):
    scheduler_running: bool
    is_syncing: bool
    initial_full_done: bool
    initial_full_started_at: Optional[str]
    initial_full_completed_at: Optional[str]
    initial_full_error: Optional[str]
    delta_sync_interval_seconds: int
    total_runs: int
    total_failures: int
    consecutive_failures: int
    last_run_at: Optional[str]
    last_run_mode: Optional[str]
    last_run_status: Optional[str]
    last_run_duration_seconds: Optional[float]
    last_error: Optional[str]
    next_delta_at: Optional[str]


class RunResult(BaseModel):
    success: bool
    mode: str
    duration_seconds: float
    timestamp: str
    message: str
    nodes: Dict[str, int] = {}
    relationships: Dict[str, int] = {}


class TriggerResponse(BaseModel):
    success: bool
    message: str


class UpdateIntervalRequest(BaseModel):
    interval_seconds: int = Field(
        ..., ge=30,
        description="New delta-sync interval in seconds (minimum 30).",
        example=3600,
    )


# ================================
# Endpoints
# ================================

@app.get("/", tags=["Info"], summary="Health check")
def root() -> Dict[str, Any]:
    """Returns basic API info and a quick snapshot of the current ETL state."""
    s = state.snapshot()
    return {
        "service": "MESONEX ETL API",
        "version": "1.0.0",
        "docs": "/docs",
        "initial_full_done": s["initial_full_done"],
        "scheduler_running": s["scheduler_running"],
        "is_syncing": s["is_syncing"],
        "last_run_status": s["last_run_status"],
        "next_delta_at": s["next_delta_at"],
    }


# ── Status ─────────────────────────────────────────────────────────────

@app.get(
    "/status",
    response_model=StatusResponse,
    tags=["Status"],
    summary="Full ETL pipeline status",
)
def get_status():
    """
    Returns the complete real-time state of the ETL pipeline including:
    - Whether the initial full migration has completed
    - Whether the auto delta-sync scheduler is running
    - Last run timestamp, mode, status, and duration
    - Next scheduled delta-sync time
    - Failure counters
    """
    return StatusResponse(**state.snapshot())


@app.get(
    "/status/history",
    tags=["Status"],
    summary="ETL run history",
)
def get_history(
    limit: int = Query(default=20, ge=1, le=50,
                       description="Number of recent runs to return.")
) -> Dict[str, Any]:
    """
    Returns the last N ETL run records, newest first.
    Each record: timestamp, mode (FULL/DELTA), success, duration_seconds, error.
    """
    history = list(reversed(state.history[-limit:]))
    return {
        "total_recorded": len(state.history),
        "returned": len(history),
        "runs": history,
    }


@app.get(
    "/status/node-counts",
    tags=["Status"],
    summary="Live node and relationship counts from Neo4j",
)
def get_node_counts() -> Dict[str, Any]:
    """Queries Neo4j directly and returns current node and relationship counts."""
    migrator = DataMigrator()
    try:
        if not migrator.connect_neo4j():
            raise HTTPException(
                status_code=500, detail="Cannot connect to Neo4j")
        report = migrator.verify_migration()
        return {
            "retrieved_at": datetime.now(tz=timezone.utc).isoformat(),
            "nodes": report.get("nodes", {}),
            "relationships": report.get("relationships", {}),
        }
    finally:
        migrator.close()


@app.get(
    "/status/last-synced",
    tags=["Status"],
    summary="Last delta-sync timestamp per node label",
)
def get_last_synced() -> Dict[str, Any]:
    """
    Returns the most recent _last_synced UTC timestamp stored on each Neo4j
    node label. Only populated after at least one delta sync has run.
    """
    migrator = DataMigrator()
    try:
        if not migrator.connect_neo4j():
            raise HTTPException(
                status_code=500, detail="Cannot connect to Neo4j")
        return {
            "retrieved_at": datetime.now(tz=timezone.utc).isoformat(),
            "node_last_synced": migrator.get_last_sync_info(),
        }
    finally:
        migrator.close()


# ── Manual Migration Triggers ───────────────────────────────────────────

@app.post(
    "/migrate/full",
    response_model=RunResult,
    tags=["Migration"],
    summary="Run a full migration (synchronous - waits for completion)",
)
def trigger_full(
    clear_existing: bool = Query(
        default=False,
        description="Delete all Neo4j nodes and relationships before migrating.",
    )
):
    """
    Runs a **full migration** immediately and waits for it to finish.
    Every row from every SQL Server view is upserted into Neo4j.

    - Use `clear_existing=true` to wipe Neo4j first (hard reset).
    - Returns a complete node and relationship count report.
    - Rejected with 409 if another sync is already running.
    """
    with state._lock:
        if state.is_syncing:
            raise HTTPException(
                status_code=409,
                detail="A sync is already in progress. Try again shortly.",
            )
        state.is_syncing = True

    start = datetime.now(tz=timezone.utc)
    try:
        report = _run_full(clear_existing=clear_existing)
        duration = (datetime.now(tz=timezone.utc) - start).total_seconds()
        state.record_run("FULL", True, duration, None)
        with state._lock:
            state.initial_full_done = True
        return RunResult(
            success=True,
            mode="FULL",
            duration_seconds=round(duration, 2),
            timestamp=start.isoformat(),
            message="Full migration completed successfully.",
            nodes=report.get("nodes", {}),
            relationships=report.get("relationships", {}),
        )
    except Exception as exc:
        duration = (datetime.now(tz=timezone.utc) - start).total_seconds()
        state.record_run("FULL", False, duration, str(exc))
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        with state._lock:
            state.is_syncing = False


@app.post(
    "/migrate/delta",
    response_model=RunResult,
    tags=["Migration"],
    summary="Run a delta sync (synchronous - waits for completion)",
)
def trigger_delta():
    """
    Runs a **delta sync** immediately and waits for it to finish.
    Only rows that are new or changed since the last sync are sent to Neo4j.

    - Much faster than a full migration for routine updates.
    - Requires the initial full migration to have completed first.
    - Rejected with 409 if another sync is already running.
    """
    if not state.initial_full_done:
        raise HTTPException(
            status_code=425,
            detail="Initial full migration has not completed yet. Check /status.",
        )
    with state._lock:
        if state.is_syncing:
            raise HTTPException(
                status_code=409,
                detail="A sync is already in progress. Try again shortly.",
            )
        state.is_syncing = True

    start = datetime.now(tz=timezone.utc)
    try:
        report = _run_delta()
        duration = (datetime.now(tz=timezone.utc) - start).total_seconds()
        state.record_run("DELTA", True, duration, None)
        return RunResult(
            success=True,
            mode="DELTA",
            duration_seconds=round(duration, 2),
            timestamp=start.isoformat(),
            message="Delta sync completed successfully.",
            nodes=report.get("nodes", {}),
            relationships=report.get("relationships", {}),
        )
    except Exception as exc:
        duration = (datetime.now(tz=timezone.utc) - start).total_seconds()
        state.record_run("DELTA", False, duration, str(exc))
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        with state._lock:
            state.is_syncing = False


@app.post(
    "/migrate/delta/now",
    response_model=TriggerResponse,
    tags=["Migration"],
    summary="Wake the scheduler for an immediate delta sync (non-blocking)",
)
def trigger_delta_now():
    """
    Tells the background scheduler to run a **delta sync right now**
    instead of waiting for the next scheduled interval.

    Returns immediately - the sync runs in the background.
    Poll `/status` to monitor progress.
    """
    if not state.initial_full_done:
        raise HTTPException(
            status_code=425, detail="Initial full migration not completed yet.")
    if not state.scheduler_running:
        raise HTTPException(
            status_code=409,
            detail="Scheduler is not running. Use POST /migrate/delta for a synchronous run.",
        )
    with state._lock:
        if state.is_syncing:
            raise HTTPException(
                status_code=409, detail="A sync is already in progress.")
    state._trigger_event.set()
    return TriggerResponse(
        success=True,
        message="Delta sync triggered. Poll /status for progress.",
    )


# ── Scheduler Control ───────────────────────────────────────────────────

@app.post(
    "/scheduler/stop",
    response_model=TriggerResponse,
    tags=["Scheduler"],
    summary="Stop the auto delta-sync scheduler",
)
def stop_scheduler():
    """Stops the background scheduler. Any in-progress sync is allowed to finish."""
    if not state.scheduler_running:
        raise HTTPException(
            status_code=409, detail="Scheduler is not running.")
    _stop_scheduler()
    return TriggerResponse(success=True, message="Scheduler stopped.")


@app.post(
    "/scheduler/start",
    response_model=TriggerResponse,
    tags=["Scheduler"],
    summary="Start (or restart) the auto delta-sync scheduler",
)
def start_scheduler():
    """
    Starts the background delta-sync scheduler.
    Useful if it was stopped or if the initial full migration failed and was
    later fixed by calling POST /migrate/full manually.
    """
    if not state.initial_full_done:
        raise HTTPException(
            status_code=425,
            detail="Initial full migration not completed. Run POST /migrate/full first.",
        )
    if state.scheduler_running:
        raise HTTPException(
            status_code=409, detail="Scheduler is already running.")
    _start_scheduler()
    return TriggerResponse(
        success=True,
        message=f"Scheduler started. Delta sync every {DELTA_SYNC_INTERVAL_SECONDS}s.",
    )


@app.patch(
    "/scheduler/interval",
    response_model=TriggerResponse,
    tags=["Scheduler"],
    summary="Update the delta-sync interval at runtime",
)
def update_interval(body: UpdateIntervalRequest):
    """
    Changes how often the scheduler triggers a delta sync.
    Takes effect after the current wait period ends.

    Example body: `{ "interval_seconds": 1800 }`
    Minimum: 30 seconds.
    """
    import config as cfg_module
    cfg_module.DELTA_SYNC_INTERVAL_SECONDS = body.interval_seconds
    return TriggerResponse(
        success=True,
        message=f"Interval updated to {body.interval_seconds}s.",
    )


# ================================
# Entry Point
# ================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("etl_api:app", host="0.0.0.0", port=8001, reload=False)
