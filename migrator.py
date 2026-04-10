"""
Data Migration Module - SQL Server to Neo4j
"""

import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
from neo4j import GraphDatabase

from config import SQL_CONFIG, NEO4J_CONFIG, BATCH_SIZE
from schema import SCHEMA

# ================================
# Logging  (ASCII only - Windows CP1252 safe)
# ================================
logger = logging.getLogger("mesonex_migrator")
logger.setLevel(logging.INFO)

if not logger.handlers:
    fh = RotatingFileHandler(
        "migrator.log", maxBytes=5 * 1024 * 1024, backupCount=3)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)
    # StreamHandler with UTF-8 encoding so Windows console does not crash on special chars
    sh = logging.StreamHandler()
    sh.stream.reconfigure(
        encoding="utf-8", errors="replace") if hasattr(sh.stream, "reconfigure") else None
    logger.addHandler(sh)


# ================================
# Helpers
# ================================

def _build_conn_str() -> str:
    """Build pyodbc connection string from SQL_CONFIG."""
    if SQL_CONFIG.get("trusted_connection", "no").lower() == "yes":
        return (
            f"DRIVER={SQL_CONFIG['driver']};"
            f"SERVER={SQL_CONFIG['server']};"
            f"DATABASE={SQL_CONFIG['database']};"
            "Trusted_Connection=yes;"
        )
    return (
        f"DRIVER={SQL_CONFIG['driver']};"
        f"SERVER={SQL_CONFIG['server']};"
        f"DATABASE={SQL_CONFIG['database']};"
        f"UID={SQL_CONFIG['username']};"
        f"PWD={SQL_CONFIG['password']};"
    )


def _build_sqlalchemy_engine():
    """
    Build a SQLAlchemy engine for pandas read_sql.
    Avoids the pandas UserWarning about non-SQLAlchemy DBAPI2 connections.
    """
    import urllib.parse
    driver = SQL_CONFIG["driver"].strip("{}")
    params = urllib.parse.quote_plus(
        f"DRIVER={{{driver}}};"
        f"SERVER={SQL_CONFIG['server']};"
        f"DATABASE={SQL_CONFIG['database']};"
        f"UID={SQL_CONFIG['username']};"
        f"PWD={SQL_CONFIG['password']};"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)


def _row_fingerprint(record: Dict) -> str:
    """SHA-256 fingerprint of a record dict — detects changed rows in delta sync."""
    return hashlib.sha256(
        json.dumps(record, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


# ================================
# DataMigrator
# ================================

class DataMigrator:
    """
    Migrates manufacturing data from SQL Server views to Neo4j.

    Full migration  (run_full)  - upserts every row from every view.
    Delta migration (run_delta) - only syncs rows that are new or changed
                                  since the last run (SHA-256 fingerprint check).
    """

    def __init__(self):
        self.sql_engine = None          # SQLAlchemy engine (for pandas)
        # pyodbc conn (for neo4j driver check)
        self.sql_conn: Optional[pyodbc.Connection] = None
        self.neo4j_driver = None
        self.schema = SCHEMA

    # ------------------------------------------------------------------ #
    # Connections                                                          #
    # ------------------------------------------------------------------ #

    def connect_sql(self) -> bool:
        """Connect to SQL Server via SQLAlchemy (used by pandas read_sql)."""
        try:
            self.sql_engine = _build_sqlalchemy_engine()
            # Quick connectivity test
            with self.sql_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("[OK] Connected to SQL Server")
            return True
        except Exception as e:
            logger.error(f"[FAIL] SQL Server connection failed: {e}")
            return False

    def connect_neo4j(self) -> bool:
        """Connect to Neo4j."""
        try:
            self.neo4j_driver = GraphDatabase.driver(
                NEO4J_CONFIG["uri"],
                auth=(NEO4J_CONFIG["username"], NEO4J_CONFIG["password"]),
            )
            with self.neo4j_driver.session() as session:
                session.run("RETURN 1")
            logger.info("[OK] Connected to Neo4j")
            return True
        except Exception as e:
            logger.error(f"[FAIL] Neo4j connection failed: {e}")
            return False

    def close(self):
        """Close all open connections."""
        if self.sql_engine:
            self.sql_engine.dispose()
        if self.neo4j_driver:
            self.neo4j_driver.close()

    # ------------------------------------------------------------------ #
    # Schema Setup                                                         #
    # ------------------------------------------------------------------ #

    def clear_database(self):
        """Delete all nodes and relationships from Neo4j."""
        logger.warning("[WARN] Clearing Neo4j database...")
        with self.neo4j_driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        logger.info("[OK] Neo4j database cleared")

    def create_constraints(self):
        """Create unique constraints on primary keys. Safe to re-run (IF NOT EXISTS)."""
        logger.info("Creating constraints...")
        with self.neo4j_driver.session() as session:
            for node_name, cfg in self.schema["nodes"].items():
                pk = cfg["primary_key"]
                try:
                    session.run(
                        f"CREATE CONSTRAINT {node_name}_{pk}_unique IF NOT EXISTS "
                        f"FOR (n:{node_name}) REQUIRE n.{pk} IS UNIQUE"
                    )
                except Exception:
                    pass  # Already exists - harmless

    def create_indexes(self):
        """Create indexes on commonly filtered properties. Safe to re-run (IF NOT EXISTS)."""
        logger.info("Creating indexes...")
        INDEXABLE = ("code", "name", "id", "status", "isactive",
                     "plantcode", "groupcode", "lineid")
        with self.neo4j_driver.session() as session:
            for node_name, cfg in self.schema["nodes"].items():
                for prop in cfg["properties"]:
                    if prop == cfg["primary_key"]:
                        continue
                    if prop.lower().endswith(INDEXABLE):
                        try:
                            session.run(
                                f"CREATE INDEX {node_name}_{prop}_idx IF NOT EXISTS "
                                f"FOR (n:{node_name}) ON (n.{prop})"
                            )
                        except Exception:
                            pass  # Already exists - harmless

    # ------------------------------------------------------------------ #
    # Data Helpers                                                         #
    # ------------------------------------------------------------------ #

    def fetch_data(self, view_name: str) -> pd.DataFrame:
        """Fetch all rows from a SQL Server view using SQLAlchemy engine."""
        try:
            df = pd.read_sql(f"SELECT * FROM {view_name}", self.sql_engine)
            logger.info(f"  Fetched {len(df)} rows from {view_name}")
            return df
        except Exception as e:
            logger.error(f"  Error fetching {view_name}: {e}")
            return pd.DataFrame()

    def sanitize_value(self, value: Any) -> Any:
        """Convert raw SQL value to a Neo4j-compatible Python type."""
        if value is None:
            return None
        if isinstance(value, bool):         # Check bool BEFORE int (bool subclasses int)
            return value
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(value, (pd.Timestamp, datetime)):
            return value.isoformat()
        if isinstance(value, int):
            if -9_223_372_036_854_775_808 <= value <= 9_223_372_036_854_775_807:
                return value
            return str(value)               # Outside Neo4j int64 range
        if isinstance(value, float):
            return float(value)
        return str(value).strip()

    def _prepare_records(self, df: pd.DataFrame, cfg: Dict) -> List[Dict]:
        """
        Sanitize a DataFrame into a list of property dicts.
        - Only keeps properties declared in the schema.
        - Column matching is case-insensitive.
        - Drops rows where the primary key is null.
        """
        pk = cfg["primary_key"]
        col_lookup = {c.lower(): c for c in df.columns}
        records: List[Dict] = []
        for _, row in df.iterrows():
            rec = {}
            for prop in cfg["properties"]:
                col = col_lookup.get(prop.lower())
                if col is not None:
                    rec[prop] = self.sanitize_value(row[col])
            if rec.get(pk) is not None:
                records.append(rec)
        return records

    # ------------------------------------------------------------------ #
    # Node Upsertion                                                       #
    # ------------------------------------------------------------------ #

    def _upsert_batch(self, node_name: str, records: List[Dict], with_fingerprint: bool = False):
        """MERGE a batch of records into Neo4j by primary key."""
        if not records:
            return
        pk = self.schema["nodes"][node_name]["primary_key"]
        props = [p for p in self.schema["nodes"]
                 [node_name]["properties"] if p != pk]
        set_clause = ", ".join(f"n.{p} = record.{p}" for p in props)
        extra = (
            ", n._fingerprint = record._fingerprint, n._last_synced = record._last_synced"
            if with_fingerprint else ""
        )
        query = (
            f"UNWIND $records AS record "
            f"MERGE (n:{node_name} {{{pk}: record.{pk}}}) "
            f"SET {set_clause}{extra}"
        )
        with self.neo4j_driver.session() as session:
            session.run(query, records=records)

    # ------------------------------------------------------------------ #
    # Full Migration                                                       #
    # ------------------------------------------------------------------ #

    def migrate_nodes(self):
        """Upsert every row from every SQL Server view into Neo4j."""
        logger.info("Migrating nodes (FULL)...")
        sorted_nodes = sorted(
            self.schema["nodes"].items(), key=lambda x: x[1]["level"])
        for node_name, cfg in sorted_nodes:
            logger.info(f"  Processing {node_name}...")
            df = self.fetch_data(cfg["view"])
            if df.empty:
                logger.info(f"  No data found in {cfg['view']} - skipping")
                continue
            records = self._prepare_records(df, cfg)
            for i in range(0, len(records), BATCH_SIZE):
                self._upsert_batch(node_name, records[i: i + BATCH_SIZE])
            logger.info(f"  [OK] {node_name}: {len(records)} nodes upserted")

    # ------------------------------------------------------------------ #
    # Delta Migration                                                      #
    # ------------------------------------------------------------------ #

    def _get_stored_fingerprints(self, node_name: str, pk: str) -> Dict[Any, str]:
        """Load {pk_value: fingerprint} stored in Neo4j from the previous sync."""
        result: Dict[Any, str] = {}
        with self.neo4j_driver.session() as session:
            for rec in session.run(
                f"MATCH (n:{node_name}) WHERE n._fingerprint IS NOT NULL "
                f"RETURN n.{pk} AS k, n._fingerprint AS fp"
            ):
                result[rec["k"]] = rec["fp"]
        return result

    def migrate_nodes_delta(self):
        """
        Only sync rows that are new or changed since the last run.
        Uses SHA-256 fingerprint comparison — unchanged rows are skipped entirely.
        """
        logger.info("Migrating nodes (DELTA)...")
        sorted_nodes = sorted(
            self.schema["nodes"].items(), key=lambda x: x[1]["level"])
        now = datetime.utcnow().isoformat()

        for node_name, cfg in sorted_nodes:
            pk = cfg["primary_key"]
            logger.info(f"  Processing {node_name} (delta)...")
            df = self.fetch_data(cfg["view"])
            if df.empty:
                continue

            all_records = self._prepare_records(df, cfg)

            # Attach fingerprint and sync timestamp to each record
            for rec in all_records:
                rec["_fingerprint"] = _row_fingerprint(rec)
                rec["_last_synced"] = now

            # Compare against what Neo4j already has
            stored = self._get_stored_fingerprints(node_name, pk)
            to_upsert = [
                rec for rec in all_records
                if rec[pk] not in stored or stored[rec[pk]] != rec["_fingerprint"]
            ]
            skipped = len(all_records) - len(to_upsert)
            logger.info(
                f"  Changed/New: {len(to_upsert)} | Unchanged (skipped): {skipped}")

            if not to_upsert:
                continue

            for i in range(0, len(to_upsert), BATCH_SIZE):
                self._upsert_batch(
                    node_name, to_upsert[i: i + BATCH_SIZE], with_fingerprint=True)
            logger.info(f"  [OK] {node_name}: {len(to_upsert)} nodes synced")

    # ------------------------------------------------------------------ #
    # Relationships                                                        #
    # ------------------------------------------------------------------ #

    def create_relationships(self):
        """Create all schema relationships in Neo4j using MERGE (idempotent)."""
        logger.info("Creating relationships...")
        with self.neo4j_driver.session() as session:
            for rel in self.schema["relationships"]:
                try:
                    result = session.run(
                        f"MATCH (a:{rel['from_node']}), (b:{rel['to_node']}) "
                        f"WHERE a.{rel['from_property']} IS NOT NULL "
                        f"  AND b.{rel['to_property']} IS NOT NULL "
                        f"  AND a.{rel['from_property']} = b.{rel['to_property']} "
                        f"MERGE (a)-[r:{rel['name']}]->(b) "
                        f"RETURN count(r) AS cnt"
                    )
                    count = result.single()["cnt"]
                    logger.info(f"  [OK] {rel['name']}: {count} relationships")
                except Exception as e:
                    logger.error(f"  [FAIL] {rel['name']}: {e}")

    # ------------------------------------------------------------------ #
    # Verification                                                         #
    # ------------------------------------------------------------------ #

    def verify_migration(self) -> Dict[str, Any]:
        """Count all nodes and relationships in Neo4j and return the report."""
        report: Dict[str, Any] = {"nodes": {}, "relationships": {}}
        with self.neo4j_driver.session() as session:
            for node_name in self.schema["nodes"]:
                count = session.run(
                    f"MATCH (n:{node_name}) RETURN count(n) AS cnt"
                ).single()["cnt"]
                report["nodes"][node_name] = count
                logger.info(f"  {node_name}: {count} nodes")
            for rel in self.schema["relationships"]:
                count = session.run(
                    f"MATCH ()-[r:{rel['name']}]->() RETURN count(r) AS cnt"
                ).single()["cnt"]
                report["relationships"][rel["name"]] = count
        return report

    # ------------------------------------------------------------------ #
    # Sync Metadata                                                        #
    # ------------------------------------------------------------------ #

    def get_last_sync_info(self) -> Dict[str, Optional[str]]:
        """Return the most recent _last_synced timestamp per node label."""
        info: Dict[str, Optional[str]] = {}
        with self.neo4j_driver.session() as session:
            for node_name in self.schema["nodes"]:
                try:
                    rec = session.run(
                        f"MATCH (n:{node_name}) WHERE n._last_synced IS NOT NULL "
                        f"RETURN max(n._last_synced) AS ts"
                    ).single()
                    info[node_name] = rec["ts"] if rec else None
                except Exception:
                    info[node_name] = None
        return info

    # ------------------------------------------------------------------ #
    # Orchestration                                                        #
    # ------------------------------------------------------------------ #

    def run_full(self, clear_existing: bool = False) -> Dict[str, Any]:
        """Run a full migration. Raises RuntimeError on connection failure."""
        if not self.connect_sql():
            raise RuntimeError("Cannot connect to SQL Server")
        if not self.connect_neo4j():
            raise RuntimeError("Cannot connect to Neo4j")
        if clear_existing:
            self.clear_database()
        self.create_constraints()
        self.create_indexes()
        self.migrate_nodes()
        self.create_relationships()
        return self.verify_migration()

    def run_delta(self) -> Dict[str, Any]:
        """Run a delta migration. Raises RuntimeError on connection failure."""
        if not self.connect_sql():
            raise RuntimeError("Cannot connect to SQL Server")
        if not self.connect_neo4j():
            raise RuntimeError("Cannot connect to Neo4j")
        self.create_constraints()
        self.create_indexes()
        self.migrate_nodes_delta()
        self.create_relationships()
        return self.verify_migration()
