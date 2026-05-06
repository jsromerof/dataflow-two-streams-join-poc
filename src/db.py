import logging
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


class PostgresClient:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self._conn = None

    def connect(self):
        self._conn = psycopg2.connect(**self.db_config)

    def ensure_connected(self):
        try:
            if self._conn is None or self._conn.closed:
                self.connect()
                return
            with self._conn.cursor() as cur:
                cur.execute("SELECT 1")
        except Exception:
            logger.warning("DB connection lost, reconnecting...")
            try:
                self._conn.close()
            except Exception:
                pass
            self.connect()

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()

    # ── lookups ────────────────────────────────────────────────────────────────

    def lookup_chassis(self, chassis_id: str) -> Optional[Dict]:
        self.ensure_connected()
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT chassis_id, chassis_number FROM chassis WHERE chassis_id = %s",
                (chassis_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def lookup_chassis_processed(self, chassis_id: str) -> Optional[Dict]:
        self.ensure_connected()
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT chassis_id, chassis_number FROM chassis_processed WHERE chassis_id = %s",
                (chassis_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def lookup_english_statement_stage(self, chassis_id: str) -> List[Dict]:
        self.ensure_connected()
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT english_statement_id, chassis_id, description
                FROM english_statement_stage
                WHERE chassis_id = %s
                """,
                (chassis_id,),
            )
            return [dict(row) for row in cur.fetchall()]

    # ── writes ─────────────────────────────────────────────────────────────────

    def insert_chassis_processed(self, chassis: Dict):
        self.ensure_connected()
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chassis_processed (chassis_id, chassis_number)
                VALUES (%s, %s)
                ON CONFLICT (chassis_id) DO NOTHING
                """,
                (chassis["chassis_id"], chassis["chassis_number"]),
            )
        self._conn.commit()

    def insert_english_statement_stage(self, es: Dict):
        self.ensure_connected()
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO english_statement_stage (english_statement_id, chassis_id, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (english_statement_id) DO NOTHING
                """,
                (es["english_statement_id"], es["chassis_id"], es["description"]),
            )
        self._conn.commit()

    def delete_english_statement_stage_by_chassis(self, chassis_id: str):
        self.ensure_connected()
        with self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM english_statement_stage WHERE chassis_id = %s",
                (chassis_id,),
            )
        self._conn.commit()

    def insert_chassis(self, chassis: Dict):
        self.ensure_connected()
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chassis (chassis_id, chassis_number)
                VALUES (%s, %s)
                ON CONFLICT (chassis_id) DO NOTHING
                """,
                (chassis["chassis_id"], chassis["chassis_number"]),
            )
        self._conn.commit()

    def insert_english_statement(self, es: Dict):
        self.ensure_connected()
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO english_statement (english_statement_id, chassis_id, description, chassis_number)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (english_statement_id) DO NOTHING
                """,
                (
                    es["english_statement_id"],
                    es["chassis_id"],
                    es["description"],
                    es.get("chassis_number"),
                ),
            )
        self._conn.commit()

    def insert_chassis_and_delete_processed(self, chassis: Dict):
        """Atomically promote a chassis from chassis_processed to chassis."""
        self.ensure_connected()
        chassis_id = chassis["chassis_id"]
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO chassis (chassis_id, chassis_number)
                    VALUES (%s, %s)
                    ON CONFLICT (chassis_id) DO NOTHING
                    """,
                    (chassis_id, chassis["chassis_number"]),
                )
                cur.execute(
                    "DELETE FROM chassis_processed WHERE chassis_id = %s",
                    (chassis_id,),
                )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
