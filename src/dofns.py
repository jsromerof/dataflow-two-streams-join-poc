import logging
from typing import Dict, Tuple

import apache_beam as beam

from db import PostgresClient

logger = logging.getLogger(__name__)


class JoinDoFn(beam.DoFn):
    """
    Stateful join between chassis and english_statement Kafka streams.

    Input element shape: (source: str, message: dict)
      source is either "chassis" or "english_statement"

    All persistence happens inside this DoFn — no downstream sink is needed.

    english_statement event logic:
      1. Lookup chassis_id in `chassis`            → enrich + insert into english_statement
      2. Lookup chassis_id in `chassis_processed`  → enrich + insert into english_statement
      3. Neither found                             → stage in english_statement_stage

    chassis event logic:
      1. Insert into chassis_processed
      2. Lookup chassis_id in english_statement_stage
         a. Found  → insert each staged english_statement,
                     then in ONE transaction: insert chassis + delete from chassis_processed
         b. Empty  → insert chassis only
    """

    def __init__(self, db_config: dict):
        self.db_config = db_config
        self._db: PostgresClient = None

    def setup(self):
        self._db = PostgresClient(self.db_config)
        self._db.connect()

    def teardown(self):
        if self._db:
            self._db.close()

    # ── english_statement path ─────────────────────────────────────────────────

    def _handle_english_statement(self, message: Dict):
        chassis_id = message["chassis_id"]

        chassis = self._db.lookup_chassis(chassis_id)
        if chassis:
            message["chassis_number"] = chassis["chassis_number"]
            self._db.insert_english_statement(message)
            return

        chassis_processed = self._db.lookup_chassis_processed(chassis_id)
        if chassis_processed:
            message["chassis_number"] = chassis_processed["chassis_number"]
            self._db.insert_english_statement(message)
            return

        # chassis not seen yet — stage for later resolution
        self._db.insert_english_statement_stage(message)
        logger.info(
            "Staged english_statement %s (chassis %s not found yet)",
            message.get("english_statement_id"),
            chassis_id,
        )

    # ── chassis path ───────────────────────────────────────────────────────────

    def _handle_chassis(self, message: Dict):
        chassis_id = message["chassis_id"]

        self._db.insert_chassis_processed(message)

        staged = self._db.lookup_english_statement_stage(chassis_id)
        if staged:
            for es in staged:
                es["chassis_number"] = message["chassis_number"]
                self._db.insert_english_statement(es)

            self._db.delete_english_statement_stage_by_chassis(chassis_id)
            self._db.insert_chassis_and_delete_processed(message)
            logger.info(
                "Resolved %d staged english_statement(s) for chassis %s",
                len(staged),
                chassis_id,
            )
            return

        self._db.insert_chassis(message)

    # ── entry point ────────────────────────────────────────────────────────────

    def process(self, element: Tuple[str, Dict]):
        source, message = element

        if source == "english_statement":
            self._handle_english_statement(message)
        elif source == "chassis":
            self._handle_chassis(message)
        else:
            logger.warning("Unknown source tag '%s', dropping element", source)
