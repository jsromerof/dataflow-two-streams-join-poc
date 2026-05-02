import logging
from typing import Dict, Tuple

import apache_beam as beam

from db import PostgresClient

logger = logging.getLogger(__name__)

# Output tag names shared between this module and pipeline.py
CHASSIS_TAG = "chassis"
ENGLISH_STATEMENT_TAG = "english_statement"


class JoinDoFn(beam.DoFn):
    """
    Stateful join between chassis and english_statement Kafka streams.

    Input element shape: (source: str, message: dict)
      source is either "chassis" or "english_statement"

    Tagged outputs:
      CHASSIS_TAG          → write to chassis table
      ENGLISH_STATEMENT_TAG → write to english_statement table

    english_statement event logic:
      1. Lookup chassis_id in `chassis`          → enrich + emit
      2. Lookup chassis_id in `chassis_processed` → enrich + emit
      3. Neither found                            → stage in english_statement_stage (side-effect, no output)

    chassis event logic:
      1. Always insert into chassis_processed
      2. Lookup chassis_id in english_statement_stage
         a. Found  → enrich all staged rows, emit them + emit chassis, delete from stage
         b. Not found → emit chassis only
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
            yield beam.pvalue.TaggedOutput(ENGLISH_STATEMENT_TAG, message)
            return

        chassis_processed = self._db.lookup_chassis_processed(chassis_id)
        if chassis_processed:
            message["chassis_number"] = chassis_processed["chassis_number"]
            yield beam.pvalue.TaggedOutput(ENGLISH_STATEMENT_TAG, message)
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
        logger.debug("Inserted chassis %s into chassis_processed", chassis_id)

        staged = self._db.lookup_english_statement_stage(chassis_id)
        if staged:
            for es in staged:
                es["chassis_number"] = message["chassis_number"]
                yield beam.pvalue.TaggedOutput(ENGLISH_STATEMENT_TAG, es)

            self._db.delete_english_statement_stage_by_chassis(chassis_id)
            logger.info(
                "Resolved %d staged english_statement(s) for chassis %s",
                len(staged),
                chassis_id,
            )

        # chassis always lands in chassis table (staged or not)
        yield beam.pvalue.TaggedOutput(CHASSIS_TAG, message)

    # ── entry point ────────────────────────────────────────────────────────────

    def process(self, element: Tuple[str, Dict]):
        source, message = element

        if source == "english_statement":
            yield from self._handle_english_statement(message)
        elif source == "chassis":
            yield from self._handle_chassis(message)
        else:
            logger.warning("Unknown source tag '%s', dropping element", source)


class WriteToPostgresDoFn(beam.DoFn):
    """
    Generic sink DoFn. Inserts a dict into `table` using the given `columns`
    (in order). Uses ON CONFLICT DO NOTHING — relies on the primary key
    constraint defined in the schema.
    """

    def __init__(self, db_config: dict, table: str, columns: list):
        self.db_config = db_config
        self.table = table
        self.columns = columns
        self._db: PostgresClient = None

    def setup(self):
        self._db = PostgresClient(self.db_config)
        self._db.connect()

    def teardown(self):
        if self._db:
            self._db.close()

    def process(self, element: Dict):
        self._db.ensure_connected()
        col_names = ", ".join(self.columns)
        placeholders = ", ".join(["%s"] * len(self.columns))
        values = tuple(element.get(col) for col in self.columns)
        query = (
            f"INSERT INTO {self.table} ({col_names}) VALUES ({placeholders})"
            " ON CONFLICT DO NOTHING"
        )
        with self._db._conn.cursor() as cur:
            cur.execute(query, values)
        self._db._conn.commit()
        logger.debug("Wrote to %s: %s", self.table, element)
