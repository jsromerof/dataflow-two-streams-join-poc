"""
Two-stream Kafka → Postgres pipeline.

Reads chassis and english_statement events from Kafka, joins them via
JoinDoFn, and persists the resolved records to Postgres.

Run example (DirectRunner, requires expansion service for Kafka IO):
    python pipeline.py \
        --runner=DirectRunner \
        --expansion_service_port=8097

Environment variables (see config.py for defaults):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP
    KAFKA_TOPIC_CHASSIS, KAFKA_TOPIC_ENGLISH_STATEMENT
"""

import json
import logging

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from config import DB_CONFIG, KAFKA_CONFIG, KAFKA_TOPICS
from dofns import (
    CHASSIS_TAG,
    ENGLISH_STATEMENT_TAG,
    JoinDoFn,
    WriteToPostgresDoFn,
)

logger = logging.getLogger(__name__)


def _tag_message(kv, source: str):
    """Decode a Kafka (key, value) bytes pair and tag it with its source name."""
    _, value = kv
    return (source, json.loads(value.decode("utf-8")))


def build_pipeline(p: beam.Pipeline) -> None:
    kafka_consumer_config = {
        "bootstrap.servers": KAFKA_CONFIG["bootstrap_servers"],
        "group.id": KAFKA_CONFIG["consumer_group"],
        "auto.offset.reset": "latest",
    }

    chassis_stream = (
        p
        | "ReadChassis"
        >> ReadFromKafka(
            consumer_config=kafka_consumer_config,
            topics=[KAFKA_TOPICS["chassis"]],
            with_metadata=False,
        )
        | "TagChassis" >> beam.Map(_tag_message, source="chassis")
    )

    english_statement_stream = (
        p
        | "ReadEnglishStatement"
        >> ReadFromKafka(
            consumer_config=kafka_consumer_config,
            topics=[KAFKA_TOPICS["english_statement"]],
            with_metadata=False,
        )
        | "TagEnglishStatement"
        >> beam.Map(_tag_message, source="english_statement")
    )

    merged = (
        (chassis_stream, english_statement_stream)
        | "MergeStreams" >> beam.Flatten()
    )

    results = merged | "JoinLogic" >> beam.ParDo(
        JoinDoFn(db_config=DB_CONFIG)
    ).with_outputs(CHASSIS_TAG, ENGLISH_STATEMENT_TAG)

    # chassis  →  chassis table
    (
        results[CHASSIS_TAG]
        | "WriteChassis"
        >> beam.ParDo(
            WriteToPostgresDoFn(
                db_config=DB_CONFIG,
                table="chassis",
                columns=["chassis_id", "chassis_number"],
            )
        )
    )

    # english_statement  →  english_statement table (enriched with chassis_number)
    (
        results[ENGLISH_STATEMENT_TAG]
        | "WriteEnglishStatement"
        >> beam.ParDo(
            WriteToPostgresDoFn(
                db_config=DB_CONFIG,
                table="english_statement",
                columns=["english_statement_id", "chassis_id", "description", "chassis_number"],
            )
        )
    )


def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        build_pipeline(p)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    run()
