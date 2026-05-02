# Dataflow Two Streams Join POC

Apache Beam 2.71 pipeline that reads `chassis` and `english_statement` events from Kafka, joins them, and persists the resolved records to PostgreSQL.

## Architecture

```
Kafka: chassis            ──┐
                             ├── Flatten ── JoinDoFn ──┬── chassis         ── chassis table
Kafka: english_statement  ──┘                          └── english_statement ── english_statement table
                                                            (side-effects)
                                                             ├── english_statement_stage (holding area)
                                                             └── chassis_processed      (lookup cache)
```

## Join logic

### `english_statement` event
| Condition | Action |
|---|---|
| `chassis_id` found in `chassis` table | Enrich with `chassis_number`, write to `english_statement` |
| Not found, but in `chassis_processed` | Enrich with `chassis_number`, write to `english_statement` |
| Neither found | Insert into `english_statement_stage` and wait |

### `chassis` event
| Condition | Action |
|---|---|
| Always | Insert into `chassis_processed` first |
| Staged rows found in `english_statement_stage` | Enrich all staged rows, write them to `english_statement`, delete from stage, write chassis to `chassis` |
| No staged rows | Write chassis to `chassis` table only |

## Project structure

```
├── requirements.txt
├── schema.sql
└── src/
    ├── config.py       # env-var configuration
    ├── db.py           # PostgresClient (all SQL operations)
    ├── dofns.py        # JoinDoFn + WriteToPostgresDoFn
    └── pipeline.py     # Kafka reads → merge → join → Postgres writes
```

## Prerequisites

- Python 3.11
- JDK 11+ (for the Beam expansion service)
- Kafka broker running
- PostgreSQL database

## Setup

### 1. Install Python dependencies

```bash
cd dataflow-two-streams-join-poc
pip install -r requirements.txt
```

### 2. Apply the database schema

```bash
psql -h localhost -U postgres -d dataflow_db -f schema.sql
```

### 3. Start the Beam Java Expansion Service

`ReadFromKafka` is a cross-language transform that requires a Java sidecar process. Download the JAR once:

```bash
wget https://repo1.maven.org/maven2/org/apache/beam/beam-sdks-java-io-expansion-service/2.71.0/beam-sdks-java-io-expansion-service-2.71.0.jar
```

Start it and leave the terminal open:

```bash
java -jar beam-sdks-java-io-expansion-service-2.71.0.jar 8097
```

## Run the pipeline

```bash
cd src

# Minimum — uses localhost defaults from config.py
python pipeline.py \
  --runner=DirectRunner \
  --expansion_service_port=8097
```

With explicit Kafka/Postgres settings:

```bash
DB_HOST=localhost \
DB_PORT=5432 \
DB_NAME=dataflow_db \
DB_USER=postgres \
DB_PASSWORD=secret \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
KAFKA_CONSUMER_GROUP=beam-join-consumer \
python pipeline.py \
  --runner=DirectRunner \
  --expansion_service_port=8097
```

The pipeline runs indefinitely (streaming mode). Stop it with `Ctrl+C`.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `DB_HOST` | `localhost` | Postgres host |
| `DB_PORT` | `5432` | Postgres port |
| `DB_NAME` | `dataflow_db` | Database name |
| `DB_USER` | `postgres` | Database user |
| `DB_PASSWORD` | `postgres` | Database password |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_CONSUMER_GROUP` | `beam-join-consumer` | Kafka consumer group id |
| `KAFKA_TOPIC_CHASSIS` | `chassis` | Chassis topic name |
| `KAFKA_TOPIC_ENGLISH_STATEMENT` | `english_statement` | English statement topic name |

## Manual testing

Once the pipeline is running, publish test messages to Kafka:

```bash
# chassis event first
echo '{"chassis_id":"c1","chassis_number":"XYZ-001"}' | \
  kafka-console-producer.sh --broker-list localhost:9092 --topic chassis

# english_statement that references it
echo '{"english_statement_id":"es1","chassis_id":"c1","description":"front axle"}' | \
  kafka-console-producer.sh --broker-list localhost:9092 --topic english_statement

# english_statement arriving BEFORE its chassis (will be staged)
echo '{"english_statement_id":"es2","chassis_id":"c999","description":"rear axle"}' | \
  kafka-console-producer.sh --broker-list localhost:9092 --topic english_statement

# chassis arriving AFTER — resolves es2 from the stage table
echo '{"chassis_id":"c999","chassis_number":"ABC-999"}' | \
  kafka-console-producer.sh --broker-list localhost:9092 --topic chassis
```

Then verify results in Postgres:

```sql
SELECT * FROM chassis;
SELECT * FROM english_statement;
SELECT * FROM english_statement_stage;  -- should be empty after c999 arrives
SELECT * FROM chassis_processed;
```

## Troubleshooting

| Error | Fix |
|---|---|
| `Connection refused` on expansion service port | Make sure the JAR is running before starting the pipeline |
| `psycopg2.OperationalError` | Check env vars match your Postgres setup |
| `ModuleNotFoundError` | Run `python pipeline.py` from the `src/` directory so local imports resolve |
| Kafka messages not consumed | Change `auto.offset.reset` from `latest` to `earliest` in `config.py` if topics have pre-existing messages |
