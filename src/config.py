import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "consumer_group": os.getenv("KAFKA_CONSUMER_GROUP", "beam-join-consumer"),
}

KAFKA_TOPICS = {
    "chassis": os.getenv("KAFKA_TOPIC_CHASSIS", "chassis"),
    "english_statement": os.getenv("KAFKA_TOPIC_ENGLISH_STATEMENT", "english_statement"),
}
