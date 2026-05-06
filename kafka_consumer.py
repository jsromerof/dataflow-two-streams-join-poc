from confluent_kafka import Consumer

conf = {
    "bootstrap.servers": "localhost:9092",  # change if needed
    "group.id": "my-consumer-group",
    "auto.offset.reset": "earliest",  # read from beginning if no offset
}

consumer = Consumer(conf)

topic = "chassis"
consumer.subscribe([topic])

print(f"Listening to topic: {topic}")

try:
    while True:
        msg = consumer.poll(1.0)  # wait 1 second

        if msg is None:
            continue

        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        print(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()