from confluent_kafka import Producer

# Configure the producer
p = Producer({'bootstrap.servers': '0.0.0.0:9092'})

def delivery_report(err, msg):
    """ Called once for each message transmitted to provide delivery results. """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Produce a message
p.produce('test-topic', 'Hello Kafka2!', callback=delivery_report)

# Wait for any outstanding messages to be delivered
p.flush()