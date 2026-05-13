from confluent_kafka import Producer

# Configure the producer
p = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    """ Called once for each message transmitted to provide delivery results. """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Produce a message
#p.produce('chassis', '{"chassis_id" : "1" , "chassis_number":"number_1"}', callback=delivery_report)
p.produce('english_statement', '{"english_statement_id" : "2" , "chassis_id" : "1" , "description":"description"}', callback=delivery_report)


# Wait for any outstanding messages to be delivered
p.flush()