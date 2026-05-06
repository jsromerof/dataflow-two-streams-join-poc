kafka-topics --bootstrap-server kafka:29092 --list

echo 'Creating kafka topics...'
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic chassis --partitions 3 --replication-factor 1
echo 'Creating kafka topics...'
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic english_statement --partitions 3 --replication-factor 1
echo 'Successfully created the following topics:'
kafka-topics --bootstrap-server kafka:29092 --list