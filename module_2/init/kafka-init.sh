# wait until kafka is reachable
kafka-topics --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094 --list

# create topics
echo -e 'Creating topics'
kafka-topics --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094 --create --if-not-exists --topic messages --replication-factor 2 --partitions 3
kafka-topics --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094 --create --if-not-exists --topic filtered_messages --replication-factor 2 --partitions 3
kafka-topics --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094 --create --if-not-exists --topic blocked_users --replication-factor 2 --partitions 3
kafka-topics --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094 --create --if-not-exists --topic blocked_words --replication-factor 2 --partitions 3
kafka-topics --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094 --create --if-not-exists --topic blocked_messages --replication-factor 2 --partitions 3

# list created topics
echo 'Successfully created topic:'
kafka-topics --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094 --list