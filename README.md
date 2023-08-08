# Kafka-Core-Java
Kafka producer and consumer implementation using core java <br>
Kafka version: 3.5.0 <br>
Steps to run the app: <br>
1. Bring up a Zookeeper instance.
2. create brokers(based on replication factor).
3. create topic(configure replication factor, partitions).
4. setup ProducerConfig in MessageProducer.java(bootstrap server, key serialization, value serialization).
5. Compile and run the app (Gradle).
6. Use the IDE CLI to produce messages(handeled by CommandLineLauncher.java).
