# pocKafkaToSynapse


## Basic Requirements, Infrastructure Setup

#### Kafka
Setup 4 Container Instances, 3 Kafka Brokers and one ZooKeeper, each Container with 0.75 CPU cores and 3 GiB Memory, use following Docker Images: https://hub.docker.com/r/bitnami/kafka/

Some basic commands for Kafka

````
kafka-configs.sh --zookeeper localhost:2181 --alter --entity-type topics --entity-name dhlpoc_kafka_topic --add-config retention.ms=86400000
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic dhlpoc_kafka_topic

````
