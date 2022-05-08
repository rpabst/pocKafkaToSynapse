# pocKafkaToSynapse


## 1 Basic Requirements, Infrastructure Setup

#### 1.1 Kafka
Setup 4 Container Instances, 3 Kafka Brokers and one ZooKeeper, each Container with 0.75 CPU cores and 3 GiB Memory, use following Docker Images: https://hub.docker.com/r/bitnami/kafka/

Set retention period for a dedicated topic

````
kafka-configs.sh --zookeeper localhost:2181 --alter --entity-type topics --entity-name dhlpoc_kafka_topic --add-config retention.ms=86400000
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic dhlpoc_kafka_topic

````
Create a topic with 10 Partitions

````
kafka-topics.sh --bootstrap-server=kafka-odspoc.westeurope.azurecontainer.io:9092 --create --topic dhlpocboundp10 –-partitions 10 –-replication-factor 3 –-config max.message.bytes=64000 –-config flush.messages=1 --config retention.ms=8640000000
````
