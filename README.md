# pocKafkaToSynapse


## Setup

### 1.1 Kafka
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


### 1.2 Message Producer VM
* Provision a VM (e.g. Standard E8bds v5 (8 vcpus, 64 GiB memory)), Windows 10 Pro.
* Install Visual Studio Code
* Copy a set of XML files to a dedicated directory
* Create a new .NET SDK C# Project, add followig Libraries:

````
<ItemGroup>
    <PackageReference Include="Confluent.Kafka" Version="1.5.0" />
    <PackageReference Include="kafka-sharp" Version="1.4.3" />
    <PackageReference Include="Microsoft.Extensions.Hosting" Version="3.1.6" />
</ItemGroup>
````
Add MessageProducer.cs from this Repo and run the Program. It will send around 25.000 XML Files per second to Kafka

### 1.3 SQL Server / Synapse
* Set up a SQL Server (8 vCores, Gen5) and / or a Synapse Instance (Dedicated SQL Pool, DW400c)
* Run SQL Script of this Repo to create all 15 tables (SQL script is for SQL Server, for Synapse be aware of explicitly define ColumnStore Index, Heap table,.. according to your needs)


### 1.4 Databricks
* Setup a Databricks Workspace
* Setup a TestCluster (Standard_DS5_v2, 56G Memory, 16 Cores, 4 Min Workers, 5 Max Workers, 1 Driver)
* Install following Libraries on Cluster
    - com.databricks:spark-xml_2.12:0.14.0
    - com.microsoft.azure:spark-mssql-connector_2.12:1.2.0


| File  | Content |
| ------------- | ------------- |
| pocScen6_StreamtoSQLCEReducedOpt2.scala  | Content Cell  |
| pocScen8_Basedon7_5Tables.scala  | Content Cell  |

