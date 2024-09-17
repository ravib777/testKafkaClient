# Kafka Client Utility

## Overview
This Java project provides a command-line utility for interacting with Apache Kafka. It allows users to produce messages, consume messages, describe Kafka topics, and view consumer group details. The utility is packaged as a fat JAR, including all necessary dependencies to run Kafka clients.

## Building the Project
This project uses Maven for dependency management and building. To build the project, navigate to the project root directory and run the following command:

```shell
 mvn clean compile assembly:single
```

This command compiles the project and creates a fat JAR in the `target` directory, which contains all the necessary dependencies.

## Usage
The utility accepts various parameters based on the operation you want to perform. The general usage pattern is:

```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation <operation> [other options]
```

### Parameters
- `--operation`: The operation to perform. Accepts `produce`, `consume`, `describe-topic`, and `describe-group` and `describe-cluster`.
- `--bootstrap-server`: The Kafka bootstrap server(s) to connect to. Format: `host1:port,host2:port`.
- `--topic`: The name of the Kafka topic to interact with.
- `--num-messages`: he number of messages to produce or consume. Default is `1`.
- `--group`: (Only for `consume` and `describe-group`) The consumer group ID.
- `--config-file`: Path of the config file including kafka client's security and other configs.
- `--format`: Format of the message to produce/consume. Supported values `string` (default) ,`avro`,`protobuf`,`jsonsr`. Expect for `string`, other formats require `schema.registry.url` and other Schema Registry security properties to be set in config file passed through `--config-file`. Note this only defines `value`'s format. `Key` format is hardcoded to `string`.
- `--schema`: Schema of the Record's value.
- `--schema-id`: Schema id for the schema from Schema Registry. Use this instead of `--schema` when schema exists on Schema Registry.
- `--send-keys` : Use this with `operation produce` to send Keys. Default is `false`


### Examples

#### Producing Messages
To produce messages to a topic:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation produce --bootstrap-server localhost:9092 --topic myTopic --num-messages 5
```

#### Producing Messages with Keys
To produce messages to a topic:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation produce --bootstrap-server localhost:9092 --topic myTopic --send-keys true --num-messages 5
```

#### Producing Avro Messages
- Create a client.properties file with following Schema Registry configs:
```shell
cat client.properties:
# Set the following when producing/consuming in Schema Registry aware format
schema.registry.url=https://psrc-xxx.us-central1.gcp.confluent.cloud
# Set below configs if Schema Registry is secured
basic.auth.credentials.source=USER_INFO
schema.registry.basic.auth.user.info=xxxx:xxxxxxxxxxxx
```
To produce messages to a topic using --schema:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation produce --bootstrap-server localhost:9092 --topic myTopic --num-messages 1 --config-file client.properties --format avro --schema '{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
>{"f1": "value1"}
```

To produce messages to a topic using --schema-id:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation produce --bootstrap-server localhost:9092 --topic myTopic --num-messages 1 --config-file client.properties --format avro --schema-id 101
>{"f1": "value1"}
```

#### Producing Protobuf Messages
- Create a client.properties file with following Schema Registry configs:
```shell
cat client.properties:
# Set the following when producing/consuming in Schema Registry aware format
schema.registry.url=https://psrc-xxx.us-central1.gcp.confluent.cloud
# Set below configs if Schema Registry is secured
basic.auth.credentials.source=USER_INFO
schema.registry.basic.auth.user.info=xxxx:xxxxxxxxxxxx
```
To produce messages to a topic using --schema:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation produce --bootstrap-server localhost:9092 --topic myTopic --num-messages 1 --config-file client.properties --format protobuf  --schema 'syntax = "proto3"; message SampleRecord {int32 my_field1 = 1 ; string my_field2 = 2;}'
>{"my_field1": 123, "my_field2": "hello world"}
```

To produce messages to a topic using --schema-id:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation produce --bootstrap-server localhost:9092 --topic myTopic --num-messages 1 --config-file client.properties --format protobuf  --schema-id 101
>{"my_field1": 123, "my_field2": "hello world"}
```

#### Producing JSon Schema Messages
- Create a client.properties file with following Schema Registry configs:
```shell
cat client.properties:
# Set the following when producing/consuming in Schema Registry aware format
schema.registry.url=https://psrc-xxx.us-central1.gcp.confluent.cloud
# Set below configs if Schema Registry is secured
basic.auth.credentials.source=USER_INFO
schema.registry.basic.auth.user.info=xxxx:xxxxxxxxxxxx
```
To produce messages to a topic using --schema:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation produce --bootstrap-server localhost:9092 --topic myTopic --num-messages 1 --config-file client.properties --format jsonsr  --schema '{"type":"object","properties":{"f1":{"type":"string"}}}'
>{"f1": "value1"}
```

To produce messages to a topic using --schema-id:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation produce --bootstrap-server localhost:9092 --topic myTopic --num-messages 1 --config-file client.properties --format jsonsr  --schema-id 101
>{"f1": "value1"}
```

#### Consuming Messages
To consume messages from a topic:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation consume --bootstrap-server localhost:9092 --topic myTopic --group myGroup --num-messages 10
```

#### Consuming Messages in Schema Registry aware format
- Create a client.properties file with following Schema Registry configs:
```shell
cat client.properties:
# Set the following when producing/consuming in Schema Registry aware format
schema.registry.url=https://psrc-xxx.us-central1.gcp.confluent.cloud
# Set below configs if Schema Registry is secured
basic.auth.credentials.source=USER_INFO
schema.registry.basic.auth.user.info=xxxx:xxxxxxxxxxxx
```
To consume messages from a topic use `--format <avro|protobuf|jsonsr>`:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation consume --bootstrap-server localhost:9092 --topic myTopic --group myGroup --num-messages 10 --config-file client.properties --format avro
```

#### Describing a Topic
To describe a Kafka topic:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation describe-topic --bootstrap-server localhost:9092 --topic myTopic
```

#### Describing a Consumer Group
To describe a Kafka consumer group:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation describe-group --bootstrap-server localhost:9092 --group myGroup
```

#### Describing cluster
To describe a Kafka cluster:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation describe-cluster --bootstrap-server localhost:9092 
```

#### Using with Confluent Cloud
To descibe topic in Confluent Cloud:

- Create a client.properties file with following configs:

```shell
cat client.properties:
ssl.endpoint.identification.algorithm=https
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="xxxxxx" password="xxxxxx";
security.protocol=SASL_SSL
# Set the following when producing/consuming in Schema Registry aware format
schema.registry.url=https://psrc-xxx.us-central1.gcp.confluent.cloud
basic.auth.credentials.source=USER_INFO
schema.registry.basic.auth.user.info=xxxx:xxxxxxxxxxxx
```


Run the command passing `--config-file client.properties`
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation describe-topic --bootstrap-server pkc-xxx.xx.gcp.confluent.cloud:9092 --topic myTopic --config-file client.properties
```

#### Enable DEBUG kafka clients logging
To enable DEBUG logging for kafka clients:
- Set rootLogger to DEBUG:
  ```shell
  log4j.rootLogger=DEBUG, stderr
  ```
- For specific class/package TRACE logging, add:
  ```shell
  log4j.logger.org.apache.kafka.clients.producer.internals.ProducerBatch=TRACE
  ```
- Run the jar with `-Dlog4j.configuration=file:`
  ```shell
  java -Dlog4j.configuration=file:src/main/resources/log4j.properties -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation <operation>
  ```
