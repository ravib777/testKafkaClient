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
- `--operation`: The operation to perform. Accepts `produce`, `consume`, `describeTopic`, and `describeGroup`.
- `--bootstrapServer`: The Kafka bootstrap server(s) to connect to. Format: `host1:port,host2:port`.
- `--topicName`: The name of the Kafka topic to interact with.
- `--numMessages`: he number of messages to produce or consume. Default is `1`.
- `--group`: (Only for `consume` and `describeGroup`) The consumer group ID.
- `--configFile`: Path of the config file including kafka client's security and other configs.

### Examples

#### Producing Messages
To produce messages to a topic:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation produce --bootstrapServer localhost:9092 --topicName myTopic --numMessages 5
```

#### Consuming Messages
To consume messages from a topic:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation consume --bootstrapServer localhost:9092 --topicName myTopic --group myGroup --numMessages 10
```

#### Describing a Topic
To describe a Kafka topic:
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation describeTopic --bootstrapServer localhost:9092 --topicName myTopic
```

#### Describing a Consumer Group
To describe a Kafka consumer group:
```shell
java -jar target/kafka-client-utility.jar --operation describeGroup --bootstrapServer localhost:9092 --group myGroup
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
```

Run the command passing `--configFile client.properties`
```shell
java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation describeTopic --bootstrapServer pkc-xxx.xx.gcp.confluent.cloud:9092 --topicName myTopic --configFile client.properties
```
