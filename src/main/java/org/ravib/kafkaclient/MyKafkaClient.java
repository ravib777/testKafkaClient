package org.ravib.kafkaclient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.Node;

import java.io.IOException;
import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


public class MyKafkaClient {

    static String operation;
    static String bootstrapServer = "localhost:9092";
    static String topicName;
    static String configFile;
    static Integer numMessages = 1;
    static String group;
    static boolean sendKeys = false;

    public static void main(String[] args) {

        if (args.length < 1) {
            System.out.println("Usage: java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar--operation <operation> [other options]\n" +
                    "\n");
            System.exit(1);
        }
        Properties properties = new Properties();


        Map<String, String> arguments = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                if (i + 1 < args.length) {
                    String key = args[i].substring(2);
                    String value = args[i + 1];
                    arguments.put(key, value);
                }
                i++;
            }
        }

        boolean bootStrapPreset = false;
        for (Map.Entry<String, String> entry : arguments.entrySet()) {
            switch (entry.getKey()) {
                case "operation":
                    operation = entry.getValue();
                    break;
                case "bootstrapServer":
                    bootStrapPreset = true;
                    bootstrapServer = entry.getValue();
                    break;
                case "sendKeys":
                    sendKeys = Boolean.parseBoolean(entry.getValue());
                    break;
                case "topicName":
                    topicName = entry.getValue();
                    break;
                case "group":
                    group = entry.getValue();
                    break;
                case "numMessages":
                    numMessages = Integer.parseInt(entry.getValue());
                    break;
                case "configFile":
                    configFile = entry.getValue();
                    try {
                        properties.load(new FileInputStream(configFile));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                default:
                    System.out.println("Unknown config: " + entry.getKey());
                    break;
            }
        }

        if (!bootStrapPreset) {
            System.out.println("--bootstrap-server <broker:port> was not set, default localhost:9092 is being used");
        }
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");

        switch (operation) {
            case "produce":
                if (topicName != null) {
                    produceMessages(properties, topicName, numMessages, sendKeys);
                } else {
                    System.out.println("consume option requires --topic <topicName> parameter to be set");
                }
                break;
            case "consume":
                if (topicName != null) {
                    consumeMessages(properties, topicName, group, numMessages);
                } else {
                    System.out.println("consume option requires --topic <topicName> parameter to be set");
                }
                break;
            case "describeTopic":
                if (topicName != null) {
                    describeTopic(properties, topicName);
                } else {
                    System.out.println("describeTopic option requires --topic <topicName> parameter to be set");
                }
                break;
            case "describeGroup":
                if (group != null) {
                    describeConsumer(properties, group);
                } else {
                    System.out.println("describeGroup option requires --group <groupName> parameter to be set");
                }
                break;
            case "describeCluster":
                describeCluster(properties);
                break;
            default:
                System.out.println("Unsupported operation. Use produce, consume, describeTopic or describeGroup .");
                System.exit(1);
        }
    }


    private static void produceMessages(Properties properties, String topic, Integer numMessages, boolean sendKeys) {
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Scanner scanner = new Scanner(System.in);
        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            int counter = 0;
            if (sendKeys) {
                while (counter < numMessages) {
                    System.out.println("Enter message " + (counter + 1) + " in <key>|value format as --sendKeys true was set. Type exit to exit anytime: ");
                    String message = scanner.nextLine();
                    if ("exit".equalsIgnoreCase(message.trim())) {
                        break;
                    }
                    String[] keyValue = message.split("\\|", 2);
                    if (keyValue.length == 2) {
                        producer.send(new ProducerRecord<>(topic, keyValue[0], keyValue[1]));
                        System.out.println("Message sent: " + message);
                    } else {
                        System.out.println("Invalid input. Please use the format 'key|value'.");
                    }
                    producer.send(new ProducerRecord<>(topic, null, message));
                    counter++;
                }
            } else {
                while (counter < numMessages) {
                    System.out.println("Enter message " + (counter + 1) + ". Type exit to exit anytime:: ");
                    String message = scanner.nextLine();
                    if ("exit".equalsIgnoreCase(message.trim())) {
                        break;
                    }
                    producer.send(new ProducerRecord<>(topic, null, message));
                    counter++;
                }
            }
            producer.close();
            System.out.println("Finished sending " + numMessages + " messages to Kafka.");
        }
    }

    private static void consumeMessages(Properties properties, String topic, String group, Integer numMessages) {
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        if (group != null) {
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        } else {
            System.out.println(" --group <groupName> option missing while consuming the message. Exiting now");
            System.exit(1);
        }
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            int counter = 0;
            while (counter < numMessages) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Consumed record with key : " + record.key() + "  and value: " + record.value());
                    counter++;
                }
            }
        }
    }

    private static void describeTopic(Properties properties, String topic) {
        try (AdminClient adminClient = AdminClient.create(properties)) {
            ArrayList<String> topicList = new ArrayList();
            topicList.add(topic);
            DescribeTopicsResult result = adminClient.describeTopics(topicList);
            TopicDescription topicDescription = result.values().get(topicName).get();

            System.out.println("\n\nTopic: " + topicName);
            System.out.println("-------------------------------------------------------------------------");
            System.out.printf("%-12s %-15s %-30s %-30s%n", "Partition", "Leader", "Replicas", "ISR");
            System.out.println("-------------------------------------------------------------------------");

            for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
                String replicas = partitionInfo.replicas().stream()
                        .map(Node::idString)
                        .reduce((n1, n2) -> n1 + ", " + n2)
                        .orElse("No Replicas");

                String isr = partitionInfo.isr().stream()
                        .map(Node::idString)
                        .reduce((n1, n2) -> n1 + ", " + n2)
                        .orElse("No ISR");

                System.out.printf("%-12d %-15s %-30s %-30s%n",
                        partitionInfo.partition(),
                        partitionInfo.leader().idString(),
                        replicas,
                        isr);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("\n\n");
    }

    private static void describeConsumer(Properties properties, String group) {
        try (AdminClient adminClient = AdminClient.create(properties)) {
            ListConsumerGroupOffsetsResult listOffsetsResult = adminClient.listConsumerGroupOffsets(group);
            Map<TopicPartition, OffsetAndMetadata> consumerOffsets = listOffsetsResult.partitionsToOffsetAndMetadata().get();

            Map<TopicPartition, OffsetSpec> requestPartitions = consumerOffsets.keySet().stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
            DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(Collections.singletonList(group));
            ConsumerGroupDescription groupDescription = describeResult.describedGroups().get(group).get();


            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> logEndOffsets = adminClient.listOffsets(requestPartitions).all().get();

            System.out.println("\n\nConsumer Group: " + group + " \t || \t Group Coordinator brokerID:" + groupDescription.coordinator().id());
            System.out.println("--------------------------------------------------------------------");
            System.out.printf("%-20s %-10s %-10s %-15s %-10s%n", "Topic", "Partition", "Offset", "Log End Offset", "Lag");
            System.out.println("--------------------------------------------------------------------");

            consumerOffsets.forEach((topicPartition, offsetAndMetadata) -> {
                long currentOffset = offsetAndMetadata.offset();
                long logEndOffset = logEndOffsets.get(topicPartition).offset();
                long lag = logEndOffset - currentOffset;

                System.out.printf("%-20s %-10d %-10d %-15d %-10d%n",
                        topicPartition.topic(),
                        topicPartition.partition(),
                        currentOffset,
                        logEndOffset,
                        lag);
            });
            System.out.println("\n");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }


    private static void describeCluster(Properties properties) {
        try (AdminClient adminClient = AdminClient.create(properties)) {
            DescribeClusterResult result = adminClient.describeCluster();
            KafkaFuture<Node> controllerFuture = result.controller();
            KafkaFuture<Collection<Node>> nodesFuture = result.nodes();

            try {
                Node controller = controllerFuture.get();
                Collection<Node> nodeList = nodesFuture.get();

                System.out.println("Cluster ID: " + result.clusterId().get());
                System.out.println("Nodes in the cluster:");
                System.out.println("Broker ID \t Advertised Listeners \t Is Controller");
                for (Node node : nodeList) {
                    boolean isController = node.id() == controller.id();
                    System.out.println(node.id() + " \t\t " + node.host() + ":" + node.port() + " \t\t " + isController);
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
