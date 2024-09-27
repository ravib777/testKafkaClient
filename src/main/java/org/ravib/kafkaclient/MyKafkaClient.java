package org.ravib.kafkaclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.Node;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.IOException;
import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


public class MyKafkaClient {

    static String operation;
    static String bootstrapServer = "localhost:9092";
    static String topic;
    static String configFile;
    static Integer numMessages = 1;
    static String group;
    static String format = "string";
    static boolean sendKeys = false;
    static String schema = null;
    static Integer schemaId = null;

    public static void main(String[] args) {

        if (args.length < 1) {
            System.out.println("Usage: java -jar target/testKafkaClient-1.0-SNAPSHOT-jar-with-dependencies.jar --operation <operation> [other options]\n" +
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
                case "bootstrap-server":
                    bootStrapPreset = true;
                    bootstrapServer = entry.getValue();
                    break;
                case "send-keys":
                    sendKeys = Boolean.parseBoolean(entry.getValue());
                    break;
                case "format":
                    format = entry.getValue();
                    break;
                case "topic":
                    topic = entry.getValue();
                    break;
                case "group":
                    group = entry.getValue();
                    break;
                case "num-messages":
                    numMessages = Integer.parseInt(entry.getValue());
                    break;
                case "schema":
                    schema = entry.getValue();
                    break;
                case "schema-id":
                    schemaId = Integer.parseInt(entry.getValue());
                    break;
                case "config-file":
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
            System.out.println("--bootstrap-server <broker:port> was not set, " +
                    "default localhost:9092 is being used");
        }
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");


        if (!(format.equals("string") || format.equals("avro") || format.equals("protobuf")
                || format.equals("jsonsr"))) {
            System.out.println("ERROR: --format only accepts one of \"string\" (default), \"avro\", " +
                    "\"protobuf\" or \"jsonsr\". Setting --format " + format + " is an incorrect value");
            System.exit(0);
        }

        if(operation==null){
            System.out.println("--operation option must be provided. Use produce, consume, describe-topic, describe-cluster, describe-group, list-topics or list-consumers.");
            System.exit(0);
        }


        switch (operation) {
            case "produce":
                if (topic != null) {
                    if (format.equals("string")) {
                        produceStringMessages(properties, topic, numMessages, sendKeys);
                    }
                    if (format.equals("avro") && (schema != null || schemaId != null)) {
                        produceAvroMessages(properties, topic, numMessages, sendKeys, schema, schemaId);
                    } else if (format.equals("avro")) {
                        System.out.println("With Avro format, option --schema needs to be set");
                    }
                    if (format.equals("protobuf") && (schema != null || schemaId != null)) {
                        produceProtobufMessages(properties, topic, numMessages, sendKeys, schema, schemaId);
                    } else if (format.equals("protobuf")) {
                        System.out.println("With Protobuf format, option --schema needs to be set");
                    }
                    if (format.equals("jsonsr") && (schema != null || schemaId != null)) {
                        produceJsonMessages(properties, topic, numMessages, sendKeys, schema, schemaId);
                    } else if (format.equals("jsonsr")) {
                        System.out.println("With JsonSR format, option --schema needs to be set");
                    }
                } else {
                    System.out.println("consume option requires --topic <topicName> parameter to be set");
                }

                break;
            case "consume":
                if (topic != null) {
                    if (format.equals("string")) {
                        consumeMessages(properties, topic, group, numMessages);
                    } else if (format.equals("avro")) {
                        consumeAvroMessages(properties, topic, group, numMessages);
                    } else if (format.equals("protobuf")) {
                        consumeProtoMessages(properties, topic, group, numMessages);
                    } else if (format.equals("jsonsr")) {
                        consumeJsonMessages(properties, topic, group, numMessages);
                    } else {
                        System.out.println("consume option --format set to " + format + ". " +
                                "But it only supports \"string\" (default), \"avo\", \"protobuf\" and \"jsonsr\"");
                    }

                } else {
                    System.out.println("consume option requires --topic <topicName> parameter to be set");
                }
                break;
            case "describe-topic":
                if (topic != null) {
                    describeTopic(properties, topic);
                } else {
                    System.out.println("describe-topic option requires --topic <topicName> parameter to be set");
                }
                break;
            case "describe-group":
                if (group != null) {
                    describeConsumer(properties, group);
                } else {
                    System.out.println("describe-group option requires --group <groupName> parameter to be set");
                }
                break;
            case "describe-cluster":
                describeCluster(properties);
                break;
            case "list-topics":
                listTopics(properties);
                break;
            case "list-consumers":
                listConsumers(properties);
                break;
            default:
                System.out.println("Unsupported operation. Use produce, consume, describe-topic, describe-cluster, describe-group, list-topics or list-consumers.");
                System.exit(1);
        }
    }

    private static void produceStringMessages(Properties properties, String topic, Integer numMessages,
                                              boolean sendKeys) {
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Scanner scanner = new Scanner(System.in);
        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            int counter = 0;
            if (sendKeys) {
                while (counter < numMessages) {
                    System.out.println("Enter message " + (counter + 1) + " in <key>|value format as " +
                            "--send-keys true was set. Type exit to exit anytime: ");
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

    private static void produceAvroMessages(Properties properties, String topic, Integer numMessages,
                                            boolean sendKeys, String stringSchema, Integer schemaId) {
        boolean containsSR = properties.keySet().stream()
                .anyMatch(key -> key.toString().contains("schema.registry.url"));
        if (!containsSR) {
            System.out.println(properties.toString());
            System.out.println("Properties file does not include schema.registry.url. " +
                    "Please set it and other SR security configs in the properties file and re-run the command");
            System.exit(1);
        }
        String schemaUrl = properties.getProperty("schema.registry.url");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        Scanner scanner = new Scanner(System.in);
        GenericRecord avroRecord = null;
        ParsedSchema parsedSchema;
        Map<String, String> map = new HashMap<>();

        Schema schema = null;
        if (schemaId != null && stringSchema != null) {
            System.out.println("Both --schema and --schema-id set, set only one");
            return;
        } else if (schemaId != null) {
            for (String key : properties.stringPropertyNames()) {
                map.put(key, properties.getProperty(key));
            }
            SchemaRegistryClient client = new CachedSchemaRegistryClient(schemaUrl, 5, map);
            try {
                parsedSchema = client.getSchemaById(schemaId);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (RestClientException e) {
                throw new RuntimeException(e);
            }
            schema = (Schema) parsedSchema.rawSchema();
        } else {
            schema = new Schema.Parser().parse(stringSchema);
        }
        try (Producer<String, GenericRecord> producer = new KafkaProducer<>(properties)) {
            int counter = 0;
            if (sendKeys) {
                while (counter < numMessages) {
                    System.out.println("Enter message " + (counter + 1) + " in <key>|value format as " +
                            "--send-keys true was set. Type exit to exit anytime: ");
                    String message = scanner.nextLine();
                    if ("exit".equalsIgnoreCase(message.trim())) {
                        break;
                    }
                    String[] keyValue = message.split("\\|", 2);
                    if (keyValue.length == 2) {
                        avroRecord = parseAvroMessage(schema, keyValue[1]);
                        producer.send(new ProducerRecord(topic, keyValue[0], avroRecord));
                        System.out.println("Message sent: " + message);
                    } else {
                        System.out.println("Invalid input. Please use the format 'key|value'.");
                        continue;
                    }
                    counter++;
                }
            } else {
                while (counter < numMessages) {
                    System.out.println("Enter message " + (counter + 1) + ". Type exit to exit anytime:: ");
                    String message = scanner.nextLine();
                    if ("exit".equalsIgnoreCase(message.trim())) {
                        break;
                    }
                    try {
                        avroRecord = parseAvroMessage(schema, message);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    producer.send(new ProducerRecord<String, GenericRecord>(topic, null, avroRecord));
                    counter++;
                }
            }
            producer.close();
            System.out.println("Finished sending " + numMessages + " messages to Kafka.");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static GenericRecord parseAvroMessage(Schema schema, String messageJson) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(messageJson);
        GenericRecord record;
        AvroSchema avroSchema = new AvroSchema(schema);
        record = (GenericRecord) AvroSchemaUtils.toObject(node, avroSchema);
        return record;
    }

    private static void produceProtobufMessages(Properties properties, String topic, Integer numMessages,
                                                boolean sendKeys, String stringSchema, Integer schemaId) {
        boolean containsSR = properties.keySet().stream()
                .anyMatch(key -> key.toString().contains("schema.registry.url"));
        if (!containsSR) {
            System.out.println(properties.toString());
            System.out.println("Properties file does not include schema.registry.url. " +
                    "Please set it and other SR security configs in the properties file and re-run the command");
            System.exit(1);
        }
        String schemaUrl = properties.getProperty("schema.registry.url");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        Scanner scanner = new Scanner(System.in);
        DynamicMessage protoMessage = null;
        ParsedSchema parsedSchema;
        Map<String, String> map = new HashMap<>();
        ProtobufSchema schema = null;
        if (schemaId != null && stringSchema != null) {
            System.out.println("Both --schema and --schema-id set, set only one");
            return;
        } else if (schemaId != null) {
            for (String key : properties.stringPropertyNames()) {
                map.put(key, properties.getProperty(key));
            }
            List<SchemaProvider> schemaProviders = new ArrayList<>();
            SchemaProvider schemaProvider = new ProtobufSchemaProvider();
            schemaProviders.add(schemaProvider);
            SchemaRegistryClient client = new CachedSchemaRegistryClient(schemaUrl, 5, schemaProviders, map);
            try {
                parsedSchema = client.getSchemaById(schemaId);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (RestClientException e) {
                throw new RuntimeException(e);
            }
            schema = (ProtobufSchema) parsedSchema;
        } else {
            schema = new ProtobufSchema(stringSchema);
        }
        try (Producer<String, DynamicMessage> producer = new KafkaProducer<>(properties)) {
            int counter = 0;
            if (sendKeys) {
                while (counter < numMessages) {
                    System.out.println("Enter message " + (counter + 1) + " in <key>|value format as " +
                            "--send-keys true was set. Type exit to exit anytime: ");
                    String message = scanner.nextLine();
                    if ("exit".equalsIgnoreCase(message.trim())) {
                        break;
                    }
                    String[] keyValue = message.split("\\|", 2);
                    if (keyValue.length == 2) {
                        protoMessage = parseProtoMessage(schema, keyValue[1]);
                        producer.send(new ProducerRecord(topic, keyValue[0], protoMessage));
                        System.out.println("Message sent: " + message);
                    } else {
                        System.out.println("Invalid input. Please use the format 'key|value'.");
                        continue;
                    }
                    counter++;
                }
            } else {
                while (counter < numMessages) {
                    System.out.println("Enter message " + (counter + 1) + ". Type exit to exit anytime:: ");
                    String message = scanner.nextLine();
                    if ("exit".equalsIgnoreCase(message.trim())) {
                        break;
                    }
                    try {
                        protoMessage = parseProtoMessage(schema, message);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    producer.send(new ProducerRecord<String, DynamicMessage>(topic, null, protoMessage));
                    counter++;
                }
            }
            producer.close();
            System.out.println("Finished sending " + numMessages + " messages to Kafka.");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static DynamicMessage parseProtoMessage(ProtobufSchema schema,
                                                    String messageJson) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(messageJson);
        DynamicMessage record;
        record = (DynamicMessage) ProtobufSchemaUtils.toObject(node, schema);
        return record;
    }

    private static void produceJsonMessages(Properties properties, String topic, Integer numMessages,
                                            boolean sendKeys, String stringSchema, Integer schemaId) {
        boolean containsSR = properties.keySet().stream()
                .anyMatch(key -> key.toString().contains("schema.registry.url"));
        if (!containsSR) {
            System.out.println(properties.toString());
            System.out.println("Properties file does not include schema.registry.url. " +
                    "Please set it and other SR security configs in the properties file and re-run the command");
            System.exit(1);
        }
        String schemaUrl = properties.getProperty("schema.registry.url");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
        Scanner scanner = new Scanner(System.in);
        JsonNode jsonrecord = null;
        ParsedSchema parsedSchema;
        Map<String, String> map = new HashMap<>();
        JsonSchema schema = null;
        if (schemaId != null && stringSchema != null) {
            System.out.println("Both --schema and --schema-id set, set only one");
            return;
        } else if (schemaId != null) {
            for (String key : properties.stringPropertyNames()) {
                map.put(key, properties.getProperty(key));
            }
            List<SchemaProvider> schemaProviders = new ArrayList<>();
            SchemaProvider schemaProvider = new JsonSchemaProvider();
            schemaProviders.add(schemaProvider);
            SchemaRegistryClient client = new CachedSchemaRegistryClient(schemaUrl, 5, schemaProviders, map);
            try {
                parsedSchema = (ParsedSchema) client.getSchemaById(schemaId);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (RestClientException e) {
                throw new RuntimeException(e);
            }
            schema = new JsonSchema(parsedSchema.toString());
        } else {
            schema = new JsonSchema(stringSchema);
        }
        try (Producer<String, JsonNode> producer = new KafkaProducer<>(properties)) {
            int counter = 0;
            if (sendKeys) {
                while (counter < numMessages) {
                    System.out.println("Enter message " + (counter + 1) + " in <key>|value format as --send-keys true was set. " +
                            "Type exit to exit anytime: ");
                    String message = scanner.nextLine();
                    if ("exit".equalsIgnoreCase(message.trim())) {
                        break;
                    }
                    String[] keyValue = message.split("\\|", 2);
                    if (keyValue.length == 2) {

                        jsonrecord = parseJsonMessage(schema, keyValue[1]);
                        producer.send(new ProducerRecord(topic, keyValue[0], jsonrecord));
                        System.out.println("Message sent: " + message);
                    } else {
                        System.out.println("Invalid input. Please use the format 'key|value'.");
                        continue;
                    }
                    counter++;
                }
            } else {
                while (counter < numMessages) {
                    System.out.println("Enter message " + (counter + 1) + ". Type exit to exit anytime:: ");
                    String message = scanner.nextLine();
                    if ("exit".equalsIgnoreCase(message.trim())) {
                        break;
                    }
                    try {

                        jsonrecord = parseJsonMessage(schema, message);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    producer.send(new ProducerRecord<String, JsonNode>(topic, null, jsonrecord));
                    counter++;
                }
            }
            producer.close();
            System.out.println("Finished sending " + numMessages + " messages to Kafka.");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static JsonNode parseJsonMessage(JsonSchema schema, String messageJson) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(messageJson);
        JsonNode record;
        record = (JsonNode) JsonSchemaUtils.toObject(node, schema);
        return record;
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
                consumer.commitAsync();
            }
        }
    }

    private static void consumeAvroMessages(Properties properties, String topic, String group, Integer numMessages) {
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        boolean containsSR = properties.keySet().stream()
                .anyMatch(key -> key.toString().contains("schema.registry.url"));
        if (!containsSR) {
            System.out.println(properties.toString());
            System.out.println("Properties file does not include schema.registry.url. " +
                    "Please set it and other SR security configs in the properties file and re-run the command");
            System.exit(1);
        }
        if (group != null) {
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        } else {
            System.out.println(" --group <groupName> option missing while consuming the message. Exiting now");
            System.exit(1);
        }
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        try (Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            int counter = 0;
            while (counter < numMessages) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.println("Consumed record with key : " + record.key() + "  and value: " + record.value());
                    counter++;
                }
                consumer.commitAsync();
            }
        }
    }

    private static void consumeProtoMessages(Properties properties, String topic, String group, Integer numMessages) {
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class.getName());

        boolean containsSR = properties.keySet().stream()
                .anyMatch(key -> key.toString().contains("schema.registry.url"));
        if (!containsSR) {
            System.out.println(properties.toString());
            System.out.println("Properties file does not include schema.registry.url. " +
                    "Please set it and other SR security configs in the properties file and re-run the command");
            System.exit(1);
        }
        if (group != null) {
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        } else {
            System.out.println(" --group <groupName> option missing while consuming the message. Exiting now");
            System.exit(1);
        }
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<String, DynamicMessage> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            int counter = 0;
            while (counter < numMessages) {
                ConsumerRecords<String, DynamicMessage> records = consumer.poll(100);
                for (ConsumerRecord<String, DynamicMessage> record : records) {
                    System.out.println("Consumed record with key : " + record.key() + "  and value: " + record.value());
                    counter++;
                }
                consumer.commitAsync();
            }
        }
    }


    private static void consumeJsonMessages(Properties properties, String topic, String group, Integer numMessages) {
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());

        boolean containsSR = properties.keySet().stream()
                .anyMatch(key -> key.toString().contains("schema.registry.url"));
        if (!containsSR) {
            System.out.println(properties.toString());
            System.out.println("Properties file does not include schema.registry.url. " +
                    "Please set it and other SR security configs in the properties file and re-run the command");
            System.exit(1);
        }
        if (group != null) {
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        } else {
            System.out.println(" --group <groupName> option missing while consuming the message. Exiting now");
            System.exit(1);
        }
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (Consumer<String, JsonNode> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            int counter = 0;
            while (counter < numMessages) {
                ConsumerRecords<String, JsonNode> records = consumer.poll(100);
                for (ConsumerRecord<String, JsonNode> record : records) {
                    System.out.println("Consumed record with key : " + record.key() + "  and value: " + record.value());
                    counter++;
                }
                consumer.commitAsync();
            }
        }
    }

    private static void describeTopic(Properties properties, String topic) {
        try (AdminClient adminClient = AdminClient.create(properties)) {
            ArrayList<String> topicList = new ArrayList();
            topicList.add(topic);
            DescribeTopicsResult result = adminClient.describeTopics(topicList);
            TopicDescription topicDescription = result.values().get(topic).get();
            System.out.println("\n\nTopic: " + topic);
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singletonList(topicResource));
            describeConfigsResult.all().get().forEach((configResource, config_) -> {
                System.out.print("Topic Configs: ");
                config_.entries().forEach(configEntry -> {
                    System.out.print(configEntry.name() + " = " + configEntry.value()+ " , ");
                });
            });

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
            String firstBrokerId = adminClient.describeCluster(new DescribeClusterOptions()).nodes().get().stream()
                    .findFirst()
                    .map(node -> String.valueOf(node.id()))
                    .orElseThrow(() -> new RuntimeException("No brokers found in the cluster"));


            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, firstBrokerId);
            DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singletonList(brokerResource));

            describeConfigsResult.all().get().forEach((configResource, config) -> {
                System.out.println("Broker Configs for one of the brokers in the cluster (brokerId#" + configResource.name()+") :");
                config.entries().forEach(configEntry ->
                        System.out.print(configEntry.name() + " = " + configEntry.value() + ","));
            });
            DescribeClusterResult result = adminClient.describeCluster();
            KafkaFuture<Node> controllerFuture = result.controller();
            KafkaFuture<Collection<Node>> nodesFuture = result.nodes();

            try {
                Node controller = controllerFuture.get();
                Collection<Node> nodeList = nodesFuture.get();
                System.out.println("Cluster ID: " + result.clusterId().get());
                System.out.println("Nodes in the cluster:");
                System.out.println("Broker ID \t Advertised Listeners \t Is Controller \t Rack");
                for (Node node : nodeList) {
                    boolean isController = node.id() == controller.id();
                    System.out.println(node.id() + " \t\t " + node.host() + ":" + node.port() + " \t\t " + isController + " \t\t " + node.rack());
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    private static void listTopics(Properties properties) {
        try (AdminClient adminClient = AdminClient.create(properties)) {
            System.out.println("Listing Kafka topics:");
            ListTopicsResult listTopicsResult = adminClient.listTopics(new ListTopicsOptions());
            List<String> topics = new ArrayList<>(listTopicsResult.names().get());
            Collections.sort(topics, String.CASE_INSENSITIVE_ORDER);
            for (String topic : topics) {
                System.out.println(topic);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void listConsumers(Properties properties) {
        try (AdminClient admin = AdminClient.create(properties)) {
            ListConsumerGroupsResult groupsResult = admin.listConsumerGroups();

            List<String> consumerNames = new ArrayList<>();
            for (ConsumerGroupListing groupListing : groupsResult.all().get()) {
                consumerNames.add(groupListing.groupId());
            }
            Collections.sort(consumerNames, String.CASE_INSENSITIVE_ORDER);
            for (String name : consumerNames) {
                System.out.println(name);
            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
