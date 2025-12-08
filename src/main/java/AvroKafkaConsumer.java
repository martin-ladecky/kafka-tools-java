import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class AvroKafkaConsumer {

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("AvroKafkaConsumer").build()
                .defaultHelp(true)
                .description("A Kafka CLI Consumer with Avro, SSL, and JSON support.");

        parser.addArgument("--bootstrap-server")
                .help("The server to connect to. Overrides config file.");

        parser.addArgument("--consumer-config")
                .help("Path to a properties file containing consumer configuration (SSL, Schema Registry, etc).");

        parser.addArgument("--topic")
                .required(true)
                .help("The topic to consume from");

        parser.addArgument("--group")
                .help("The consumer group ID. Defaults to a random UUID.");

        parser.addArgument("--from-beginning")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("Start from earliest offset.");

        parser.addArgument("--property")
                .nargs("*")
                .help("Custom properties. Overrides config file values.");

        parser.addArgument("--output")
                .choices("json", "raw")
                .setDefault("raw")
                .help("Output format. 'json' wraps metadata and value in a JSON object.");

        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        // 1. Initialize Properties
        Properties props = new Properties();

        // 2. Load from File
        if (ns.getString("consumer_config") != null) {
            try (FileInputStream fis = new FileInputStream(ns.getString("consumer_config"))) {
                props.load(fis);
            } catch (IOException e) {
                System.err.println("Error reading config file: " + e.getMessage());
                System.exit(1);
            }
        }

        // 3. Apply CLI Overrides
        if (ns.getString("bootstrap_server") != null) {
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ns.getString("bootstrap_server"));
        }

        if (!props.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            System.err.println("Error: --bootstrap-server must be provided via CLI or config file.");
            System.exit(1);
        }

        String groupId = ns.getString("group");
        if (groupId != null) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        } else if (!props.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "console-consumer-" + UUID.randomUUID().toString());
        }

        if (ns.getBoolean("from_beginning")) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        } else if (!props.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        }

        if (ns.getList("property") != null) {
            for (Object obj : ns.getList("property")) {
                String prop = (String) obj;
                String[] parts = prop.split("=", 2);
                if (parts.length == 2) {
                    props.put(parts[0], parts[1]);
                }
            }
        }

        // 4. Feature Detection (SSL & Avro)
        boolean sslEnabled = false;
        for (Object key : props.keySet()) {
            if (key.toString().startsWith("ssl.")) {
                sslEnabled = true;
                break;
            }
        }
        if (sslEnabled && !props.containsKey("security.protocol")) {
            props.put("security.protocol", "SSL");
        }

        String schemaRegistryUrl = props.getProperty("schema.registry.url");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        if (schemaRegistryUrl != null) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
            props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
            props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false");
        } else {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }

        // 5. Consumption Loop
        boolean isJsonOutput = "json".equals(ns.getString("output"));
        ObjectMapper mapper = new ObjectMapper();

        try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(ns.getString("topic")));

            // Suppress standard log output for cleaner CLI usage
            System.setProperty(org.slf4j.simple.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "WARN");

            while (true) {
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Object> record : records) {
                    if (isJsonOutput) {
                        try {
                            ObjectNode root = mapper.createObjectNode();
                            root.put("topic", record.topic());
                            root.put("partition", record.partition());
                            root.put("offset", record.offset());
                            root.put("timestamp", record.timestamp());

                            if (record.key() != null) root.put("key", record.key());

                            if (record.value() instanceof GenericRecord) {
                                JsonNode valueNode = mapper.readTree(record.value().toString());
                                root.set("value", valueNode);
                            } else if (record.value() != null) {
                                root.put("value", record.value().toString());
                            }
                            System.out.println(mapper.writeValueAsString(root));
                        } catch (Exception e) {
                            System.err.println("Error formatting JSON: " + e.getMessage());
                        }
                    } else {
                        System.out.println(record.value());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}