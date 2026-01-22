import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Iterator;
import java.util.Map;

public class AvroKafkaProducer {

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("AvroKafkaProducer").build()
                .defaultHelp(true)
                .description("A Kafka CLI Producer with Avro and SSL support.");

        parser.addArgument("--bootstrap-server")
                .help("The server to connect to. Overrides config file.");

        parser.addArgument("--producer-config")
                .help("Path to a properties file containing producer configuration (SSL, Schema Registry, etc).");

        parser.addArgument("--topic")
                .required(true)
                .help("The topic to produce to");

        parser.addArgument("--value-schema")
                .help("Path to the Avro schema file for the value. Required if using Avro.");

        parser.addArgument("--property")
                .nargs("*")
                .help("Custom properties. Overrides config file values.");

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
        if (ns.getString("producer_config") != null) {
            try (FileInputStream fis = new FileInputStream(ns.getString("producer_config"))) {
                props.load(fis);
            } catch (IOException e) {
                System.err.println("Error reading config file: " + e.getMessage());
                System.exit(1);
            }
        }

        // 3. Apply CLI Overrides
        if (ns.getString("bootstrap_server") != null) {
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ns.getString("bootstrap_server"));
        }

        if (!props.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            System.err.println("Error: --bootstrap-server must be provided via CLI or config file.");
            System.exit(1);
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
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Schema schema = null;
        if (schemaRegistryUrl != null) {
            if (ns.getString("value_schema") == null) {
                System.err.println("Error: --value-schema is required when Schema Registry is configured.");
                System.exit(1);
            }
            try {
                String schemaContent = new String(Files.readAllBytes(Paths.get(ns.getString("value_schema"))));
                schema = new Schema.Parser().parse(schemaContent);
            } catch (IOException e) {
                System.err.println("Error reading schema file: " + e.getMessage());
                System.exit(1);
            }

            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
            props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        } else {
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }

        // 5. Production Loop
        ObjectMapper mapper = new ObjectMapper();
        System.out.println("Enter messages (JSON for Avro, text for String). Press Ctrl+C to exit.");

        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(props);
             BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                Object value;
                if (schema != null) {
                    try {
                        value = jsonToAvro(mapper.readTree(line), schema);
                    } catch (Exception e) {
                        System.err.println("Invalid JSON for Avro schema: " + e.getMessage());
                        continue;
                    }
                } else {
                    value = line;
                }

                ProducerRecord<String, Object> record = new ProducerRecord<>(ns.getString("topic"), value);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Error producing message: " + exception.getMessage());
                    } else {
                        System.out.println("Produced to " + metadata.topic() + "-" + metadata.partition() + "@" + metadata.offset());
                    }
                });
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Object jsonToAvro(JsonNode json, Schema schema) {
        if (json.isNull()) return null;

        switch (schema.getType()) {
            case RECORD:
                GenericRecord record = new GenericData.Record(schema);
                for (Schema.Field field : schema.getFields()) {
                    JsonNode fieldJson = json.get(field.name());
                    if (fieldJson == null) {
                        if (field.defaultVal() != null) {
                            // This is a simplification. Avro defaults are complex to handle manually.
                            // Ideally, use a library like avro-json-decoder if available, but for now we skip or error.
                            // For this simple CLI, we'll assume the JSON must match.
                            continue; 
                        }
                        continue;
                    }
                    record.put(field.name(), jsonToAvro(fieldJson, field.schema()));
                }
                return record;
            case ARRAY:
                GenericData.Array<Object> array = new GenericData.Array<>(json.size(), schema);
                for (JsonNode element : json) {
                    array.add(jsonToAvro(element, schema.getElementType()));
                }
                return array;
            case STRING:
                return json.asText();
            case INT:
                return json.asInt();
            case LONG:
                return json.asLong();
            case FLOAT:
                return (float) json.asDouble();
            case DOUBLE:
                return json.asDouble();
            case BOOLEAN:
                return json.asBoolean();
            case UNION:
                // Simple union handling: try the first non-null type that matches
                for (Schema subSchema : schema.getTypes()) {
                    if (subSchema.getType() == Schema.Type.NULL) continue;
                    try {
                        return jsonToAvro(json, subSchema);
                    } catch (Exception ignored) {}
                }
                return null;
            default:
                return json.asText();
        }
    }
}