import com.google.common.collect.Maps;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class kafkaServer {

    public static Map<String, Object> buildefaultclientconfig() {
        Map<String, Object> defaultClientConfig = Maps.newHashMap();
        defaultClientConfig.put("bootstrap.servers", "ksntech.kafka01.ksntech.com:9093");
        defaultClientConfig.put("client.id", "test-consumer-id");
        defaultClientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        defaultClientConfig.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "C:\\Users\\Admin\\Documents\\SSL\\kafka.server.keystore_01.jks");
        defaultClientConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "C:\\Users\\Admin\\Documents\\SSL\\kafka.server.truststore_01.jks");
        defaultClientConfig.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "ksntech");
        defaultClientConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "ksntech");
        defaultClientConfig.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "ksntech");
        defaultClientConfig.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,"");
//        defaultClientConfig.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
//        defaultClientConfig.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        return defaultClientConfig;
    }

    public static void main(String[] args) throws InterruptedException,ExecutionException {

//        Properties properties = new Properties();
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ksntech.kafka01.ksntech.com:9092");
//        AdminClient adminClient = AdminClient.create(properties);
//        CreateTopicsResult result = adminClient.createTopics(
//                Stream.of("foo", "bar", "baz").map(
//                        name -> new NewTopic(name, 3, (short) 1)
//                ).collect(Collectors.toList())
//        );
//        result.all().get();


        createTopic("javaTopic2", 3);

//        Map<String, List<PartitionInfo>> topics;
//        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(buildefaultclientconfig());
//        topics = consumer.listTopics();
//        consumer.close();

    }

    public static void createTopic(final String topicname, final int partitions) {
        final short replicationFactor = 1;

        try (final AdminClient adminClient = AdminClient.create(buildefaultclientconfig())) {
            try {
                final NewTopic newTopic = new NewTopic(topicname, partitions, replicationFactor);

                final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
                createTopicsResult.values().get(topicname).get();
            } catch (InterruptedException | ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        }
    }
}


