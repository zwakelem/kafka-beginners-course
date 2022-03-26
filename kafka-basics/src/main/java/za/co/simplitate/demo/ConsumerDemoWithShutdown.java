package za.co.simplitate.demo;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        LOG.info("I am a kafka Consumer");

        String bootstrapServer = "localhost:9092";
        String topic = "demo_java";
        String groupId = "my-consumer_app";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList(topic));

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOG.info("Detected a shutdown");
                consumer.wakeup();

                // join main thread
                try {
                     Thread.currentThread().join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            while(true) {
                LOG.info("Polling ....");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record: records) {
                    LOG.info("Key: " + record.key() + ", Value: " + record.value());
                    LOG.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch(WakeupException e) {
            // this is expected
            LOG.info("wake up exception");
        } catch(Exception e) {
            LOG.error("Unexpected exception");
        } finally {
            consumer.close(); // this will commit offsets
            LOG.info("the consumer is gracefully closed");
        }
    }
}
