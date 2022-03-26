package za.co.simplitate.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKey {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithKey.class.getSimpleName());

    public static void main(String[] args) {
        LOG.info("I am a kafka producer");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i = 0; i < 50; i++) {

            String topic = "demo_java";
            String value = "hello world " + i;
            String key = "id" + i;

            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key,value);

            // send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) { // record sent successfully
                        LOG.info("\n");
                        LOG.info("topic: " + recordMetadata.topic() + "\n" +
                                "offset: " + recordMetadata.offset() + "\n" +
                                "partition: " + recordMetadata.partition() + "\n" +
                                "key: " + producerRecord.key() + "\n" +
                                "timestamp: " + recordMetadata.timestamp());
                        LOG.info("\n");
                    } else {
                        LOG.error("error sending record: ", e);
                    }
                }
            });
        }

        // flush and close producer
        producer.flush();
        producer.close();
    }
}
