package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // Create producer properties.
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        String acks = "all";

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, acks);

        // Create the producer.
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);



        for (int i = 0; i < 10; i++) {
            // Create producer record
            final ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", "Hello Kafka message #"+Integer.toString(i+1));
            // Send data.
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Executes every time a record is successfully sent or an exception is thrown.
                    if (e == null) {
                        // the record was successfully sent.
                        logger.info("Receive new metadata. \n" +
                                "Topic: " + recordMetadata.topic()+"\n" +
                                "Partition: "+ recordMetadata.partition()+"\n" +
                                "Offset: "+ recordMetadata.offset()+"\n" +
                                "Timestamp: "+recordMetadata.timestamp()+"\n");

                    } else {
                        logger.error("Error while producing message to Kafka: ", e);
                    }
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
