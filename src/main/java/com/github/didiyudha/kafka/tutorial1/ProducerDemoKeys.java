package com.github.didiyudha.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // Create producer properties.
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        String acks = "all";
        String topic = "first_topic";

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, acks);

        // Create the producer.
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        String prefixValue = "Hello Kafka Message #";
        String prefixKey = "message_id_";

        for (int i = 0; i < 10; i++) {

            // Build message.
            String msg = prefixValue + Integer.toString(i+1);

            // Build producer key.
            String key = prefixKey + Integer.toString(i+1);

            // Some notes, for next run to check whether key is going to the same partition or not. Make sure what we
            // are doing is exactly the same with what theory said. These information are from log.
            // Producer Key: message_id_1 -> Partition: 1
            // Producer Key: message_id_2 -> Partition: 1
            // Producer Key: message_id_3 -> Partition: 0
            // Producer Key: message_id_4 -> Partition: 2
            // Producer Key: message_id_5 -> Partition: 1
            // Producer Key: message_id_6 -> Partition: 2
            // Producer Key: message_id_7 -> Partition: 1
            // Producer Key: message_id_8 -> Partition: 1
            // Producer Key: message_id_9 -> Partition: 1
            // Producer Key: message_id_10 -> Partition: 0

            // Log the key.
            logger.info("Producer Key: "+key);

            // Create producer record
            final ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, msg);
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
            }).get(); // block the .send() to make it synchronous - don't do this in production.
        }

        producer.flush();
        producer.close();
    }
}
