package org.example;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ProducerDemoWithCallback {

    public static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        logger.info("Producer Demo Started");
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:19092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("batch.size", "300");
        props.put("linger.ms", "300");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("my_new_topic", "Hello World " + i);
                producer.send(producerRecord, (recordMetadata, e) -> {
                    if (e == null) {
                        logger.info("consumer received message");
                        logger.info("topic: "+recordMetadata.topic());
                        logger.info("partition: "+recordMetadata.partition());
                        logger.info("offset: "+recordMetadata.offset());
                        logger.info("timestamp: "+recordMetadata.timestamp());
                    } else {
                        logger.error(e.getMessage());
                    }
                });
                try {
                    Thread.sleep(700);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        producer.close();
    }
}