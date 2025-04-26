package org.example;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ProducerDemoWithKeys {

    public static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) {
        logger.info("Producer Demo Started");
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:19092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 10; i++) {
                var topic = "my_new_topic";
                var key = "id" + i;
                var value = "Hello " + i;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                producer.send(producerRecord, (recordMetadata, e) -> {
                    if (e == null) {
                        logger.info("-----------------------consumer received message -----------------------");
//                        logger.info("topic: "+recordMetadata.topic());
                        logger.info("partition: "+recordMetadata.partition());
                        logger.info("key: "+producerRecord.key());
                        logger.info("value: "+producerRecord.value());
                        logger.info("offset: "+recordMetadata.offset());
                        logger.info("timestamp: "+recordMetadata.timestamp());
                    } else {
                        logger.error(e.getMessage());
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        producer.close();
    }
}