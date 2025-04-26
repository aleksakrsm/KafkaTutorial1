package org.example;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ProducerDemo {

    public static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        logger.info("Producer Demo Started");
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:19092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        Producer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("my_new_topic", "Hello World");
        producer.send(producerRecord);
        producer.flush();
        producer.close();
    }
}