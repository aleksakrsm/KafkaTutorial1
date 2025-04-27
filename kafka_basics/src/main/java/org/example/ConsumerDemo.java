package org.example;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConsumerDemo {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        logger.info("I am Consumer Demo");
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:19092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", "my-java-application");
        props.put("auto.offset.reset", "earliest");
//        props.put("auto.offset.reset", "latest");
//        props.put("auto.offset.reset", "none");

        var topic = "my_new_topic";
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            logger.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(500);
            for (ConsumerRecord<String, String> record : records) {
                logger.info("key: " + record.key() + ", value: " + record.value() + ", partition: " + record.partition() + ", offset: " + record.offset());
            }
        }

    }
}