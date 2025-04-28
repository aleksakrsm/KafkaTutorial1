package org.example;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConsumerDemoWithShutdown {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);

    public static void main(String[] args) {
        logger.info("I am Consumer Demo");
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:19092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", "my-java-application");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.offset.reset", "earliest");
//        props.put("auto.offset.reset", "latest");
//        props.put("auto.offset.reset", "none");

        props.put("group.instance.id", System.getenv("GROUP_INSTANCE_ID"));//static group membership

        var topic = "my_new_topic";
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        final var mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){ //called on ctrl + c or system shutdown.
            public void run() {
                logger.info("Shutting down");
                consumer.wakeup(); // consumerr wakes up from sleeping inside pool method
                try {
                    mainThread.join(); //waits for main to finish and then this shutdown thread finishes
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try{
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                logger.info("Polling");
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("key: " + record.key() + ", value: " + record.value() + ", partition: " + record.partition() + ", offset: " + record.offset());
                }
            }
        } catch(WakeupException e) {
            logger.info("Shutting down FROM WAKEUP");
        } catch (Exception e){
            logger.error("Error while shutting down", e);
        } finally {
            consumer.close();
            logger.info("Shutting down");
        }



    }
}