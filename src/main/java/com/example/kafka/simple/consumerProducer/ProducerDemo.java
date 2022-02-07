package com.example.kafka.simple.consumerProducer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    final static String bootstrapServers = "127.0.0.1:9092";

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //writing 10 messages to topic
        for (int i = 0; i < 30; i++) {
            //create producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("first_topic", Integer.toString(i%3),"messages " + i);
            //sends data asynchronous
            producer.send(record, (recordMetadata, e) -> {
                //executes on every success execution or throw an Error
                if (e == null) {
                    //the record was successfully sent
                    logger.info("receive new meta data \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "TimeStamp: " + recordMetadata.timestamp()
                    );
                } else {
                    //An error was occurred
                    logger.error("Error while producing " + e);
                }

            });
        }
        producer.flush();
        producer.close();
    }
}
