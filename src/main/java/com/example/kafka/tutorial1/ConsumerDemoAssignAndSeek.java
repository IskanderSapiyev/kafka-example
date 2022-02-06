package com.example.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerDemoAssignAndSeek {
    final static String bootstrapServers = "127.0.0.1:9092";
    final static String groupId = "my-fifth-group";
    final static String topic = "first_topic";


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class.getName());

        Properties properties = new Properties();
        //create consumer config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(List.of(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagingToRead = 5;
        int numberOfMessagesSoFar = 0;
        boolean keepOnReading = true;

        //poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesSoFar += 1;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition());
                logger.info("Offsets: " + record.offset());
                //reading 5 messages from  offsetToReadFrom and break the loop
                if (numberOfMessagesSoFar >= numberOfMessagingToRead) {
                    keepOnReading = false;
                    break;
                }
            }

        }


    }
}
