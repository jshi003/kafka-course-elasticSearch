package com.joey.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ComsumerDemoAssignSeek {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ComsumerDemoAssignSeek.class);


        //creat consumer config
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are mostly used to replay data or fetch specific message

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        long offsetToReadFrom = 15L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessageToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessageReadSoFar = 0;


        //subscribe consumer to topic
        consumer.subscribe(Collections.singleton(topic));

        //poll for new data
        while(keepOnReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records){
                numberOfMessageReadSoFar +=1;
                logger.info("key: " +record.key() +", Value: "+record.value());
            }
            if(numberOfMessageReadSoFar>=numberOfMessageToRead){
                keepOnReading =false;
                break;
            }
        }

        logger.info("exit the application");
    }


}
