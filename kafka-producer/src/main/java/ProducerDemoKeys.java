package com.joey.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);


        String bootstrapServers = "127.0.0.1:9092";

        //create Producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for( int i=0; i<5; i++) {
            //send data
            String key = "id_" + i;
            logger.info("key: " + key);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", key, "hello world " + i);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Recevied new metadata. \n" + "Topic: " + recordMetadata.topic() + "\n" +
                                "Paritition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("error while producing", e);
                    }
                }
            });
        }
        //flush data
        producer.flush();


    }

}
