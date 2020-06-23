package com.joey.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public  static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "127.0.0.1:9092";

        //create Producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //send data
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e ==null){
                    logger.info("Recevied new metadata. \n" + "Topic: " + recordMetadata.topic() +"\n"+
                            "Paritition: " + recordMetadata.partition() +"\n"+
                            "Offset: " + recordMetadata.offset() +"\n"+
                            "Timestamp: " + recordMetadata.timestamp());
                }else{
                    logger.error("error while producing", e);
                }
            }
        });

        //flush data
        producer.flush();


    }

}
