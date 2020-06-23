package com.joey.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoGroupThread {

    public static void main(String[] args) {

        new ConsumerDemoGroupThread().run();


    }

    private ConsumerDemoGroupThread(){}

    private void run(){

        final Logger logger = LoggerFactory.getLogger(ConsumerDemoGroupThread.class);
        //creat consumer config
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-second-application";
        String topic = "first_topic";

        //latch for dealing with multiple threads //advance to synchronization
        CountDownLatch latch = new CountDownLatch(1);

        //create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                latch, bootstrapServers,groupId,topic
        );

        //start thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();


        //add a shutdown hook.
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Caught Shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            }catch (InterruptedException ie){
                logger.error("application is interripted", ie);
            }finally {
                logger.info("application is closing");
            }
        }));


        try{
            latch.await();
        }catch (InterruptedException ie){
            logger.error("application is Interrupted", ie);
        }finally {
            logger.info("application is closing");
        }



    }



    public class ConsumerRunnable implements  Runnable{
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoGroupThread.class);
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private String bootstrapServers;
        private String groupId;
        private String topic;

        public ConsumerRunnable(CountDownLatch latch, String bootstrapServers, String groupId, String topic){
            this.latch = latch;
            this.bootstrapServers = bootstrapServers;
            this.groupId = groupId;
            this.topic = topic;

            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //create consumer
            this.consumer = new KafkaConsumer<String, String>(properties);
            //subscribe consumer to topic
            consumer.subscribe(Collections.singleton(topic));

        }

        //poll for new data
        @Override
        public void run() {
            try{
                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String, String> record : records){
                        logger.info("key: " +record.key() +", Value: "+record.value());
                        logger.info("partition: "+ record.partition() + ", offset: "+record.offset());
                    }

                }
            }catch (WakeupException we){
                logger.info("Received shutdown signal!");
            }finally{
                consumer.close();
                latch.countDown();
            }

        }


        public void shutdown(){
            //wakeup method is to interrupt consumer.poll()
            //it will throw WakeUpException
            consumer.wakeup();
        }




    }

}
