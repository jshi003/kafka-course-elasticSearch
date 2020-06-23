package com.joey.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {


    final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    List<String> terms = Lists.newArrayList("Kafka");
    public TwitterProducer(){

    }


    public static void main(String[] args) {

        new TwitterProducer().run();

    }

    public void run(){
        //create a twitter client
/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client client = this.createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();
        //create a kafka producer
        KafkaProducer<String, String> producer = this.createKafkaProducer();
        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("stopping application ");
            client.stop();
            //IMPORTANT always close the producer when the application is shutdown.
            producer.close();
        }));

        //loop to send tweets to kafka
        while (!client.isDone()) {
            String msg =null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);

            }catch (InterruptedException ite){
                ite.printStackTrace();
                client.stop();
            }
            if(msg!=null){
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e !=null){
                            logger.error("error while producing", e);
                        }
                    }
                });
            }
        }

        logger.info("end of application");

    }


    String consumerKey = "6RiJuca6sAZLnfGQDJpOQcK1i";
    String consumerSecret = "GFp5QtXvKM1NtnDZLnteKydfn5e28NV7hGtDCzt5y4jGsFmbjm";
    String token = "803785076563591168-KL7aHZq0PSvMsMwv4FgzaECIF7HLU7X";
    String secret = "Bw5CL7dqvIk9VgMHBUC8fALLDGp0yG25O2NhEOcncG6QJ";

    public Client createTwitterClient(BlockingQueue<String> msgQueue){


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();


        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;


    }


    public KafkaProducer<String, String> createKafkaProducer(){
        String bootstrapServers = "127.0.0.1:9092";

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //high throughput producer (compress based on batch
        // at the expense of  a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));


        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

}
