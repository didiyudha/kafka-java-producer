package com.github.didiyudha.kafka.tutorial2;

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

    static Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private String consumerKey;
    private String consumerSecret;
    private String token;
    private String secret;
    private List<String> terms = Lists.newArrayList("kafka");

    public TwitterProducer() {
        this.consumerKey = "XnjzSwz7MEyL7lGjlLM5XWnjN";
        this.consumerSecret = "E5lneV721pyGpAaMzBH7xNOxGn7ZMlIsG3e1cheTEP5qjOwfao";
        this.token = "190505732-VkGau0qSN9fWrmiVPaMrKMrHWhxLV6SY6r2vUYod";
        this.secret = "JftrYybiqbJD98fGQaThoSarRgqLWUiQcXzliRLsSxovz";

    }

    public static void main(String[] args) {
        TwitterProducer twitterProducer = new TwitterProducer();
        twitterProducer.run();
    }

    public void run() {
        // Create a twitter client.
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client hosebirdClient = this.createTwitterClient(msgQueue);
        hosebirdClient.connect();

        // create kafka producer.
        KafkaProducer<String, String> producer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            logger.info("stopping application");
            logger.info("shutting down client from twitter...");
            hosebirdClient.stop();
            logger.info("closing producer...");
            producer.close();
        }));

        // loop to send tweets to kafka.
        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }
            if (msg != null) {
                logger.info(msg);
                ProducerRecord<String, String> record = new ProducerRecord<>("twitter_tweets", null, msg);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("something bad happened", e);
                        }                    }
                });
            }
        }

        logger.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /*BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);*/

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();



        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(this.consumerKey, this.consumerSecret, this.token, this.secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
                //.eventMessageQueue(eventQueue);

        return builder.build();
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        // Create producer properties.
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        String acks = "all";

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, acks);

        // Create the producer.
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
