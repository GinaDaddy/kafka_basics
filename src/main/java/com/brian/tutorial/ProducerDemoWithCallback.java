package com.brian.tutorial;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prerequisite: topic "first_topic" should be created.
 */
public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");

        // send data - asynchronus. This code will send a record but you can't see it by below cli, becasue it is asynchronous
        // ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group my-first-application
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                // executes every time a record is successfully sent or an exception is thrown.
                if (e == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata. \nTopic:{}\nPartition:{}\nOffset:{}\nTimestamp:{}",
                        metadata.timestamp(), metadata.partition(), metadata.offset(), metadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            }
        });

        // you need to flush data
        producer.flush();
        // or you need to flush and close producer
        producer.close();
    }
}
