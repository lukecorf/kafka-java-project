package com.github.lukecorf.tutorial1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    public static void main (String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

        //Create Producer Properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //Help the productor know how serialize item to bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //Create The Producer
        //The key are a string and the value are a string
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        //Create a Producer Record
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","test");
        //Send Data - asynchronous
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                //Executes every time a record is successfully sent or an exception is thrown
                if(e == null) {
                    logger.info("Received new metadata.\n" +
                                "Topic:"+record.topic()+"\n" +
                                "Partition:"+record.partition()+"\n" +
                                "Timestamp:"+record.timestamp());
                } else {
                    logger.error("Error while producing",e);
                }
            }
        });
        //Flush item
        producer.flush();
        //Close producer
        producer.close();
    }

}
