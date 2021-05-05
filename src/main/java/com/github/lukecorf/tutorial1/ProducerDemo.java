package com.github.lukecorf.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main (String[] args) {
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
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","hello world");
        //Send Data - asynchronous
        producer.send(record);
        //Flush item
        producer.flush();
        //Close producer
        producer.close();
    }

}
