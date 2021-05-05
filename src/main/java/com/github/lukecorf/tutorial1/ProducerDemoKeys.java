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
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main (String[] args) throws ExecutionException, InterruptedException {
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
        for(int i=0; i <10; i++) {
            String topic = "first_topic";
            String value = "Test" + i;
            String key = "key_" + i;

            //Create a Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            //The same key always go to the same partition
            logger.info(("Key: "+key));
            //Send Data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    //Executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("Received new metadata.\n" +
                                "Topic:" + metadata.topic() + "\n" +
                                "Partition:" + metadata.partition() + "\n" +
                                "Offset:" + metadata.offset() + "\n" +
                                "Timestamp:" + metadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //Block the send to make it synchronous - don`t do this in production
            //Flush item
            producer.flush();
        }
        //Close producer
        producer.close();
    }

}
