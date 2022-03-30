package com.github.clumsy48.kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
    
    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        for(int i=0;i<10;++i){
            String topic = "demo-first-topic";
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,"id_"+i,"Hello Kafka"+i);

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    logger.info("TOPIC : "+recordMetadata.topic()+"\n"+
                            "PARTITION : "+recordMetadata.partition());
                }
            });
        }


        producer.close();
    }
}
