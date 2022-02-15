package com.github.mksmanjit.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
    public static void main(String args[]) throws ExecutionException, InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";


        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i = 1;i<=10;i++) {
            String topic = "first_topic";
            String value = "HelloWorld " + i;
            String key = "id_" + i;
            logger.info("Key " + key);
            ProducerRecord<String, String> record = new ProducerRecord(topic,key,value);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata. " + "\n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "TimeStamp:" + recordMetadata.timestamp() + "\n"
                        );
                    } else {

                    }
                }
            }).get();
        }
        producer.flush();
        producer.close();


    }
}
