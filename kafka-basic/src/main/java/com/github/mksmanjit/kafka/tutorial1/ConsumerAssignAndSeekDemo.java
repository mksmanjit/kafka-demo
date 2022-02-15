package com.github.mksmanjit.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerAssignAndSeekDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerAssignAndSeekDemo.class);
    public static void main(String args[]) {
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-five-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        int numberOfMessagedToRead = 5;
        consumer.seek(partitionToReadFrom, 15l);
        boolean keepOnReading = true;
        int numberOfMessageReadSoFar = 0;

        while (keepOnReading) {
          ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
            numberOfMessageReadSoFar++;
          for(ConsumerRecord<String,String> record : records) {
              logger.info("Key " + record.key() + " Value: " + record.value());
              if(numberOfMessageReadSoFar >= numberOfMessagedToRead) {
                  keepOnReading = false;
              }
          }

        }
    }
}
