package com.vratant.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;


public class MessageConsumer {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);
    KafkaConsumer<String, String> kafkaConsumer;
    ArrayList<String> topicName = new ArrayList<String>();
    public MessageConsumer(HashMap<String, Object> propsMap) {
        kafkaConsumer = new KafkaConsumer<>(propsMap);
    }

    public static HashMap<String, Object> buildConsumerProperties() {
        HashMap<String, Object> propsMap = new HashMap<>();
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerOne");
        return propsMap;
    }

    public void kafkaPoll() {
        kafkaConsumer.subscribe(topicName);
        Duration timeOutDuration = Duration.of(100, ChronoUnit.MILLIS);
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecord = kafkaConsumer.poll(timeOutDuration);
                consumerRecord.forEach((record) -> {
                    logger.info("Received message with key {} and value {} from partition {} ",
                            record.key(), record.value(), record.partition());
                });
            }
        } catch (Exception e) {
            logger.error("Exception in MessageConsumer: " + e);
        } finally {
            kafkaConsumer.close();
        }
    }

    public static void main(String[] args) {
        MessageConsumer messageConsumer = new MessageConsumer(buildConsumerProperties());
        messageConsumer.topicName.add("test-topic-with-replication-factor");
        messageConsumer.kafkaPoll();
    }
}
