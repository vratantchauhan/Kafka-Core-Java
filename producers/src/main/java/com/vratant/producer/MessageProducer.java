package com.vratant.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MessageProducer {

    String topicA = "topic-a";
    String topicWithReplicationFactorThree = "test-topic-with-replication-factor";
    KafkaProducer<String, String> kafkaProducer;
    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    Callback callback = ((metadata, exception) -> {
        if (exception != null) {
            logger.error("Exception during sending message.");
        } else {
            logger.info("Message sent successfully to partition: ", metadata.partition());
        }
    });

    public void close() {
        kafkaProducer.close();
    }
    public MessageProducer(Map<String, Object> propsMap) {

        kafkaProducer = new KafkaProducer<String, String>(propsMap);
    }

    public static Map<String, Object> propsMap() {

        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.ACKS_CONFIG, "all");
        propsMap.put(ProducerConfig.RETRIES_CONFIG, 20);
        propsMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 3000);
        return propsMap;
    }

    public void publishMessageAsync(String key, String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicWithReplicationFactorThree, key, value);
        Future<RecordMetadata> recordMetadata = kafkaProducer.send(producerRecord, callback);
        logger.info("offset: ", recordMetadata.get().offset());
    }

    public void publishMessageSync(String key, String value) throws ExecutionException, InterruptedException {

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicWithReplicationFactorThree, key, value);
        RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
        logger.info("Partition: {} Offset: {} ", recordMetadata.partition(), recordMetadata.offset());
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        MessageProducer messageProducer = new MessageProducer(propsMap());
        messageProducer.publishMessageSync("300", "abc");
        messageProducer.publishMessageAsync(null, "xyz-async");
    }
}
