package com.learning.beginner.kafka.wikimedia.components;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@RequiredArgsConstructor
public class ConsumerComponent {

    private static final Logger log = LoggerFactory.getLogger(ConsumerComponent.class.getSimpleName());
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final RestHighLevelClient restHighLevelClient;

    @PreDestroy
    public void onShutDown() throws IOException {
        log.info("Running pre destroy task as shutdown signal received!!");
        kafkaConsumer.wakeup();
        log.info("Closing kafka consumer!!");
        kafkaConsumer.close();
        log.info("Closing opensearch client!!");
        restHighLevelClient.close();
        log.info("Consumer is now gracefully shutdown!!!");
    }
}
