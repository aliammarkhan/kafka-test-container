package com.kafa.producer.demo.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = "DEMO-TOPIC", groupId = "jt-group")
    public void consumeEvents(String message) {
        log.info("Received Message {}", message);
    }
}
