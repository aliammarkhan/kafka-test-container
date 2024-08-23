package com.kafa.producer.demo.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, Object> template;
    public void sendMessageToTopic(String message) {
        CompletableFuture<SendResult<String, Object>> future = template.send("DEMO-TOPIC", message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                log.error("Unable to send message=[" + message + "] due to : " + ex.getMessage());
            }
        });

    }
}
