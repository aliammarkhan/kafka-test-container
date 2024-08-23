package com.kafa.producer.demo;

import com.kafa.producer.demo.service.KafkaConsumer;
import com.kafa.producer.demo.service.KafkaProducer;
import nl.altindag.log.LogCaptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
public class KafkaConsumerTests {
    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.5"));

    @Autowired
    private KafkaProducer producer;

    private static LogCaptor logCaptor;

    @DynamicPropertySource
    public static void init(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka:: getBootstrapServers);
    }

    @BeforeEach
    public void setupLogCaptor() {
        logCaptor = LogCaptor.forClass(KafkaConsumer.class);
        logCaptor.setLogLevelToInfo();
    }

    @Test
    public void testSendMessageToTopic() {
        String message = "Hello World";
        producer.sendMessageToTopic(message);
        await().pollInterval(Duration.ofSeconds(3))
                .atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    // assert producer has received the message
                    assertThat(logCaptor.getInfoLogs()).contains("Received Message " + message);
                });
    }
}
