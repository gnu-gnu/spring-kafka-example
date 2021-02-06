package com.gnu.spring.kafka.springkafkaexample;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.gnu.spring.kafka.springkafkaexample.clients.KafkaClient;
import com.gnu.spring.kafka.springkafkaexample.dto.PojoMessage;
import com.gnu.spring.kafka.springkafkaexample.service.KafkaService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.util.Assert;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootTest
@EmbeddedKafka(topics = {KafkaService.POJO_TOPIC, KafkaService.REPLY_TOPIC, KafkaService.TOPIC}, brokerProperties = {"listeners=PLAINTEXT://127.0.0.1:9092"})
class SpringKafkaExampleApplicationTests {
    private static final Logger LOG = LoggerFactory.getLogger(SpringKafkaExampleApplicationTests.class);

    @Autowired
    private EmbeddedKafkaBroker broker;
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired
    @Qualifier("pojoKafkaTemplate")
    private KafkaTemplate<String, Object> pojoKafkaTemplate;
    @Autowired
    private RoutingKafkaTemplate routingKafkaTemplate;
    @Autowired
    private ReplyingKafkaTemplate<String, String, String> replyTemplate;
    @Autowired
    private KafkaAdmin kafkaAdmin;
    @Autowired
    private KafkaClient kafkaClient;
    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, PojoMessage> listenerContainerFactory;
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Test
    void produceForString() throws JsonProcessingException {
        kafkaClient.produceForString("handler-topic-1", "This is String");
    }

    @Test
    void produceForPojo() throws JsonProcessingException, InterruptedException {
        MessageListenerContainer pojoListenerContainer = registry.getListenerContainer("pojo_container");
        pojoListenerContainer.stop();
        AcknowledgingConsumerAwareMessageListener messageListener = (AcknowledgingConsumerAwareMessageListener) pojoListenerContainer.getContainerProperties().getMessageListener();
        CountDownLatch latch = new CountDownLatch(1);
        pojoListenerContainer.getContainerProperties().setMessageListener((AcknowledgingConsumerAwareMessageListener<String, PojoMessage>) (data, acknowledgment, consumer) -> {
            LOG.info("-- Test message consumed, pass to KafkaListener");
            messageListener.onMessage(data,acknowledgment,consumer);
            latch.countDown();
        });
        pojoListenerContainer.start();
        kafkaClient.produceForPojo(KafkaService.POJO_TOPIC, new PojoMessage(1, "Pojo Message", true));
        Assert.isTrue(latch.await(5L, TimeUnit.SECONDS), "POJO produce failed");

    }


}
