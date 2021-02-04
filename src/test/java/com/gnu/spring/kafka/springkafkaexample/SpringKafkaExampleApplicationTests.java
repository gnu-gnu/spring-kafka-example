package com.gnu.spring.kafka.springkafkaexample;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.gnu.spring.kafka.springkafkaexample.clients.KafkaClient;
import com.gnu.spring.kafka.springkafkaexample.dto.PojoMessage;
import com.gnu.spring.kafka.springkafkaexample.service.KafkaService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest
@EmbeddedKafka(topics = {KafkaService.POJO_TOPIC, KafkaService.REPLY_TOPIC, KafkaService.TOPIC}, brokerProperties = {"listeners=PLAINTEXT://127.0.0.1:9092"})
class SpringKafkaExampleApplicationTests {

    @Autowired private EmbeddedKafkaBroker broker;
    @Autowired private KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired @Qualifier("pojoKafkaTemplate") private KafkaTemplate<String, Object> pojoKafkaTemplate;
    @Autowired private RoutingKafkaTemplate routingKafkaTemplate;
    @Autowired private ReplyingKafkaTemplate<String, String, String> replyTemplate;
    @Autowired private KafkaAdmin kafkaAdmin;
    @Autowired private KafkaClient kafkaClient;


    @Test
    public void produceForString() throws JsonProcessingException {
        kafkaClient.produceForString("handler-topic-1", "This is String");
    }

    @Test
    public void produceForPojo() throws JsonProcessingException {
        kafkaClient.produceForPojo("handler-topic-1", new PojoMessage(1, "Pojo Message", true));
    }

}
