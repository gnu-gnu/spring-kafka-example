package com.gnu.spring.kafka.springkafkaexample;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringKafkaExampleApplication {
    private static final Logger LOG = LoggerFactory.getLogger(SpringKafkaExampleApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaExampleApplication.class, args);
    }

    public void run(String... args) throws JsonProcessingException {
        // kafkaStatus();
        // produceForString("for-produce", "String Message");
        // produceForPojo(KafkaService.POJO_TOPIC, new PojoMessage(1, "Pojo Message", true));
        // produceAndReply("for-reply", "Replying message");
    }


}
