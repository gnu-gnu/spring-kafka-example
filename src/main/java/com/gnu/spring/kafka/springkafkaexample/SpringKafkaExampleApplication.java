package com.gnu.spring.kafka.springkafkaexample;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.gnu.spring.kafka.springkafkaexample.clients.KafkaClient;
import com.gnu.spring.kafka.springkafkaexample.dto.PojoMessage;
import com.gnu.spring.kafka.springkafkaexample.service.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.util.concurrent.ListenableFutureCallback;

@SpringBootApplication
public class SpringKafkaExampleApplication implements CommandLineRunner {

    final KafkaClient kafkaClient;

    private static final Logger LOG = LoggerFactory.getLogger(SpringKafkaExampleApplication.class);

    public SpringKafkaExampleApplication(KafkaClient kafkaClient) {
        this.kafkaClient = kafkaClient;
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaExampleApplication.class, args);
    }

    public void run(String... args) throws JsonProcessingException {
        kafkaClient.kafkaStatus();
        // Java 객체로 된 메시지를 Kafka 로 전송
        kafkaClient.produceForPojo(KafkaService.POJO_TOPIC, new PojoMessage(System.currentTimeMillis(), "Pojo Message", true));

        kafkaClient.produceAndReply(KafkaService.REPLY_TOPIC, "Reply message").addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
                LOG.error("\u001B[31mError returned: {}", throwable);
            }

            @Override
            public void onSuccess(ConsumerRecord<String, String> record) {
                LOG.info("\u001B[32mResponse: {}", record.value());
            }
        });

        kafkaClient.produceAndReply(KafkaService.REPLY_TOPIC, "error").addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
                LOG.error("\u001B[31mError returned: {}", throwable);
            }

            @Override
            public void onSuccess(ConsumerRecord<String, String> record) {
                LOG.info("\u001B[32mResponse: {}", record.value());
            }
        });
    }


}
