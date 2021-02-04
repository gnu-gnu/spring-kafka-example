package com.gnu.spring.kafka.springkafkaexample;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gnu.spring.kafka.springkafkaexample.dto.PojoMessage;
import com.gnu.spring.kafka.springkafkaexample.service.KafkaService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.SerializationUtils;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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
