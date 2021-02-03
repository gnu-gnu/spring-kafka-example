package com.gnu.spring.kafka.springkafkaexample.service;

import com.gnu.spring.kafka.springkafkaexample.dto.PojoMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaService.class);

    @KafkaListener(topics = "for-produce", groupId = "for-produce-group")
    public void consumer(String value, ConsumerRecord<String, String> record){
        LOG.info("String message delivered: {}", value);
    }

    @KafkaListener(topics = "for-produce-pojo", groupId = "for-produce-pojo-group", containerFactory = "kafkaPojoListenerContainerFactory")
    public void pojoConsumer(PojoMessage value){
        LOG.info("Pojo message delivered: {}", value);
    }

    @KafkaListener(topics = "for-reply", groupId = "for-reply-group")
    @SendTo
    public String forReply(String value, ConsumerRecord<String, String> record){
        LOG.info("String message for reply delivered: {}", value);
        return value.toUpperCase();
    }
}
