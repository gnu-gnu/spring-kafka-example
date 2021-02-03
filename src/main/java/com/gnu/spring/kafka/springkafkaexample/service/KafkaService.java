package com.gnu.spring.kafka.springkafkaexample.service;

import com.gnu.spring.kafka.springkafkaexample.dto.PojoMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaService.class);
    public static final String CONSUMER_GROUP = "for-produce-group";
    public static final String POJO_CONSUMER_GROUP = "for-produce-pojo-group";
    public static final String REPLY_CONSUMER_GROUP = "for-reply-group";
    /**
     * KafkaAutoConfigruation이 제공하는 ListenerFactory가 아닌 다른 Factory를 사용할 때는 Bean 이름을 @KafkaListener의 containerFactory에 제공한다
     * @see com.gnu.spring.kafka.springkafkaexample.config.KafkaConfigurer#kafkaPojoListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer, ObjectProvider) 
     */
    public static final String POJO_CONTAINER_FACTORY = "kafkaPojoListenerContainerFactory";
    public static final String TOPIC = "for-produce";
    public static final String POJO_TOPIC = "for-produce-pojo";
    public static final String REPLY_TOPIC = "for-reply";

    @KafkaListener(topics = TOPIC, groupId = CONSUMER_GROUP)
    public void consumer(String value, ConsumerRecord<String, String> record){
        LOG.info("String message delivered: {}", value);
    }

    @KafkaListener(topics = POJO_TOPIC, groupId = POJO_CONSUMER_GROUP, containerFactory = POJO_CONTAINER_FACTORY)
    public void pojoConsumer(PojoMessage value){
        LOG.info("Pojo message delivered: {}", value);
    }

    @KafkaListener(topics = REPLY_TOPIC, groupId = REPLY_CONSUMER_GROUP)
    @SendTo
    public String forReply(String value, ConsumerRecord<String, String> record){
        LOG.info("String message for reply delivered: {}", value);
        return value.toUpperCase();
    }
}
