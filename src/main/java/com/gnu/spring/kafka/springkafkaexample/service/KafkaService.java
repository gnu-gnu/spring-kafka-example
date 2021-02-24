package com.gnu.spring.kafka.springkafkaexample.service;

import com.gnu.spring.kafka.springkafkaexample.dto.PojoMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaService.class);
    public static final String CONSUMER_GROUP = "for-produce-group";
    public static final String POJO_CONSUMER_GROUP = "for-produce-pojo-group";
    public static final String REPLY_CONSUMER_GROUP = "for-reply-group";
    public static final String POJO_CONTAINER_FACTORY = "kafkaPojoListenerContainerFactory";
    public static final String STRING_TOPIC = "string-topic";
    public static final String POJO_TOPIC = "pojo-topic";
    public static final String REPLY_TOPIC = "reply-topic";

    /**
     * 
     * String 으로 된 메시지를 Consume 하는 컨슈머
     * 
     * @param value
     * @param record
     */
    @KafkaListener(topics = STRING_TOPIC, groupId = CONSUMER_GROUP)
    public void consumer(String value, ConsumerRecord<String, String> record) {
        LOG.info("\u001B[32mString message delivered: {}", value);
    }

    /**
     * 
     * 자바 객체로 된 메시지를  Consume하는 컨슈머
     * 
     * @param value
     */
    @KafkaListener(topics = POJO_TOPIC, groupId = POJO_CONSUMER_GROUP, containerFactory = POJO_CONTAINER_FACTORY, id = "pojo_container", errorHandler = "listenErrorHandler")
    public void pojoConsumer(PojoMessage value) {
        LOG.info("\u001B[32mPojo message delivered: {}", value);
    }

    /**
     *
     * 카프카를 요청-응답의 동기식 구조로 사용하기 위해서 @SendTo 애너테이션을 사용
     *
     * @param value
     * @param record
     * @return
     */
    @KafkaListener(topics = REPLY_TOPIC, groupId = REPLY_CONSUMER_GROUP, errorHandler = "listenErrorHandler")
    @SendTo
    public String forReply(String value, ConsumerRecord<String, String> record) {
        LOG.info("\u001B[32mString message for reply delivered: {}", value);
        if(value.contains("error")){
            throw new RuntimeException("trigger error");
        } else {
            return value.toUpperCase();
        }
    }

    /**
     *
     * KafkaListener 내부에서 작업이 오류가 발생했을 경우 처리를 위한 에러 핸들러
     * KafkaListenerErrorHandler의 return값은 SendTo의 Reply의 Fallback의 용도이다. (이외에는 무시)
     *
     * @return
     */
    @Bean
    public KafkaListenerErrorHandler listenErrorHandler() {
        return (message, exception) -> {
            final StringBuilder sb = new StringBuilder();
            message.getHeaders().forEach((key, value) -> {
                sb.append(System.lineSeparator()).append(key).append(":").append(value);
            });
            LOG.error("listener error: {}", sb.toString());
            return "\u001B[31mError occured";
        };
    }
}
