package com.gnu.spring.kafka.springkafkaexample.service;

import com.gnu.spring.kafka.springkafkaexample.dto.PojoMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;

//@KafkaListener(topics = "handler-topic-1", groupId = "handler-group-1", containerFactory = "kafkaPojoListenerContainerFactory")
public class KafkaHandlerService {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaHandlerService.class);


    @KafkaHandler(isDefault = true)
    public void stringHandler(String value){
        LOG.info("String {}",value);
    }

    @KafkaHandler
    public void pojoHandler(PojoMessage value){
        LOG.info("POJO {}",value);
    }
}
