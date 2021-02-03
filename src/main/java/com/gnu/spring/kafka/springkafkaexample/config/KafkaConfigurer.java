package com.gnu.spring.kafka.springkafkaexample.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Configuration
public class KafkaConfigurer {

    private final ProducerFactory<String, String> pf;
    private final ConsumerFactory<String, String> cf;
    private final KafkaProperties properties;


    public KafkaConfigurer(ProducerFactory<String, String> pf, ConsumerFactory<String, String> cf, KafkaProperties kafkaProperties) {
        this.pf = pf;
        this.cf = cf;
        this.properties = kafkaProperties;
    }

    @Bean
    public ReplyingKafkaTemplate<String, String, String> replyingTemplate() {
        ContainerProperties replyProperties = new ContainerProperties("replies");
        replyProperties.setGroupId("replies-consumer-group-1");
        ConcurrentMessageListenerContainer<String, String> repliesContainer = new ConcurrentMessageListenerContainer<>(cf, replyProperties);
        repliesContainer.setAutoStartup(false);
        return new ReplyingKafkaTemplate<>(pf, repliesContainer);
    }

    @Bean
    @Primary
    public KafkaTemplate<?, ?> kafkaTemplate(ProducerListener<Object, Object> kafkaProducerListener, ObjectProvider<RecordMessageConverter> messageConverter) {
        KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate(pf);
        messageConverter.ifUnique(kafkaTemplate::setMessageConverter);
        kafkaTemplate.setProducerListener(kafkaProducerListener);
        return kafkaTemplate;
    }
    @Bean
    public KafkaTemplate<?, ?> pojoKafkaTemplate(ProducerListener<Object, Object> kafkaProducerListener, ObjectProvider<RecordMessageConverter> messageConverter) {
        Map<String, Object> defaultProperties = pf.getConfigurationProperties();
        LinkedHashMap<String, Object> configurationProperties = new LinkedHashMap<>(defaultProperties);
        configurationProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        DefaultKafkaProducerFactory<Object, Object> pojoPf = new DefaultKafkaProducerFactory<>(configurationProperties);
        KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate(pojoPf);
        messageConverter.ifUnique(kafkaTemplate::setMessageConverter);
        kafkaTemplate.setProducerListener(kafkaProducerListener);
        return kafkaTemplate;
    }

    @Bean
    public RoutingKafkaTemplate routingKafkaTemplate(GenericApplicationContext context){
        Map<String, Object> configurationProperties = new HashMap<>(pf.getConfigurationProperties());
        configurationProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        DefaultKafkaProducerFactory<Object, Object> pojoPf = new DefaultKafkaProducerFactory<>(configurationProperties);
        context.registerBean(DefaultKafkaProducerFactory.class, "pojo", pojoPf);
        Map<Pattern, ProducerFactory<Object, Object>> map = new LinkedHashMap<>();
        map.put(Pattern.compile("for-produce-pojo"), pojoPf);
        map.put(Pattern.compile(".+"), new DefaultKafkaProducerFactory<>(pf.getConfigurationProperties()));
        return new RoutingKafkaTemplate(map);
    }

    @Bean
    @Primary
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, (ConsumerFactory)kafkaConsumerFactory.getIfAvailable(() -> {
            return new DefaultKafkaConsumerFactory(this.properties.buildConsumerProperties());
        }));
        return factory;
    }
    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaPojoListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, (ConsumerFactory)kafkaConsumerFactory.getIfAvailable(() -> {
            return new DefaultKafkaConsumerFactory(this.properties.buildConsumerProperties());
        }));
        factory.setMessageConverter(new StringJsonMessageConverter());
        return factory;
    }

    /*
    @Bean
    public StringJsonMessageConverter stringJsonMessageConverter(){
        return new StringJsonMessageConverter();
    }
*/

}
