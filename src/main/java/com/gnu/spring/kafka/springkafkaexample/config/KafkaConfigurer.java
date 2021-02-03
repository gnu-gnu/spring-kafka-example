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

    /**
     * Produce한 메시지의 응답이 @SendTo를 통해서 return 으로 돌아올 때 특정 토픽으로 Produce 하여 해당 토픽에서 응답을 수신하는 Producer
     * @return
     */
    @Bean
    public ReplyingKafkaTemplate<String, String, String> replyingTemplate() {
        ContainerProperties replyProperties = new ContainerProperties("replies");
        replyProperties.setGroupId("replies-consumer-group-1");
        ConcurrentMessageListenerContainer<String, String> repliesContainer = new ConcurrentMessageListenerContainer<>(cf, replyProperties);
        repliesContainer.setAutoStartup(false);
        return new ReplyingKafkaTemplate<>(pf, repliesContainer);
    }

    /**
     * 
     * KafkaTemplate을 여러 개 사용할 경우 KafkaAutoConfiguration에서 지정한 KafkaTemplate의 Bean이 생성되지 않으므로 별도로 생성 해 줌
     * 
     * @param kafkaProducerListener
     * @param messageConverter
     * @return
     */
    @Bean
    @Primary
    public KafkaTemplate<?, ?> kafkaTemplate(ProducerListener<Object, Object> kafkaProducerListener, ObjectProvider<RecordMessageConverter> messageConverter) {
        KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate(pf);
        messageConverter.ifUnique(kafkaTemplate::setMessageConverter);
        kafkaTemplate.setProducerListener(kafkaProducerListener);
        return kafkaTemplate;
    }

    /**
     *
     * application.properties에 설정한 기본 Serializer와 다른 Serializer를 사용하기 위해 별도로 설정한 KafkaTemplate
     *
     * @param kafkaProducerListener
     * @param messageConverter
     * @return
     */
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

    /**
     *
     * 별도의 KafkaTemplate을 사용하지 않고 하나의 KafkaTemplate을 통해서 TOPIC별로 각각의 Serializer를 적용할 경우 사용하는 KafkaTemplate
     *
     * @param context
     * @return
     */
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

    /**
     * 
     * 한 애플리케이션 안에서 여러 Type을 받아들이는 KafkaListener가 존재하는 경우 KafkaListener가 사용하는 ListenerContainerFactory를 다르게 지정해야 하므로 여러 개를 생성 해 줌
     * 
     * @param configurer
     * @param kafkaConsumerFactory
     * @return
     */
    @Bean
    @Primary
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, (ConsumerFactory)kafkaConsumerFactory.getIfAvailable(() -> {
            return new DefaultKafkaConsumerFactory(this.properties.buildConsumerProperties());
        }));
        return factory;
    }

    /**
     *
     * POJO를 Listen하는 @KafkaListener를 위한 Container Factory
     *
     * @param configurer
     * @param kafkaConsumerFactory
     * @return
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaPojoListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, (ConsumerFactory)kafkaConsumerFactory.getIfAvailable(() -> {
            return new DefaultKafkaConsumerFactory(this.properties.buildConsumerProperties());
        }));
        factory.setMessageConverter(new StringJsonMessageConverter());
        return factory;
    }
}
