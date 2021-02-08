package com.gnu.spring.kafka.springkafkaexample.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Configuration
public class KafkaConfigurer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConfigurer.class);

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
     *
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
     * application.properties에 설정한 기본 Serializer와 다른 Serializer를 사용하기 위해 별도로 설정한 KafkaTemplate
     * POJO를 Produce하기 위해서는 JsonSerializer를 사용해야 한다.
     * 이 때 Consumer가 StringDeserializer를 사용하면 Spring의 StirngJsonConverter를 사용하고
     * Consumer가 Kafka의 JsonDeserializer를 사용하면 TrustedPackage를 지정 해 주어야 한다.
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
        kafkaTemplate.setProducerListener(kafkaProducerListener);
        return kafkaTemplate;
    }

    @Bean
    public KafkaTemplate<?, ?> jsonKafkaTemplate(ProducerListener<Object, Object> kafkaProducerListener, ObjectProvider<RecordMessageConverter> messageConverter) {
        Map<String, Object> defaultProperties = pf.getConfigurationProperties();
        LinkedHashMap<String, Object> configurationProperties = new LinkedHashMap<>(defaultProperties);
        configurationProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configurationProperties.put(JsonSerializer.TYPE_MAPPINGS, "pojo:com.gnu.spring.kafka.springkafkaexample.dto.PojoMessage");
        DefaultKafkaProducerFactory<Object, Object> jsonPf = new DefaultKafkaProducerFactory<>(configurationProperties);
        KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate(jsonPf);
        kafkaTemplate.setProducerListener(kafkaProducerListener);
        kafkaTemplate.setMessageConverter(new JsonMessageConverter());
        return kafkaTemplate;
    }

    /**
     * 별도의 KafkaTemplate을 사용하지 않고 하나의 KafkaTemplate을 통해서 TOPIC별로 각각의 Serializer를 적용할 경우 사용하는 KafkaTemplate
     *
     * @param context
     * @return
     */
    @Bean
    public RoutingKafkaTemplate routingKafkaTemplate(GenericApplicationContext context) {
        Map<String, Object> configurationProperties = new HashMap<>(pf.getConfigurationProperties());
        DefaultKafkaProducerFactory<Object, Object> pojoPf = new DefaultKafkaProducerFactory<>(configurationProperties);
        context.registerBean(DefaultKafkaProducerFactory.class, "pojo", pojoPf);
        Map<Pattern, ProducerFactory<Object, Object>> map = new LinkedHashMap<>();
        map.put(Pattern.compile("for-produce-pojo"), pojoPf);
        map.put(Pattern.compile(".+"), new DefaultKafkaProducerFactory<>(pf.getConfigurationProperties()));
        return new RoutingKafkaTemplate(map);
    }

    /**
     * 한 애플리케이션 안에서 여러 Type을 받아들이는 KafkaListener가 존재하는 경우 KafkaListener가 사용하는 ListenerContainerFactory를 다르게 지정해야 하므로 여러 개를 생성 해 줌
     *
     * @param configurer
     * @param kafkaConsumerFactory
     * @return
     */
    @Bean
    @Primary
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> listenerFactory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(listenerFactory, new DefaultKafkaConsumerFactory(this.properties.buildConsumerProperties()));
        return listenerFactory;
    }

    /**
     * POJO를 Listen하는 @KafkaListener를 위한 Container Factory
     *
     * @param configurer
     * @return
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaPojoListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer) {
        Map<String, Object> consumerProperties = this.properties.buildConsumerProperties();
        DefaultKafkaConsumerFactory consumerFactory = new DefaultKafkaConsumerFactory(consumerProperties);
        ConcurrentKafkaListenerContainerFactory<Object, Object> listenerFactory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(listenerFactory, consumerFactory);
        listenerFactory.setMessageConverter(new StringJsonMessageConverter()); // Kakfa로 전달된 String(JSON format)을 POJO로 변환 수행할 수 있는 메시지 컨버터를 설정
        SeekToCurrentErrorHandler seekToCurrentErrorHandler = new SeekToCurrentErrorHandler(new FixedBackOff(0L, 10L));
        listenerFactory.setErrorHandler(seekToCurrentErrorHandler);
        return listenerFactory;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaJsonListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer) {
        Map<String, Object> consumerProperties = this.properties.buildConsumerProperties();
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProperties.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        consumerProperties.put(JsonDeserializer.TYPE_MAPPINGS, "pojo:com.gnu.spring.kafka.springkafkaexample.dto.PojoMessage");
        DefaultKafkaConsumerFactory consumerFactory = new DefaultKafkaConsumerFactory(consumerProperties);
        ConcurrentKafkaListenerContainerFactory<Object, Object> listenerFactory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(listenerFactory, consumerFactory);
        return listenerFactory;
    }
}
