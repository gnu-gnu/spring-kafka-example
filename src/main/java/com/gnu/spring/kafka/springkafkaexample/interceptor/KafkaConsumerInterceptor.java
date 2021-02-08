package com.gnu.spring.kafka.springkafkaexample.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class KafkaConsumerInterceptor implements ConsumerInterceptor<String, String> {

    private final Logger LOG = LoggerFactory.getLogger(KafkaConsumerInterceptor.class);

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {
        consumerRecords.forEach(record -> {
            LOG.info("\u001B[32mConsume: {}", record);
            record.headers().headers("kafka_correlationId").forEach(header->{
                String key = header.key();
                int correlationId = header.value().hashCode();
                LOG.info("\u001B[32mConsume {}: {}", key, correlationId);
            });
        });
        return consumerRecords;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        LOG.info("\u001B[32mConsumer commit: {}", map);
    }

    @Override
    public void close() {
        LOG.info("\u001B[32mConsumer close");
    }

    @Override
    public void configure(Map<String, ?> map) {
        LOG.info("\u001B[32mConusumer config: {}", map);
    }
}
