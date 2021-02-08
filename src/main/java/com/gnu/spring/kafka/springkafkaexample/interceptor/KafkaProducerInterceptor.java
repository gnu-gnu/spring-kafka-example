package com.gnu.spring.kafka.springkafkaexample.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class KafkaProducerInterceptor implements ProducerInterceptor<String, String> {

    private final Logger LOG = LoggerFactory.getLogger(KafkaProducerInterceptor.class);

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        LOG.info("\u001B[32mSend: {}", record);
        record.headers().headers("kafka_correlationId").forEach(header->{
            String key = header.key();
            int correlationId = header.value().hashCode();
            LOG.info("\u001B[32mProduce {}: {}", key, correlationId);
        });
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        LOG.info("\u001B[32mTopic '{}' Ack: Partition {}-Offset {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
    }

    @Override
    public void close() {
        LOG.info("\u001B[32mClose");
    }

    @Override
    public void configure(Map<String, ?> map) {
        LOG.info("\u001B[32mConfig: {}", map);
    }
}
