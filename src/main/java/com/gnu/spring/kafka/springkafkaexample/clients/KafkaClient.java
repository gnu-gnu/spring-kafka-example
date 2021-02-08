package com.gnu.spring.kafka.springkafkaexample.clients;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.gnu.spring.kafka.springkafkaexample.dto.PojoMessage;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class KafkaClient {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaClient.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTemplate<String, Object> pojoKafkaTemplate;
    private final RoutingKafkaTemplate routingKafkaTemplate;
    private final ReplyingKafkaTemplate<String, String, String> replyTemplate;
    private final KafkaAdmin kafkaAdmin;

    public KafkaClient(KafkaTemplate<String, Object> kafkaTemplate,
                                         @Qualifier("pojoKafkaTemplate") KafkaTemplate<String, Object> pojoKafkaTemplate,
                                         RoutingKafkaTemplate routingKafkaTemplate,
                                         ReplyingKafkaTemplate<String, String, String> replyTemplate,
                                         KafkaAdmin kafkaAdmin) {
        this.kafkaTemplate = kafkaTemplate;
        this.pojoKafkaTemplate = pojoKafkaTemplate;
        this.routingKafkaTemplate = routingKafkaTemplate;
        this.replyTemplate = replyTemplate;
        this.kafkaAdmin = kafkaAdmin;
    }

    public void produceForString(String topic, String msg) {
        kafkaTemplate.send(topic, msg).addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
                throwable.getStackTrace();
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                RecordMetadata metadata = result.getRecordMetadata();
                String topic = metadata.topic();
                long offset = metadata.offset();
                int partition = metadata.partition();
                LOG.info("\u001B[32mString Produce result: Topic {}, Partition {}, Offset {}", topic, partition, offset);
            }
        });
    }

    public void produceForPojo(String topic, PojoMessage msg) throws JsonProcessingException {
        pojoKafkaTemplate.send(topic, msg).addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {

            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                RecordMetadata metadata = result.getRecordMetadata();
                String topic = metadata.topic();
                long offset = metadata.offset();
                int partition = metadata.partition();
                LOG.info("\u001B[32mPojo Produce result: Topic {}, Partition {}, Offset {}", topic, partition, offset);
            }
        });
    }

    public RequestReplyFuture<String, String, String> produceAndReply(String topic, String msg) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
        return replyTemplate.sendAndReceive(record);
    }

    public void kafkaStatus() {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            DescribeClusterResult cluster = adminClient.describeCluster();
            cluster.clusterId().whenComplete((id, t) -> {
                LOG.info("\u001B[32mCluster Id: {}", id);
            });
            cluster.nodes().whenComplete((nodes, t) -> {
                nodes.forEach(node -> {
                    LOG.info("\u001B[32mNode: {}", node);
                });
            });
            cluster.controller().whenComplete((controller, t) -> {
                LOG.info("\u001B[32mController: {}", controller);
            });
            try {
                Collection<TopicListing> topics = adminClient.listTopics().listings().get();
                ArrayList<String> topicList = topics.stream().map(topic -> topic.name()).collect(Collectors.toCollection(ArrayList::new));
                adminClient.describeTopics(topicList).all().whenComplete((topicDescs, t2) -> {
                    topicDescs.entrySet().forEach(topicDesc -> {
                        LOG.info("\u001B[32mTopic: {}", topicDesc.getValue());
                    });
                });

                Collection<ConsumerGroupListing> consumerGroups = adminClient.listConsumerGroups().all().get();
                consumerGroups.forEach(group -> {
                    LOG.info("\u001B[32mConsumerGroup: {}", group);
                    String groupId = group.groupId();
                    adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().whenComplete(((metadataMap, throwable) -> {
                        LOG.info("\u001B[32m  ConsumerGroup {} detail, Offset: {}", groupId, metadataMap);
                    }));
                });
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        }
    }
}
