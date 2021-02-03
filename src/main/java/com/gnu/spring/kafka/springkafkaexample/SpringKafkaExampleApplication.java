package com.gnu.spring.kafka.springkafkaexample;

import com.gnu.spring.kafka.springkafkaexample.dto.PojoMessage;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@SpringBootApplication
public class SpringKafkaExampleApplication implements CommandLineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(SpringKafkaExampleApplication.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTemplate<String, Object> pojoKafkaTemplate;
    private final RoutingKafkaTemplate routingKafkaTemplate;
    private final ReplyingKafkaTemplate<String, String, String> replyTemplate;
    private final KafkaAdmin kafkaAdmin;

    public SpringKafkaExampleApplication(KafkaTemplate<String, Object> kafkaTemplate,
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

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaExampleApplication.class, args);
    }

    @Override
    public void run(String... args) {
        // kafkaStatus();
        produceForString("for-produce", "String Message");
        produceForPojo("for-produce-pojo", new PojoMessage(1, "Pojo Message", true));
        produceAndReply("for-reply", "Replying message");
    }

    private void produceForString(String topic, String msg) {
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
                LOG.info("-- String Produce result: Topic {}, Partition {}, Offset {}", topic, partition, offset);
            }
        });
    }

    private void produceForPojo(String topic, PojoMessage msg) {
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
                LOG.info("-- Pojo Produce result: Topic {}, Partition {}, Offset {}", topic, partition, offset);
            }
        });
    }

    private void produceAndReply(String topic, String msg) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
        replyTemplate.sendAndReceive(record).addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
                throwable.getStackTrace();
            }

            @Override
            public void onSuccess(ConsumerRecord<String, String> record) {
                LOG.info("-- Response: {}", record.value());
            }
        });
    }

    private void kafkaStatus() {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            DescribeClusterResult cluster = adminClient.describeCluster();
            cluster.clusterId().whenComplete((id, t) -> {
                LOG.info("-- Cluster Id: {}", id);
            });
            cluster.nodes().whenComplete((nodes, t) -> {
                nodes.forEach(node -> {
                    LOG.info("-- Node: {}", node);
                });
            });
            cluster.controller().whenComplete((controller, t) -> {
                LOG.info("-- Controller: {}", controller);
            });
            try {
                Collection<TopicListing> topics = adminClient.listTopics().listings().get();
                ArrayList<String> topicList = topics.stream().map(topic -> topic.name()).collect(Collectors.toCollection(ArrayList::new));
                adminClient.describeTopics(topicList).all().whenComplete((topicDescs, t2) -> {
                    topicDescs.entrySet().forEach(topicDesc -> {
                        LOG.info("-- Topic: {}", topicDesc.getValue());
                    });
                });

                Collection<ConsumerGroupListing> consumerGroups = adminClient.listConsumerGroups().all().get();
                consumerGroups.forEach(group -> {
                    LOG.info("-- ConsumerGroup: {}", group);
                    String groupId = group.groupId();
                    adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().whenComplete(((metadataMap, throwable) -> {
                        LOG.info("---- ConsumerGroup {} detail, Offset: {}", groupId, metadataMap);
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
