package com.confluent.lag.tasks;

import com.confluent.lag.beans.GroupMetrics;
import com.confluent.lag.beans.GroupMetricsMbean;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ExecutionException;

import java.util.stream.Collectors;

public class ExportTask extends TimerTask {
    static Logger log = LoggerFactory.getLogger(ExportTask.class.getName());
    private AdminClient adminClient = null;
    private KafkaConsumer consumer = null;

    public ExportTask(Properties kafkaConnectionProperties) throws IOException {
         //conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        adminClient = AdminClient.create(kafkaConnectionProperties);
        kafkaConnectionProperties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConnectionProperties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(kafkaConnectionProperties);
    }

    private Map<TopicPartition, Long> getConsumerGroupData(String groupId)  throws InterruptedException, ExecutionException{
        ListConsumerGroupOffsetsResult info = adminClient.listConsumerGroupOffsets(groupId);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = info.partitionsToOffsetAndMetadata().get();
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        Map<TopicPartition, Long> groupOffset = topicPartitionOffsetAndMetadataMap.entrySet().stream()
                .collect(Collectors.toMap(
                        entry-> entry.getKey(),
                        entry-> entry.getValue().offset()
                        ));
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(groupOffset.keySet());
        Map<TopicPartition, Long> lags  =
                endOffsets.entrySet().stream()
                        .collect(Collectors.toMap(
                                entry-> entry.getKey(),
                                entry-> {
                                    ObjectName objectName = null;
                                    long lag = entry.getValue()-groupOffset.get(entry.getKey());
                                    try {

                                        objectName = new ObjectName( "com.confluent.consumergroup:type=metrics,groupid="+groupId+",topic="+entry.getKey().topic()+",partition="+entry.getKey().partition());
                                        GroupMetricsMbean groupMetrics = new GroupMetrics(entry.getValue(), groupOffset.get(entry.getKey()), lag);
                                        StandardMBean groupMetricsMbean = new StandardMBean(groupMetrics, GroupMetricsMbean.class);
                                        try {
                                            mBeanServer.registerMBean(groupMetricsMbean, objectName );
                                        } catch (InstanceAlreadyExistsException ex) {
                                            mBeanServer.unregisterMBean(objectName);
                                            mBeanServer.registerMBean(groupMetricsMbean, objectName);
                                        }
                                    } catch (MalformedObjectNameException | InstanceNotFoundException | MBeanRegistrationException | NotCompliantMBeanException | InstanceAlreadyExistsException e) {
                                        e.printStackTrace();
                                    }

                                   return lag;
                                })
                        );
        log.info( "these are the lags for group "+groupId+":"+lags);
        return lags;
    }

    private void getConsumerGroupsData() throws InterruptedException, ExecutionException{
        ListConsumerGroupsResult consumerGroupsResult = adminClient.listConsumerGroups();
        Collection<ConsumerGroupListing> consumerGroupListings = consumerGroupsResult.all().get();
        consumerGroupListings.stream().forEach(consumerGroupListing -> {
            String groupId = consumerGroupListing.groupId();
            System.out.println("getting lag for consumer group "+groupId);
            try {
                getConsumerGroupData(groupId);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }


        });
    }


    @Override
    public void run() {
        try {
            getConsumerGroupsData();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        log.debug("Consumer groups lag data exported at "+new Date());

    }
}
