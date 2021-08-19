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
    private Map<String,AdminClient> adminClients = null;
    private Map<String, KafkaConsumer>  consumers = null;

    public ExportTask(Properties kafkaConnectionProperties) throws IOException {
        Enumeration propertyNames =  kafkaConnectionProperties.propertyNames();
        Map<String, Properties>  propertiesMap = new HashMap<>();
        while (propertyNames.hasMoreElements()) {
            String property = (String) propertyNames.nextElement();
            String clusterId = property.substring(0, property.indexOf('.'));
            Properties properties = propertiesMap.get(clusterId);
            if(properties==null) {
                properties = new Properties();
                properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
                properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
                propertiesMap.put(clusterId, properties);
            }
            properties.put(property.substring(property.indexOf('.')+1),kafkaConnectionProperties.get(property));
        }
        adminClients =propertiesMap.entrySet().stream().collect(Collectors.toMap(
                entry-> entry.getKey(),
                entry-> AdminClient.create(entry.getValue())
        ));
        consumers =propertiesMap.entrySet().stream().collect(Collectors.toMap(
                entry-> entry.getKey(),
                entry-> new KafkaConsumer<>(entry.getValue())
        ));

    }

    private Map<TopicPartition, Long> getConsumerGroupData(String clusterId, String groupId, AdminClient adminClient, KafkaConsumer consumer)  throws InterruptedException, ExecutionException{
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

                                        objectName = new ObjectName( "com.confluent.consumergroup:type=metrics,clusterid="+clusterId+",groupid="+groupId+",topic="+entry.getKey().topic()+",partition="+entry.getKey().partition());
                                        GroupMetricsMbean groupMetrics = new GroupMetrics( entry.getValue(), groupOffset.get(entry.getKey()), lag);
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

    private void getConsumerGroupsData(String clusterId, AdminClient adminClient, KafkaConsumer consumer) throws ExecutionException, InterruptedException {
        ListConsumerGroupsResult consumerGroupsResult = adminClient.listConsumerGroups();
        Collection<ConsumerGroupListing> consumerGroupListings = consumerGroupsResult.all().get();
        consumerGroupListings.stream().forEach(consumerGroupListing -> {
            String groupId = consumerGroupListing.groupId();
            System.out.println("getting lag for consumer group "+groupId+", cluster: "+clusterId);
            try {
                getConsumerGroupData(clusterId, groupId, adminClient, consumer);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }


        });
    }


    @Override
    public void run() {

        adminClients.entrySet().forEach(adminClientEntry -> {
            try {
                getConsumerGroupsData(adminClientEntry.getKey(), adminClientEntry.getValue(), consumers.get(adminClientEntry.getKey()));
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });


        log.debug("Consumer groups lag data exported at "+new Date());

    }
}
