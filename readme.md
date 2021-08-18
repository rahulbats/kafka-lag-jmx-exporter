## kafka lag JMX exporter
* The only way to monitor jmx lag is through consumer group cli
* The client consumer JMX `record-lag` does not show the true lag, but the lag of the fetched offset instead of the comitted offset
* This app runs a scheduled job which extracts the end and current offsets and the lag and publishes them as JMX Mbeans which you can use in your exporter.

### How to run
1. build the jar using `mvn clean package`
2. to use jconsole to view the metrics.
   * Run with the jmx parameters enabled like this `java  -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false  -jar target/kafka-lag-jmx-exporter-1.0-SNAPSHOT-jar-with-dependencies.jar ./main/resources/kafka.properties`
   * Open `jconsole` in another terminal
3. to use a exporter to create a http endpoint. 
   * Download [prometheus exporter jar](https://mvnrepository.com/artifact/io.prometheus.jmx/jmx_prometheus_javaagent)
   * Run with the included `jmx-exporter.yml` like this ` java -javaagent:/Users/rahul/jmx_prometheus_javaagent-0.16.1.jar=8081:jmx-exporter.yml  -jar target/kafka-lag-jmx-exporter-1.0-SNAPSHOT-jar-with-dependencies.jar ./main/resources/kafka.properties`
    

### How are the MBeans arranged 
1. The Mbean object name is `com.confluent.consumergroup:type=metrics,groupid=([-.w]+),topic=([-.w]+),partition=([0-9]+)`
2. It has 3 attributes 
   * current - current offset of the consumer 
   * end - ending offset of the topic 
   * lag - lag of the consumer derived from (end - current) 