package com.confluent.lag;

import com.confluent.lag.tasks.ExportTask;

import javax.management.MBeanServer;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.Timer;

public class LagExporter {
        public static void main(String args[]) throws IOException {
            Timer t=new Timer();
            Properties kafkaConnectionProperties = new Properties();
            if(args.length==0){
                System.out.println("Needs the path of the properties file");
            }
            kafkaConnectionProperties.load(new FileInputStream(new File(args[0])));

            t.scheduleAtFixedRate(new ExportTask(kafkaConnectionProperties), 0, Long.parseLong(kafkaConnectionProperties.getProperty("interval")));
        }
}
