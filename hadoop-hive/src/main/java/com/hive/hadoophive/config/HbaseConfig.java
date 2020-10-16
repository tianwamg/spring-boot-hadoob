package com.hive.hadoophive.config;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

import java.io.IOException;

@Configuration
public class HbaseConfig {

    @Value("${hbase.zookeeper.quorum}")
    private String zookeeperQuorum;

    @Value("${hbase.zookeeper.property.clientPort}")
    private String clientPort;

    @Value("${zookeeper.znode.parent}")
    private String znodeParent;


    @Bean("conf")
    public org.apache.hadoop.conf.Configuration conf(){
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("hbase.zookeeper.quorum",zookeeperQuorum);
        conf.set("hbase.zookeeper.property.clientPort",clientPort);
        conf.set("zookeeper.znode.parent",znodeParent);
        return conf;
    }

    @Bean
    public HbaseTemplate hbaseTemplate(org.apache.hadoop.conf.Configuration conf){
        HbaseTemplate template = new HbaseTemplate();
        template.setAutoFlush(true);
        template.setConfiguration(conf);
        return template;
    }

    @Bean("hBaseAdmin")
    public Admin hBaseAdmin(org.apache.hadoop.conf.Configuration conf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        return admin;
    }
}
