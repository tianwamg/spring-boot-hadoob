package com.hive.hadoophive.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class PvCountDriver {
    public static void main(String[] args) {
        // 1.创建拓扑
        TopologyBuilder builder = new TopologyBuilder();

        // 2.指定设置
        builder.setSpout("pvcountspout", new PvCountSpout(), 1);
        builder.setBolt("pvsplitbolt", new PvCountSplitBolt(), 6).setNumTasks(4).fieldsGrouping("pvcountspout",
                new Fields("logs"));
        builder.setBolt("pvcountbolt", new PvCountSumBolt(), 1).fieldsGrouping("pvsplitbolt",
                new Fields("threadid", "pvnum"));

        // 3.创建配置信息
        Config conf = new Config();
        conf.setNumWorkers(2);

        // 4.提交任务
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("pvcounttopology", conf, builder.createTopology());
    }

}
