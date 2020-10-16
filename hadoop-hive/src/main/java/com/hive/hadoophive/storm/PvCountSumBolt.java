package com.hive.hadoophive.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PvCountSumBolt implements IRichBolt {

    private OutputCollector collector;
    private HashMap<Long, Integer> hashmap=new HashMap<>();

    @Override
    public void cleanup() {

    }

    @Override
    public void execute(Tuple input) {
        //1.获取数据
        Long threadId = input.getLongByField("threadid");
        Integer pvnum = input.getIntegerByField("pvnum");

        //2.创建集合 存储 (threadid,pvnum)
        hashmap.put(threadId, pvnum);

        //3.累加求和(拿到集合中所有value值)
        Iterator<Integer> iterator = hashmap.values().iterator();

        //4.清空之前的数据
        int sum=0;
        while(iterator.hasNext()) {
            sum+=iterator.next();
        }

        System.err.println(Thread.currentThread().getName() + "总访问量为->" + sum);
    }

    @Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}
