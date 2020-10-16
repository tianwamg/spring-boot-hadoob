package com.hive.hadoophive.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

public class PvCountSpout implements IRichSpout {

    private SpoutOutputCollector collector;
    private BufferedReader br;
    private String line;

    @Override
    public void nextTuple() {
        //发送读取的数据的每一行
        try {
            while((line=br.readLine()) != null) {
                //发送数据到splitbolt
                collector.emit(new Values(line));
                //设置延迟
                Thread.sleep(500);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    @Override
    public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
        this.collector=collector;

        //读取文件
        try {
            br=new BufferedReader(new InputStreamReader(new FileInputStream("f:/temp/storm实时统计访问量/weblog.log")));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //别关流！！！！
//		finally {
//			if(br!=null) {
//				try {
//					br.close();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//		}

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //声明
        declarer.declare(new Fields("logs"));
    }

    //处理tuple成功 回调的方法。就像kafka的那个callback回调函数，还有zookeeper中的回调函数 process
    @Override
    public void ack(Object arg0) {
        // TODO Auto-generated method stub

    }

    //如果spout在失效的模式中 调用此方法来激活，和在Linux中那个命令 storm activate [拓扑名称] 一样的效果
    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }

    //在spout程序关闭前执行 不能保证一定被执行 kill -9 是不执行 storm kill 是不执行
    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    //在spout失效期间，nextTuple不会被调用 和在Linux中那个命令 storm deactivate [拓扑名称] 一样的效果
    @Override
    public void deactivate() {
        // TODO Auto-generated method stub

    }

    //处理tuple失败回调的方法
    @Override
    public void fail(Object arg0) {
        // TODO Auto-generated method stub

    }

    //配置
    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}
