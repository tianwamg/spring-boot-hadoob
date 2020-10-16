package com.hive.hadoophive.Hbase;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.List;

@Service
public class HBaseService{

    @Autowired
    HbaseUtil hbaseUtil;

    public void createTable(String tablename){
        String[] family= {"base","extends"};
        hbaseUtil.createTable(tablename,family);
    }

    public void save(HbaseEntity entity){
        HQuery hQuery = new HQuery("bus_receiver",String.valueOf(entity.getId()));
        hQuery.getColumns().add(new HBaseColumn("base","name",entity.getName(),entity.getId()));
        hQuery.getColumns().add(new HBaseColumn("base","regionCode",entity.getRegionCode(),entity.getId()));
        hQuery.getColumns().add(new HBaseColumn("extends","address",entity.getAddress(),entity.getId()));
        hQuery.getColumns().add(new HBaseColumn("extends","memberFamily",entity.getMemberFamily(),entity.getId()));
        hQuery.getColumns().add(new HBaseColumn("extends","enName",entity.getEnName(),entity.getId()));
        hbaseUtil.bufferInsert(hQuery);
    }

    public void batchSave(List<HbaseEntity> list){
        HQuery hQuery = new HQuery("bus_receiver");
        for(HbaseEntity entity : list){
            hQuery.getColumns().add(new HBaseColumn("base","name",entity.getName(),entity.getId()));
            hQuery.getColumns().add(new HBaseColumn("base","regionCode",entity.getRegionCode(),entity.getId()));
            hQuery.getColumns().add(new HBaseColumn("extends","address",entity.getAddress(),entity.getId()));
            hQuery.getColumns().add(new HBaseColumn("extends","memberFamily",entity.getMemberFamily(),entity.getId()));
            hQuery.getColumns().add(new HBaseColumn("extends","enName",entity.getEnName(),entity.getId()));
        }
        hbaseUtil.bufferInsert(hQuery);
    }

    public HbaseEntity get(Serializable id){
        HQuery hQuery = new HQuery("bus_receiver",id);
        HbaseEntity entity = hbaseUtil.get(hQuery,HbaseEntity.class);
        return entity;
    }

    public void delete(Serializable id){
        String[] family= {"base","extends"};
        for(String s:family) {
            HQuery hQuery = new HQuery("bus_receiver", id, s);

            hbaseUtil.delete(hQuery);
        }
    }

    public void deleteAll(String table){
        hbaseUtil.deleteTable(table);
    }
}
