package com.hive.hadoophive.Hbase;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Component
public class HbaseUtil {

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @Autowired
    private Admin hBaseAdmin;

    /**
     * 建表
     * @param tableName
     * @param family
     */
    public void createTable(String tableName,String[] family){
        TableName table = TableName.valueOf(tableName);
        try {
            if(!hBaseAdmin.tableExists(table)){
                HTableDescriptor tableDescriptor = new HTableDescriptor(table);
                for(int i=0; i<family.length;i++){
                    tableDescriptor.addFamily(new HColumnDescriptor(family[i]));
                }
                hBaseAdmin.createTable(tableDescriptor);
            }else{
                System.out.println("===exists===");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Object execute(HQuery hQuery){
        if(StringUtils.isBlank(String.valueOf(hQuery.getRow())) || hQuery.getColumns().isEmpty()){
            return null;
        }
        return hbaseTemplate.execute(hQuery.getTable(), new TableCallback<Object>() {

            @SuppressWarnings("deprecation")
            @Override
            public Object doInTable(HTableInterface hTableInterface) throws Throwable {
                try{
                    byte[] rowKey = String.valueOf(hQuery.getRow()).getBytes();
                    Put put = new Put(rowKey);
                    for(HBaseColumn column: hQuery.getColumns()){
                        put.addColumn(Bytes.toBytes(column.getFamily()),Bytes.toBytes(column.getQualifier()),Bytes.toBytes(column.getValue()));
                    }
                    hTableInterface.put(put);
                }catch (Exception e){
                    e.printStackTrace();
                }
                return null;
            }
        });
    }

    /**
     *
     * @param hQuery
     * @return
     */
    public Object bufferInsert(HQuery hQuery) {
        return hbaseTemplate.execute(hQuery.getTable(), new TableCallback<Object>() {

            @SuppressWarnings("deprecation")
            @Override
            public Object doInTable(HTableInterface hTableInterface) throws Throwable {
                hTableInterface.setAutoFlush(false);
                //设置缓冲区大小
                hTableInterface.setWriteBufferSize(10 * 1024 * 1024);
                try {
                    for (HBaseColumn column : hQuery.getColumns()) {
                        byte[] rowKey = String.valueOf(hQuery.getRow()).getBytes();
                        Put put = new Put(rowKey);
                        put.addColumn(Bytes.toBytes(column.getFamily()), Bytes.toBytes(column.getQualifier()), Bytes.toBytes(column.getValue()));
                        hTableInterface.put(put);
                    }
                    //刷新缓冲区
                    hTableInterface.flushCommits();
                    hTableInterface.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        });
    }

    public <T> T get(HQuery hQuery,Class<T> c){
        if(StringUtils.isBlank(String.valueOf(hQuery.getRow())) || (StringUtils.isBlank(String.valueOf(hQuery.getTable())))){
            return null;
        }

        return hbaseTemplate.get(hQuery.getTable(), String.valueOf(hQuery.getRow()), new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int i) throws Exception {
                List<Cell> cellList = result.listCells();
                T item = c.newInstance();
                JSONObject json = new JSONObject();
                if(cellList != null && cellList.size() >0){
                    for(Cell cell: cellList){
                        json.put(Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),
                                cell.getQualifierLength()),Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
                    }
                }else {
                    return null;
                }
                System.out.println(json.toJSONString());
                item = JSONObject.parseObject(json.toJSONString(),c);
                return item;
            }
        });
    }

    public <T> List<T> find(HQuery hQuery,Class<T> c){
        hQuery.getScan().setCacheBlocks(false);
        hQuery.getScan().setCaching(2000);
        return hbaseTemplate.find(hQuery.getTable(), hQuery.getScan(), new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int i) throws Exception {
                List<Cell> list = result.listCells();
                JSONObject json = new JSONObject();
                T item = c.newInstance();
                if(list != null && list.size() >0){
                    for(Cell cell : list){
                        String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                        String quali = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                        if(value.startsWith("[")){
                            json.put(quali, JSONArray.parseArray(value));
                        }else{
                            json.put(quali,value);
                        }
                    }
                }
                item = JSONObject.parseObject(json.toJSONString(),c);
                return item;
            }
        });
    }

    /**
     * 删除数据
     * @param hQuery
     */
    public void delete(HQuery hQuery){
        hbaseTemplate.delete(hQuery.getTable(),String.valueOf(hQuery.getRow()),hQuery.getFamily());
    }

    /**
     * 删除表
     * @param table
     */
    public void deleteTable(String table){
        TableName name = TableName.valueOf(table);
        try {
            hBaseAdmin.disableTable(name);
            hBaseAdmin.deleteTable(name);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
