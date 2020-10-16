package com.hive.hadoophive;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hive.hadoophive.Hbase.HBaseService;
import com.hive.hadoophive.Hbase.HbaseEntity;
import com.hive.hadoophive.Hbase.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hive.metastore.hbase.HBaseUtils;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


@SpringBootTest
public class HadoopHiveApplicationTests {

    @Autowired
    JdbcTemplate hiveJdbcTemplate;

    @Test
    void contextLoads() {
        String sql = "select id,name, gender,age from user_test";
        List<Map<String,Object>> list = hiveJdbcTemplate.queryForList(sql);
        System.out.println(list.size());
    }

    @Test
    public void w() throws IOException {
        JSONArray array = new JSONArray();
        for(int i=0;i<1000000;i++){
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("id",i);
            jsonObject.put("name","test_"+i);
            jsonObject.put("gender","male");
            jsonObject.put("age", new Random().nextInt(100));
            array.add(jsonObject);
        }
        String path = "E:\\a.txt";
        File bfile = new File(path);
        if (!bfile.getParentFile().exists()) {
            bfile.getParentFile().mkdirs();
        }
        /*if (bfile.exists()) {
            bfile.delete();
        }*/
        java.io.Writer ow = new FileWriter(path,true);

        for(int i=0;i< array.size();i++){
            JSONObject jsonObject = array.getJSONObject(i);
            Iterator iterator = jsonObject.entrySet().iterator();
            StringBuilder builder = new StringBuilder();
            while (iterator.hasNext()){
                Map.Entry entry = (Map.Entry) iterator.next();
                builder.append(entry.getValue()+",");
            }

            //builder.append("\n");
            ow.write(builder.toString().substring(0,builder.length()-1)+"\n");
        }

        ow.close();
    }

    @Test
    public void orc() throws Exception {
        Path testFilePath = new Path("E:\\test.orc");
        Configuration conf = new Configuration();
        TypeDescription schema = TypeDescription.fromString("struct<id:int,name:String,gender:String,age:int>");
        Writer writer = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf).setSchema(schema).compress(CompressionKind.SNAPPY));
        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector first = (LongColumnVector) batch.cols[0];
        BytesColumnVector second = (BytesColumnVector) batch.cols[1];
        //StringColumnVector second = (LongColumnVector) batch.cols[1];
        BytesColumnVector third = (BytesColumnVector) batch.cols[2];
        LongColumnVector fourth = (LongColumnVector) batch.cols[3];

        final int BATCH_SIZE = batch.getMaxSize();
        // add 1500 rows to file
        for (int r = 0; r < 15000000; ++r) {
            int row = batch.size++;
            first.vector[row] = r;
            second.vector[row] = ("test_"+r).getBytes();
            third.vector[row] = "male".getBytes();
            fourth.vector[row] = new Random().nextInt(100);
            if (row == BATCH_SIZE - 1) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();
    }

    @Test
    public void restore(){
        long start = System.currentTimeMillis();
        String sql = "insert into user_test_orc partition (ages =65) select * from user_test";
        hiveJdbcTemplate.execute(sql);
        System.out.println(System.currentTimeMillis() -start);
    }


    @Autowired
    HbaseUtil hbaseUtil;

    @Test
    public void Hbase(){
        String[] familys = {"base","extends"};
        TableName tableName = TableName.valueOf("bus_receiver");
        hbaseUtil.createTable("bus_receiver",familys);
    }

    @Autowired
    HBaseService hBaseService;

    @Test
    public void hi(){
        long start = System.currentTimeMillis();
        List<HbaseEntity> list = new ArrayList<>();
        for(int i=1;i<1000000;i++) {
            HbaseEntity entity = new HbaseEntity();
            entity.setId(i);
            entity.setAddress("a_"+i);
            entity.setName("n_"+i);
            entity.setRegionCode("rc_"+i);
            entity.setMemberFamily("mf_"+i);
            entity.setEnName("en_"+i);
            list.add(entity);
        }
        hBaseService.batchSave(list);
        System.out.println(System.currentTimeMillis()-start);
    }

    @Test
    public void hg(){
        HbaseEntity entity = hBaseService.get(1);
        System.out.println(entity.getAddress());
    }

    @Test
    public void hd(){
        hBaseService.delete(1);

    }

    @Test
    public void hda(){
        hBaseService.deleteAll("bus_receiver");

    }
}
