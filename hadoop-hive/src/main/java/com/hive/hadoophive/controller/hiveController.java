package com.hive.hadoophive.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

@RestController
@RequestMapping("/hive2")
public class hiveController {

    @Autowired
    JdbcTemplate hiveJdbcTemplate;

    @GetMapping("/t")
    public List<Map<String,Object>> t(){
        long start = System.currentTimeMillis();
        String sql = "select id,name, gender,age from user_test limit 100";
        List<Map<String,Object>> list = hiveJdbcTemplate.queryForList(sql);
        System.out.println(System.currentTimeMillis() -start);
        return list;
    }


    @GetMapping("/g")
    public List<Map<String,Object>> g(){
        long start = System.currentTimeMillis();
        String sql = "select count(age) from user_test_orc ";
        List<Map<String,Object>> list = hiveJdbcTemplate.queryForList(sql);
        System.out.println(System.currentTimeMillis() -start);
        return list;
    }

    @GetMapping("/i")
    public long i(){
        long start = System.currentTimeMillis();
        String sql = "insert into user_test values(3,'test3','female',3)";
        hiveJdbcTemplate.execute(sql);
        return System.currentTimeMillis() -start;
    }

    @GetMapping("/x")
    public long txt(){
        long start = System.currentTimeMillis();
        String sql = "LOAD DATA LOCAL INPATH '/home/test/sample.txt'" + "OVERWRITE INTO TABLE user_test";
        hiveJdbcTemplate.execute(sql);
        return System.currentTimeMillis() -start;
    }

    @GetMapping("/c")
    public long c(){
        long start = System.currentTimeMillis();
        String sql = "select count(*) from user_test";
        int i = hiveJdbcTemplate.queryForObject(sql,Integer.TYPE);
        System.out.println(System.currentTimeMillis() -start);
        return i;
    }

    public void w() throws IOException {
        JSONArray array = new JSONArray();
        for(int i=0;i<100;i++){
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("id",i);
            jsonObject.put("name","test_"+i);
            jsonObject.put("gender","male");
            jsonObject.put("age", new Random().nextInt(100));
            array.add(jsonObject);
        }
        String path = "E:\\a.txt";
        File bfile = new File("");
        if (!bfile.getParentFile().exists()) {
            bfile.getParentFile().mkdirs();
        }
        if (bfile.exists()) {
            bfile.delete();
        }
        Writer ow = new FileWriter("",true);

        for(int i=0;i< array.size();i++){
            JSONObject jsonObject = array.getJSONObject(i);
            Iterator iterator = jsonObject.entrySet().iterator();
            StringBuilder builder = new StringBuilder();
            while (iterator.hasNext()){
                Map.Entry entry = (Map.Entry) iterator.next();
                builder.append(entry.getValue()+",");
            }
            builder.append("\n");
            ow.write(builder.toString());
        }

        ow.close();
    }
}
