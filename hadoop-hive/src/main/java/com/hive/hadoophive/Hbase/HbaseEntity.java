package com.hive.hadoophive.Hbase;

import lombok.Data;

@Data
public class HbaseEntity {

    private int id;
    private String name;
    private String regionCode;
    private String address;
    private String memberFamily;
    private String enName;
}
