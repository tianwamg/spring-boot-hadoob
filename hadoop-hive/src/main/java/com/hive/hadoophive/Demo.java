package com.hive.hadoophive;

import java.sql.*;

public class Demo {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        // Register driver and create driver instance
        Class.forName(driverName);

        // get connection
        Connection con = DriverManager.getConnection("jdbc:hive2://192.168.2.3:10000/hivedb", "roothost"  , "3@mz32mu7JhQ$ATu");

        // create statement
        Statement stmt = con.createStatement();

        // execute statement
        ResultSet res = stmt.executeQuery("SELECT * FROM user_test");

        System.out.println("Result:");
        System.out.println(" ID \t Name \t Salary \t Designation \t Dept ");

        while (res.next()) {
            System.out.println(res.getInt(1) + " " + res.getString(2) + " " + res.getString(3) + " " + res.getInt(4) );
        }
        con.close();
    }

}
