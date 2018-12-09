package com.cris.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 获取数据库的连接
 *
 * @author cris
 * @version 1.0
 **/
public class JdbcUtil {

    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://hadoop101:3306/cu?useUnicode=true&characterEncoding=UTF-8";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "123456";

    static {
        try {
            Class.forName(MYSQL_DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
