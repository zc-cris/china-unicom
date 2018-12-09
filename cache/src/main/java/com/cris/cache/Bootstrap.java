package com.cris.cache;

import com.cris.util.JdbcUtil;

import java.sql.Connection;

/**
 * 启动我们的 Redis 缓存服务器，向 Redis 增加 MySQL 的映射数据
 *
 * @author cris
 * @version 1.0
 **/
public class Bootstrap {
    public static void main(String[] args) {


    }

    public static void addMysqlCache() {
        Connection connection = JdbcUtil.getConnection();

    }
}
