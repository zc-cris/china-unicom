package com.cris.consumer;

import com.cris.bean.AbstractConsumer;
import com.cris.consumer.bean.CalllogConsumer;
import com.cris.consumer.dao.HbaseDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 启动消费者的程序入口
 * 使用 Kafka 消费 Flume 的数据，最后存储到 HBase 中
 *
 * @author cris
 * @version 1.0
 **/
public class Bootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(String.valueOf(Bootstrap.class)); 

    public static void main(String[] args) {

        // 这里先初始化 HBase 的命名空间和表
        HbaseDao hbaseDao = new HbaseDao();
        try {
            hbaseDao.init();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 创建消费者
        AbstractConsumer consumer = new CalllogConsumer(hbaseDao);
//        AbstractConsumer consumer = new CalllogConsumer();

        // 启动消费者
        try {
            consumer.consume();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            // 关闭资源
            consumer.close();
        }
    }
}
