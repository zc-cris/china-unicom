package com.cris.consumer.bean;

import com.cris.bean.AbstractConsumer;
import com.cris.constant.Names;
import com.cris.consumer.dao.HbaseDao;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * 通话日志的消费者
 *
 * @author cris
 * @version 1.0
 **/
@NoArgsConstructor
public class CalllogConsumer extends AbstractConsumer {

    private HbaseDao hbaseDao;
    private volatile boolean flag = true;

    public CalllogConsumer(HbaseDao hbaseDao) {
        this.hbaseDao = hbaseDao;
    }

    /**
     * 具体的数据消费流程
     */
    @Override
    public void consume() throws IOException {

        // 配置类，读取 properties 配置文件
        Properties prop = new Properties();
        /*主叫记录*/
        Calllog calllog = new Calllog();
        /*被叫记录*/
        Callee callee = new Callee();

        try {
            // 通过应用类加载器来读取配置文件
            prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("kafka_consumer.properties"));

            // 创建 Kafka 的消费者，这里使用高级 API
            KafkaConsumer consumer = new KafkaConsumer(prop);
            // 订阅主题
            consumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));

            // 消费数据，我们这里将从 Kafka 得到的数据打印出来
            while (flag) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
//                    System.out.println(record.value());
                    // 通过 HBaseDao 来往 HBase 插入数据
                    System.out.println("record.value() = " + record.value());
                    hbaseDao.insertValue(record.value());

                    /// 通过使用注解的方法向 HBase 表插入数据，更加方便通用
//                    String value = record.value();
//                    calllog.setValue(value);
//                    callee.setValue(value);
//                    hbaseDao.insertValue(calllog,callee);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭资源
     */
    @Override
    public void close() {
        try {
            hbaseDao.end();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
