package com.cris.bean;


import com.cris.util.DatetimeUtil;
import com.cris.util.NumberUtil;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Random;

/**
 * 我们自定义的读取本地数据的生产类
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings({"SpellCheckingInspection", "JavaDoc"})
@NoArgsConstructor
public class LocalFileProducer extends AbstractProducer {

    /**
     * 数据输入操作类
     **/
    private DataIn dataIn;
    /**
     * 数据输出操作类
     **/
    private DataOut dataOut;
    /**
     * 内存可见性的标志
     **/
    private volatile boolean flag = true;

    public LocalFileProducer(DataIn dataIn, DataOut dataOut) {
        this.dataIn = dataIn;
        this.dataOut = dataOut;
    }

    @Override
    public void setDataIn(DataIn dataIn) {
        this.dataIn = dataIn;
    }

    @Override
    public void setDataOut(DataOut dataOut) {
        this.dataOut = dataOut;
    }

    /**
     * 实际的数据生产逻辑方法
     *
     * @throws IllegalAccessException
     * @throws IOException
     * @throws InstantiationException
     * @throws ParseException
     */
    @Override
    public void produce() throws IllegalAccessException, IOException, InstantiationException, ParseException {
        // 成功拿到通讯录的用户信息并封装完毕 ok
        List<?> read = dataIn.read(Contact.class);
        StringBuilder stringBuilder1 = new StringBuilder();
        StringBuilder stringBuilder2 = new StringBuilder();
        while (flag) {
            // 获取两个不同的电话号码 ok
            int[] indexs = getTwoDiffrentIndex(read.size());
            Contact contact1 = (Contact) read.get(indexs[0]);
            Contact contact2 = (Contact) read.get(indexs[1]);
            stringBuilder1.append(contact1.getTel());
            stringBuilder2.append(contact2.getTel());

            // 通话时长的格式化 ok [0001,3000]
            int duration = new Random().nextInt(3000) + 1;
            String formatDuration = NumberUtil.format(duration, 4);

            // 获取到随机的通话时间
            String time = DatetimeUtil.parse("20180101000000", "20190101000000", "yyyyMMddHHmmss");

            String value = stringBuilder1.toString() + "," + stringBuilder2.toString() + "," + time + "," + formatDuration;

            // 组装数据
            Calllog calllog = new Calllog();
            calllog.setValue(value);
            dataOut.write(calllog);

            // 一秒钟生产两条数据
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            stringBuilder1.delete(0, stringBuilder1.length());
            stringBuilder2.delete(0, stringBuilder2.length());
        }
    }

    /**
     * 获取到两个不同的 index 索引值
     *
     * @param size 通讯录数据的总条数
     * @return
     */
    private int[] getTwoDiffrentIndex(int size) {
        int[] indexs = new int[2];
        while (true) {
            int index1 = new Random().nextInt(size);
            int index2 = new Random().nextInt(size);
            if (index1 != index2) {
                indexs[0] = index1;
                indexs[1] = index2;
                break;
            }
        }
        return indexs;
    }

    /**
     * 关闭资源，这里实际是通过数据操作类来关闭资源
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        this.dataIn.close();
        this.dataOut.close();
    }
}
