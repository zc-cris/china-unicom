package com.cris.bean;

import java.io.Closeable;
import java.io.IOException;
import java.text.ParseException;

/**
 * 定义生产者，用于生产我们想要的数据
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings({"JavaDoc", "unused"})
public abstract class AbstractProducer implements Closeable {

    /**
     * 设置生产者的数据输入者
     *
     * @param dataIn
     */
    public abstract void setDataIn(DataIn dataIn);

    /**
     * 设置生产者的数据输出者
     *
     * @param dataOut
     */
    public abstract void setDataOut(DataOut dataOut);


    /**
     * 数据生产流程
     *
     * @throws IllegalAccessException
     * @throws IOException
     * @throws InstantiationException
     * @throws ParseException
     */
    public abstract void produce() throws IllegalAccessException, IOException, InstantiationException, ParseException;

    /**
     * 关闭数据输入资源和数据输出资源
     *
     * @throws IOException
     */
    @Override
    public abstract void close() throws IOException;
}
