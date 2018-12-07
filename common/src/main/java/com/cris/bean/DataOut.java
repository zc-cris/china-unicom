package com.cris.bean;

import java.io.Closeable;
import java.io.IOException;

/**
 * 写数据操作接口
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings({"JavaDoc", "unused"})
public interface DataOut extends Closeable {

    /**
     * 设置文件路径
     *
     * @param path 文件路径
     */
    void setPath(String path);

    /**
     * 向外写数据
     *
     * @param val 待向外写的数据对象
     */
    void write(Val val);

    /**
     * 关闭资源的方法
     *
     * @throws IOException
     */
    @Override
    void close() throws IOException;
}
