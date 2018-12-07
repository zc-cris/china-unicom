package com.cris.bean;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * 读取数据操作接口
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings("JavaDoc")
public interface DataIn extends Closeable {

    /**
     * 设置文件路径
     *
     * @param path 文件路径
     */
    @SuppressWarnings("unused")
    void setPath(String path);

    /**
     * 读取文件数据转换为指定类型的集合数据
     *
     * @param clazz 泛型约束的数据
     * @return 指定数据类型的集合
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws IOException
     */
    List<?> read(Class<? extends Val> clazz) throws IllegalAccessException, InstantiationException, IOException;

    /**
     * 关闭资源
     *
     * @throws IOException
     */
    @Override
    void close() throws IOException;
}
