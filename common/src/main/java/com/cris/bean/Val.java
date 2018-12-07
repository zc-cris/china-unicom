package com.cris.bean;

/**
 * 值对象，用于存储数据和对外提供数据
 *
 * @author cris
 * @version 1.0
 **/
public interface Val<T> {

    /**
     * 获取数据的方法
     *
     * @return 存储的数据
     */
    @SuppressWarnings("unused")
    T getValue();

    /**
     * 存储数据的方法
     *
     * @param t 需要存储的数据
     */
    void setValue(T t);
}
