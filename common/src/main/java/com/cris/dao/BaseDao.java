package com.cris.dao;

import java.io.IOException;

/**
 * 封装了数据访问最基本的功能
 *
 * @author cris
 * @version 1.0
 **/
public abstract class BaseDao {


    /**
     * 执行流程的开始
     */
    protected abstract void start();

    /**
     * 执行流程的结束
     */
    protected abstract void end() throws IOException;


}
