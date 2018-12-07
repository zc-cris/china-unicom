package com.cris.bean;

/**
 * 专门用来向 HBase 表中插入数据的对象
 *
 * @author cris
 * @version 1.0
 **/
public abstract class AbstractHbaseBean {

    /**
     * 这个对象需要有专门的设置 regionNum 的方法
     *
     * @param regionNum
     */
    protected abstract void setRegionNum(int regionNum);

    /**
     * 获取 rowkey 的方法
     *
     * @return
     */
    public abstract String getRowKey();
}
