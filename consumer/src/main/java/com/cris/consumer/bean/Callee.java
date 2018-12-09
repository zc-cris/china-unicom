package com.cris.consumer.bean;

import com.cris.api.Column;
import com.cris.api.TableRef;
import com.cris.bean.AbstractHbaseBean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * TODO
 *
 * @author cris
 * @version 1.0
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
@TableRef("cris:calllog")
public class Callee extends AbstractHbaseBean {

    @Column(columnFamily = "callee")
    private String call1;
    @Column(columnFamily = "callee")
    private String call2;
    @Column(columnFamily = "callee")
    private String calltime;
    @Column(columnFamily = "callee")
    private String duration;
    /**
     * 判断第一个号码 call1 是主叫还是被叫
     **/
    @Column(columnFamily = "callee")
    private String flg = "0";

    /**
     * 插入 HBase 表需要的regionNum
     **/
    private int regionNum;

    /**
     * 这个对象需要有专门的设置 regionNum 的方法
     *
     * @param regionNum
     */
    @Override
    public void setRegionNum(int regionNum) {
        this.regionNum = regionNum;
    }

    /**
     * 获取 rowkey 的方法
     *
     * @return
     */
    @Override
    public String getRowKey() {
        return this.regionNum + "_" + this.call1 + "_" + this.calltime + "_" + this.call2 + "_" + this.duration + "_" + this.flg;
    }

    @SuppressWarnings("Duplicates")
    public void setValue(String s) {
        String[] split = s.split("\t");
        this.call1 = split[0];
        this.call2 = split[1];
        this.calltime = split[2];
        this.duration = split[3];
    }

}
