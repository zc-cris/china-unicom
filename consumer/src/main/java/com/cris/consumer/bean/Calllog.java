package com.cris.consumer.bean;

import com.cris.api.Column;
import com.cris.api.TableRef;
import com.cris.bean.AbstractHbaseBean;
import com.cris.bean.Val;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * 需要保存数据到 HBase 通话记录表的对象
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings("SpellCheckingInspection")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
@TableRef("cris:calllog")
public class Calllog extends AbstractHbaseBean implements Val<String> {

    @Column(columnFamily = "caller")
    private String call1;
    @Column(columnFamily = "caller")
    private String call2;
    @Column(columnFamily = "caller")
    private String calltime;
    @Column(columnFamily = "caller")
    private String duration;
    /**
     * 判断第一个号码 call1 是主叫还是被叫
     **/
    @Column(columnFamily = "caller")
    private String flg = "1";

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
     * 获取数据的方法
     *
     * @return 存储的数据
     */
    @Override
    public String getValue() {
        return this.toString();
    }

    /**
     * 存储数据的方法
     *
     * @param s 需要存储的数据
     */
    @Override
    public void setValue(String s) {
        String[] split = s.split("\t");
        this.call1 = split[0];
        this.call2 = split[1];
        this.calltime = split[2];
        this.duration = split[3];
    }

    /**
     * 返回指定格式的 rowkey
     *
     * @return rowkey
     */
    @Override
    public String getRowKey() {
        return this.regionNum + "_" + this.call1 + "_" + this.call2 + "_" + this.calltime + "_" + this.duration + "_" + this.flg;
    }
}
