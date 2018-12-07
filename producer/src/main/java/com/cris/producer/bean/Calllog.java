package com.cris.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * 通话记录封装对象
 *
 * @author cris
 * @version 1.0
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class Calllog implements Val<String> {

    /**
     * 主叫电话
     **/
    private String call1;
    /**
     * 被叫电话
     **/
    private String call2;
    /**
     * 通话时间
     **/
    private String datetime;
    /**
     * 通话时长
     **/
    private String duration;

    @Override
    public String getValue() {
        return this.toString();
    }

    @Override
    public String toString() {
        return this.getCall1() + "\t" + this.getCall2() + "\t" + this.getDatetime() + "\t" + this.getDuration();
    }

    /**
     * 通话记录的数据组装方法
     *
     * @param s 存储的数据
     */
    @Override
    public void setValue(String s) {
        String[] split = s.split(",");
        this.call1 = split[0];
        this.call2 = split[1];
        this.datetime = split[2];
        this.duration = split[3];
    }
}
