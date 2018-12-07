package com.cris.constant;

import com.cris.bean.Val;

/**
 * 定义枚举来存放常量
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings({"unused", "SpellCheckingInspection"})
public enum Names implements Val<String> {

    /**
     * 命名空间
     */
    NAMESPACE("cris"),
    /**
     * Topic 主题
     **/
    TOPIC("cu"),
    /**
     * 默认的列族名字
     **/
    DEFAULT_CF("info"),
    /**
     * 主叫列族名
     **/
    CF_CALLER("caller"),
    /**
     * 被叫列族名
     **/
    CF_CALLEE("callee"),
    /**
     * HBase 通话记录表名
     **/
    TABLE_NAME("cris:calllog");

    private String val;

    Names(String val) {
        this.val = val;
    }

    @Override
    public String getValue() {
        return this.val;
    }

    @Override
    public void setValue(String val) {
        this.val = val;
    }
}
