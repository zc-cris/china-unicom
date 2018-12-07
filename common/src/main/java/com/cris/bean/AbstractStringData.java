package com.cris.bean;

/**
 * 存储数据的对象，内容为 String 类型
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings("unused")
public abstract class AbstractStringData implements Val<String> {

    private String content;

    @Override
    public String getValue() {
        return content;
    }

    @Override
    public void setValue(String value) {
        this.content = value;
    }
}
