package com.cris.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * 通讯录联系人对象
 *
 * @author cris
 * @version 1.0
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class Contact implements Val<String> {

    private String name;
    private String tel;

    @Override
    public String getValue() {
        return this.toString();
    }

    /**
     * javaBean 为自己设置数据的具体转换逻辑
     *
     * @param s 存储的数据
     */
    @Override
    public void setValue(String s) {
        String[] split = s.split("\t");
        this.tel = split[0];
        this.name = split[1];
    }
}
