package com.cris.constant;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * 使用另外一种读取配置文件的方式定义常量，相比与枚举，静态变量更加灵活
 *
 * @author cris
 * @version 1.0
 **/
public class ConfigConstant {

    public static final Map<String, String> CONFIG_MAP = new HashMap<>();

    static {
        ResourceBundle resourceBundle = ResourceBundle.getBundle("cu");
        Enumeration<String> keys = resourceBundle.getKeys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            String value = resourceBundle.getString(key);
            CONFIG_MAP.put(key, value);
        }
    }

    public static void main(String[] args) {
        String s = ConfigConstant.CONFIG_MAP.get("calllog.regionCount");
        System.out.println(s);
    }
}
