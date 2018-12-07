package com.cris.util;

import java.text.DecimalFormat;

/**
 * 生成指定格式数字的工具类
 *
 * @author cris
 * @version 1.0
 **/
public class NumberUtil {

    public static String format(int num, int length) {
        StringBuilder stringBuffer = new StringBuilder();
        for (int i = 0; i < length; i++) {
            stringBuffer.append("0");
        }
        /*对数字进行指定位数的填充，数字位数大于指定位数，不做改变；数字位数小于指定位数，从前面使用 0 填充, 填充的数字任意，不一定要为0
         * 视业务而定*/
        DecimalFormat decimalFormat = new DecimalFormat(stringBuffer.toString());
        return decimalFormat.format(num);
    }

    public static void main(String[] args) {
        String format = format(123, 5);
        String format1 = format(123, 2);
        System.out.println("format = " + format);
        System.out.println("format1 = " + format1);
    }
}
