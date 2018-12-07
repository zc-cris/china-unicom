package com.cris.consumer.dao;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.ResourceBundle;

/**
 * 使用最标准的 Junit 进行自测，只有自测过的代码才能提交！！！
 */
class HbaseDaoTest {

    /**
     * 测试分区号的计算 ok
     */
    @Test
    void genRegionNumTest() throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        //PrivateMethod pm = new PrivateMethod();
        //获取目标类的class对象
        Class<HbaseDao> clazz = HbaseDao.class;

        //获取目标类的实例
        Object instance = clazz.newInstance();

        //getDeclaredMethod（）  可获取 公共、保护、默认（包）访问和私有方法，但不包括继承的方法。
        //getMethod（） 只可获取公共的方法
        Method method = clazz.getDeclaredMethod("genRegionNum", String.class, String.class);

        //值为true时 反射的对象在使用时 让一切已有的访问权限取消
        method.setAccessible(true);

        int result = (int) method.invoke(instance, "138756443410", "20180418080808");
        Assert.assertTrue(result >= 0 && result < 6);
    }

    /**
     * 测试分区键算法 ok
     */
    @Test
    void genSplitKeysTest() throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Class<HbaseDao> clazz = HbaseDao.class;
        HbaseDao hbaseDao = clazz.newInstance();

        Method genSplitKeys = clazz.getDeclaredMethod("genSplitKeys", int.class);
        genSplitKeys.setAccessible(true);

        byte[][] result = (byte[][]) genSplitKeys.invoke(hbaseDao, 6);
        for (byte[] bytes : result) {
            System.out.println("new String(bytes) = " + new String(bytes));
        }
    }

    // 测试根据电话号和年月范围得到 rowkey 以便进行查询
    @Test
    void getStartRowKeyAndStopRowKeyTest() throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Class<HbaseDao> clazz = HbaseDao.class;
        HbaseDao hbaseDao = clazz.newInstance();

        Method method = clazz.getDeclaredMethod("getStartRowKeyAndStopRowKey", String.class, String.class, String.class);
        method.setAccessible(true);

        List<byte[][]> result = (List<byte[][]>) method.invoke(hbaseDao, "17623887389", "201811", "201901");
        for (byte[][] bytes : result) {
            System.out.println("new String(bytes) = " + new String(bytes[0]));
            System.out.println("new String(bytes) = " + new String(bytes[1]));
            System.out.println("---------------------");
        }
    }


    public static void main(String[] args) {
        A a = new B();
        a.setName("cris");
        a.say();        // null
        ResourceBundle cu = ResourceBundle.getBundle("cu");
    }

}
class A {
    private String name;

    public void setName(String name) {
        this.name = name;
    }

    public void say(){
        System.out.println("this.name = " + name);
    }
}
class B extends A{
    private String name;

    public void setName(String name) {
        this.name = name;
    }
}