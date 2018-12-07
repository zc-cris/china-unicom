package com.cris.io;

import com.cris.bean.DataIn;
import com.cris.bean.Val;
import lombok.NoArgsConstructor;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 本地文件读取操作类
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings({"unused", "JavaDoc"})
@NoArgsConstructor
public class LocalFileDataIn implements DataIn {

    private String path;
    private BufferedReader bufferedReader;

    /**
     * 构造方法初始化 io 类
     *
     * @param path 文件路径
     * @throws FileNotFoundException
     */
    public LocalFileDataIn(String path) throws FileNotFoundException {
        this.path = path;
        this.bufferedReader = new BufferedReader(new FileReader(path));
    }

    @Override
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * 读取文本数据，将每行文本转换为 javaBean 对象，转换的具体流程由 javaBean 自身解决
     *
     * @param clazz 传入的 Class 对象，需要使用泛型约束
     * @return 每条文本数据组成的集合
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws IOException
     */
    @Override
    public List<?> read(Class<? extends Val> clazz) throws IllegalAccessException, InstantiationException, IOException {
        String string;
        List<Val> list = new ArrayList<>();
        while ((string = bufferedReader.readLine()) != null) {
            // 将数据转换的方法放到 javaBean 本身去实现
            Val val = clazz.newInstance();
            val.setValue(string);
            list.add(val);
        }
        return list;
    }

    @Override
    public void close() throws IOException {
        if (bufferedReader != null) {
            bufferedReader.close();
        }
    }
}
