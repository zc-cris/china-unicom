package com.cris.io;

import com.cris.bean.DataIn;
import com.cris.bean.DataOut;
import com.cris.bean.Val;
import lombok.NoArgsConstructor;

import java.io.*;
import java.util.List;

/**
 * 本地文件输出操作类
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings("unused")
@NoArgsConstructor
public class LocalFileDataOut implements DataOut {

    private String path;
    private PrintWriter printWriter;

    public LocalFileDataOut(String path) throws FileNotFoundException {
        this.path = path;
        printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path)), false);
    }

    @Override
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * 通过 val 的 toString 方法向外写数据
     *
     * @param val 需要向外写的数据对象
     */
    @Override
    public void write(Val val) {
        printWriter.println(val);
        printWriter.flush();
    }

    @Override
    public void close() {
        if (printWriter != null) {
            printWriter.close();
        }
    }
}
