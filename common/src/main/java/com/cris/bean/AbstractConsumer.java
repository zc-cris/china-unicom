package com.cris.bean;

import java.io.Closeable;
import java.io.IOException;

/**
 * 消费者抽象类
 *
 * @author cris
 * @version 1.0
 **/
public abstract class AbstractConsumer implements Closeable {

    public abstract void consume() throws IOException;

    @Override
    public void close() {

    }
}
