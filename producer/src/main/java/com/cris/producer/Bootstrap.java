package com.cris.producer;

import com.cris.bean.AbstractProducer;
import com.cris.bean.DataIn;
import com.cris.bean.DataOut;
import com.cris.bean.LocalFileProducer;
import com.cris.io.LocalFileDataIn;
import com.cris.io.LocalFileDataOut;

import java.io.IOException;
import java.text.ParseException;

/**
 * 生产者模块的启动类
 *
 * @author cris
 * @version 1.0
 **/
public class Bootstrap {

    private static final int ARGS_LENGTH = 2;

    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException, ParseException {
        if (args.length < ARGS_LENGTH) {
            System.out.println("输入的参数不正确，正确格式应该为 java -jar xxx.jar inPath outPath");
            System.exit(1);
        }
        DataIn dataIn = new LocalFileDataIn(args[0]);
        DataOut dataOut = new LocalFileDataOut(args[1]);

        AbstractProducer producer = new LocalFileProducer(dataIn, dataOut);
        producer.produce();
        producer.close();
    }
}
