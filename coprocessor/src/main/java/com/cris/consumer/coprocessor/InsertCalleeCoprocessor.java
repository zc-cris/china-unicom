package com.cris.consumer.coprocessor;

import com.cris.constant.Names;
import com.cris.consumer.bean.Callee;
import com.cris.consumer.dao.HbaseDao;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 使用协处理器帮助我们增加被叫用户记录
 * 1. 创建类
 * 2. 和 HBase 表关联（table 的描述器）
 * 3. 项目打包（关联的包一起上传到 HBase 中）并分发
 *
 * @author cris
 * @version 1.0
 **/
public class InsertCalleeCoprocessor extends BaseRegionObserver {

    private static final Logger LOGGER = LoggerFactory.getLogger(String.valueOf(InsertCalleeCoprocessor.class));

    private final HbaseDao dao = new CoprocessorDao();
    private final Callee callee = new Callee();
    private final StringBuilder stringBuilder = new StringBuilder();
    private static final String CALLER_FLAG = "1";
    private static final int FLAG_INDEX = 5;

    /**
     * 保存主叫用户的数据之后，让 HBase 帮助我们完成被叫用户记录的保存
     *
     * @param e          上下文环境
     * @param put        主叫用户的 put 对象
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        /*组装被叫用户记录的 rowkey:分区号+callee+calltime+caller+duration+被叫标识 */
        String[] split = Bytes.toString(put.getRow()).split("_");
        /*说明是添加 主叫用户记录的时候调用的该方法，则执行添加被叫用户的代码，否则不添加*/
        if (StringUtils.equals(CALLER_FLAG, split[FLAG_INDEX])) {
            stringBuilder.append(String.valueOf(dao.genRegionNum(split[2], split[3]))).append("_").append(split[3]).
                    append("_").append(split[2]).append("_").append(split[1]).append("_").append(split[4]).append("_0");

            Put calleePut = new Put(Bytes.toBytes(stringBuilder.toString()));
            stringBuilder.delete(0, stringBuilder.length());

            // 为 callee 属性赋值!
            callee.setValue(stringBuilder.append(split[3]).append("\t").append(split[2]).append("\t").append(split[1]).
                    append("\t").append(split[4]).toString());
            stringBuilder.delete(0, stringBuilder.length());


            /*初始化 callee 列的信息*/
            dao.initColumns(callee, callee.getClass(), calleePut);

            // 获取到表
            Table table = e.getEnvironment().getTable(TableName.valueOf(Names.TABLE_NAME.getValue()));
            table.put(calleePut);
            table.close();
        }
    }

    /**
     * 使用内部类，继承 HbaseDao
     */
    private static final class CoprocessorDao extends HbaseDao {
    }
}
