package com.cris.analysis.mapper;

import com.cris.util.DatetimeUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * 自定义的 MapReduce 中的 Mapper ，继承了 HBase 提供的 TableMapper
 *
 * @author cris
 * @version 1.0
 **/
public class AnalysisCalllogTextMapper extends TableMapper<Text, Text> {

    /*主叫用户的输出数据（根据年，年月，年月日进行区分）*/

    private Text callerTextYear = new Text();
    private Text callerTextYearMonth = new Text();
    private Text callerTextYearMonthDay = new Text();

    /*被叫用户的输出数据（根据年，年月，年月日进行区分）*/

    private Text calleeTextYear = new Text();
    private Text calleeTextYearMonth = new Text();
    private Text calleeTextYearMonthDay = new Text();

    /*通话时长*/

    private Text durationText = new Text();

    private StringBuilder stringBuilder = new StringBuilder();

    /**
     * 将每条数据根据时间日期格式映射成两组数据（主叫和被叫）
     *
     * @param key     实际上就是 HBase 每条数据的 rowkey
     * @param value   每条数据对应的结果集（列族数据，因为有 Scan 的设置，这里只会进入 caller 列族有数据的每条数据）
     * @param context 上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        calllogMapStrategy(key, context);
    }

    /**
     * 具体的对通话记录进行处理的方法
     *
     * @param key     rowkey
     * @param context 上下文
     * @throws IOException
     * @throws InterruptedException
     */
    private void calllogMapStrategy(ImmutableBytesWritable key, Context context) throws IOException, InterruptedException {
        String rowkey = Bytes.toString(key.get());

        /*rowkey 数据格式为 分区号_caller_calltime_callee_duration_主叫标识*/
        // 5_19154926260_20180824013059_15647679901_2609_1
        String[] split = rowkey.split("_");
        String caller = split[1];
        String callee = split[3];
        String calltime = split[2];
        String duration = split[4];

        String year = calltime.substring(0, 4);
        String month = calltime.substring(0, 6);
        String date = calltime.substring(0, 8);

        durationText.set(duration);

        callerTextYear.set(DatetimeUtil.StringTool.packString(stringBuilder, "-", caller, year));
        callerTextYearMonth.set(DatetimeUtil.StringTool.packString(stringBuilder, "-", caller, year, month));
        callerTextYearMonthDay.set(DatetimeUtil.StringTool.packString(stringBuilder, "-", caller, year, month));
        /*这里的 Mapper 阶段需要对每一条数据映射成两组（主叫和被叫）和 六条数据（时间区分），主叫和被叫的数据格式需要的数据可以通过每条数据的 rowkey 就可以获取到*/

        calleeTextYear.set(DatetimeUtil.StringTool.packString(stringBuilder, "-", callee, year));
        calleeTextYearMonth.set(DatetimeUtil.StringTool.packString(stringBuilder, "-", callee, year, month));
        calleeTextYearMonthDay.set(DatetimeUtil.StringTool.packString(stringBuilder, "-", callee, month, date));

        context.write(callerTextYear, durationText);
        context.write(callerTextYearMonth, durationText);
        context.write(callerTextYearMonthDay, durationText);

        context.write(calleeTextYear, durationText);
        context.write(calleeTextYearMonth, durationText);
        context.write(calleeTextYearMonthDay, durationText);
    }
}
