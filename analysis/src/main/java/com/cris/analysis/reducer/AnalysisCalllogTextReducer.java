package com.cris.analysis.reducer;

import com.cris.util.DatetimeUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义的 Reducer ，需要和 MySQL 交互
 *
 * @author cris
 * @version 1.0
 **/
public class AnalysisCalllogTextReducer extends Reducer<Text, Text, Text, Text> {

    private Text value = new Text();
    private StringBuilder stringBuilder = new StringBuilder();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int totalDuration = 0;
        int totalCount = 0;
        for (Text text : values) {
            totalDuration += Integer.parseInt(text.toString());
            ++totalCount;
        }
        String v = DatetimeUtil.StringTool.packString(stringBuilder, "-", String.valueOf(totalCount), String.valueOf(totalDuration));

        this.value.set(v);
        context.write(key, this.value);
    }
}
