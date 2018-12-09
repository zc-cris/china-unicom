package com.cris.analysis.tool;

import com.cris.analysis.io.MysqlOutputTextFormat;
import com.cris.analysis.mapper.AnalysisCalllogTextMapper;
import com.cris.analysis.reducer.AnalysisCalllogTextReducer;
import com.cris.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * 实际执行 MapReduce 任务的代码
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings("SpellCheckingInspection")
public class AnalysisCalllogTextTool implements Tool {
    private Configuration configuration;

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(AnalysisCalllogTextTool.class);

        /*指定对 HBase 表数据的扫描器，只扫描 caller 列族有数据的数据，可以减少一半的数据扫描量，大大提高效率*/
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue()));


        /*使用 HBase 提供的工具类快速完成 Mapper 的书写*/
        TableMapReduceUtil.initTableMapperJob(Names.TABLE_NAME.getValue(),
                scan, AnalysisCalllogTextMapper.class, Text.class, Text.class, job);


        /*因为 Reducer 阶段需要和 MySQL 做交互，所以不能使用 HBase 提供的工具类*/
        job.setReducerClass(AnalysisCalllogTextReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        /*OutputFormat*/
        job.setOutputFormatClass(MysqlOutputTextFormat.class);

        // 返回表示任务执行成功或者是失败的数字
        return job.waitForCompletion(true) ? JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }
}
