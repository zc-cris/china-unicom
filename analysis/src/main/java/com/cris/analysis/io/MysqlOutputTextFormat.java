package com.cris.analysis.io;

import com.cris.util.JdbcUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Reducer 将会调用这个类的 getRecordWriter 方法得到一个 RecordWriter 来将数据写进 MySQL
 *
 * @author cris
 * @version 1.0
 **/
public class MysqlOutputTextFormat extends OutputFormat<Text, Text> {

    private FileOutputCommitter committer = null;

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    /**
     * 往外写数据的方法
     *
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MysqlRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext job) throws IOException, InterruptedException {
    }

    /**
     * Get the output committer for this output format. This is responsible
     * for ensuring the output is committed correctly.
     *
     * @param context the task context
     * @return an output committer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

    /**
     * 实际执行写数据操作的类
     *
     * @param <Text>
     */
    protected static class MysqlRecordWriter<Text>
            extends RecordWriter<Text, Text> {

        private Connection connection = null;

        public MysqlRecordWriter() {
            this.connection = JdbcUtil.getConnection();
        }

        /**
         * Writes a key/value pair.
         *
         * @param key   the key to write.
         * @param value the value to write.
         * @throws IOException
         */
        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException {
            String sql = "insert into calllog (tellid,dateid,sumcallcount,sumduration) values (?,?,?,?)";
            PreparedStatement preparedStatement = null;
            try {
                preparedStatement = connection.prepareStatement(sql);
                String[] splitValue = value.toString().split("-");

                preparedStatement.setInt(1, 1);
                preparedStatement.setInt(2, 2);
                preparedStatement.setInt(3, Integer.parseInt(splitValue[0]));
                preparedStatement.setInt(4, Integer.parseInt(splitValue[1]));
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (preparedStatement != null) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        /**
         * Close this <code>RecordWriter</code> to future operations.
         *
         * @param context the context of the task
         * @throws IOException
         */
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
