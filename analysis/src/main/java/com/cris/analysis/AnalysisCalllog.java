package com.cris.analysis;

import com.cris.analysis.tool.AnalysisCalllogTextTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReduce 任务的入口，分析通话记录信息
 *
 * @author cris
 * @version 1.0
 **/
public class AnalysisCalllog {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new AnalysisCalllogTextTool(), args);
    }
}
