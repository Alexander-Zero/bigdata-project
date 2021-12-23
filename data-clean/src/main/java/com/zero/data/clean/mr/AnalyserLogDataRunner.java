package com.zero.data.clean.mr;

import com.zero.data.clean.common.EventLogConstants;
import com.zero.data.clean.common.GlobalConstants;
import com.zero.data.clean.util.TimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


/**
 * @author Alexander Zero
 * @version 1.0.0
 * @date 2021/12/22
 * 启动类
 */
@Slf4j
public class AnalyserLogDataRunner implements Tool {


    private Configuration conf = null;


    /**
     * 启动类
     */
    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(true), new AnalyserLogDataRunner(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Job job = Job.getInstance(conf, "data-clean");

        //处理参数 这里的参数是Parser处理后的参数,将要处理的时间放置进去
        this.processArgs(args);

        job.setJarByClass(AnalyserLogDataRunner.class);
        //设置mapper
        job.setMapperClass(AnalyserLogDataMapper.class);
        //没有key,所以为Null
        job.setMapOutputKeyClass(NullWritable.class);
        //输出为put对象
        job.setMapOutputValueClass(Put.class);

        TableMapReduceUtil.initTableReducerJob(EventLogConstants.TABLE_NAME, null, job, null, null, null, null, false);
        //不需要Reduce阶段
        job.setNumReduceTasks(0);

        //设置文件读取路径
        this.setJobInputPaths(job);


        final boolean b = job.waitForCompletion(true);
        return b ? 1 : 0;
    }

    private void processArgs(String[] args) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if ("-d".equals(args[i])) {
                if (i + 1 < args.length) {
                    date = args[++i];
                    break;
                }
            }
        }

        System.out.println("-----" + date);

        // 要求date格式为: yyyy-MM-dd
        if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
            // date是一个无效时间数据, 默认设置为昨天的数据
            date = TimeUtil.getYesterday();
            System.out.println(date);
        }
        this.conf.set(GlobalConstants.LOG_DATE, date);
    }


    /**
     *  设置读取HDFS的文件路径
     */
    private void setJobInputPaths(Job job) {
        //读取路径应为 /flume/events/2021-12-21

        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            //将参数先解析后放置再conf中
            String date = conf.get(GlobalConstants.LOG_DATE);
            final String pathStr = "/flume/events/" + date;
            Path path = new Path(pathStr);
            if (fs.exists(path)) {
                TextInputFormat.addInputPath(job, path);
            } else {
                log.error("目录不存在");
                System.out.println("文件路径不存在,data: "+pathStr);
            }

        } catch (IOException e) {
            log.error("路径异常");
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    @Override
    public void setConf(Configuration configuration) {
        //要将数据输出到HBase,需整合,具体查看HBase官网

        //1,设置zookeeper地址, 注册发现中心 (7默认配置)
        configuration.set("hbase.zookeeper.quorum", "node02,node03,node04");
        //2,异构平台配置
        configuration.set("mapreduce.app-submission.cross-platform", "true");
        configuration.set("mapreduce.framework.name", "local");

        this.conf = HBaseConfiguration.create(configuration);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
