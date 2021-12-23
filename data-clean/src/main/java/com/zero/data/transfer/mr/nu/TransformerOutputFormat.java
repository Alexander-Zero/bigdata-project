package com.zero.data.transfer.mr.nu;

import com.zero.data.clean.common.GlobalConstants;
import com.zero.data.clean.common.KpiType;
import com.zero.data.transfer.model.dim.base.BaseDimension;
import com.zero.data.transfer.model.value.BaseStatsValueWritable;
import com.zero.data.transfer.service.IDimensionConverter;
import com.zero.data.transfer.service.impl.DimensionConverterImpl;
import com.zero.data.transfer.util.JdbcManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Alexander Zero
 * @version 1.0.0
 * @date 2021/12/23
 */
@Slf4j
public class TransformerOutputFormat extends OutputFormat<BaseDimension, BaseStatsValueWritable> {
    @Override
    public RecordWriter<BaseDimension, BaseStatsValueWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        //需要JDBC 连接对象
        Connection conn = null;
        IDimensionConverter converter = new DimensionConverterImpl();
        try {
            conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
            //批量提交,效率更高
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            log.error("获取数据库连接失败", e);
            throw new IOException("获取数据库连接失败", e);
        }
        return new  TransformerRecordWriter(conn, conf, converter);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }


    public class TransformerRecordWriter extends RecordWriter<BaseDimension, BaseStatsValueWritable> {
        private Connection conn = null;
        private Configuration conf = null;
        private IDimensionConverter converter = null;
        //提交语句
        private Map<KpiType, PreparedStatement> map = new HashMap<KpiType, PreparedStatement>();
        //模块 => 批量提交条数
        private Map<KpiType, Integer> batch = new HashMap<KpiType, Integer>();

        public TransformerRecordWriter(Connection conn, Configuration conf, IDimensionConverter converter) {
            super();
            this.conn = conn;
            this.conf = conf;
            this.converter = converter;
        }

        @Override
        public void write(BaseDimension key, BaseStatsValueWritable value) throws IOException, InterruptedException {
            if (key == null || value == null) {
                return;
            }
            try {
                KpiType kpi = value.getKpi();
                PreparedStatement pstmt = null;//每一个pstmt对象对应一个sql语句
                int count = 1;//sql语句的批处理，一次执行10
                if (map.get(kpi) == null) {
                    //预处理语句 获取 select xxx from xx where id = ? AND name = ? ;
                    pstmt = this.conn.prepareStatement(conf.get(kpi.name));
                    map.put(kpi, pstmt);
                } else {
                    pstmt = map.get(kpi);
                    count = batch.get(kpi);
                    count++;
                }
                batch.put(kpi, count); // 批量次数的存储

                String collectorName = conf.get(GlobalConstants.OUTPUT_COLLECTOR_KEY_PREFIX + kpi.name);
                Class<?> clazz = Class.forName(collectorName);
                //将值写入 预处理语句中, 将 ? 替换成 值
                IOutputCollector collector = (IOutputCollector) clazz.newInstance();//把value插入到mysql的方法。由于kpi维度不一样。插入到不能表里面。
                collector.collect(conf, key, value, pstmt, converter);

                //前面关闭了批量提交,这里实现批量提交
                //这里存在一个问题, 若最后未满10条如何处理, 见close方法
                if (count % Integer.valueOf(conf.get(GlobalConstants.JDBC_BATCH_NUMBER, GlobalConstants.DEFAULT_JDBC_BATCH_NUMBER)) == 0) {
                    pstmt.executeBatch();
                    conn.commit();
                    // 对应批量计算删除
                    batch.put(kpi, 0);
                }
            } catch (Throwable e) {
                log.error("在writer中写数据出现异常", e);
                throw new IOException(e);
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            try {
                for (Map.Entry<KpiType, PreparedStatement> entry : this.map.entrySet()) {
                    entry.getValue().executeBatch();
                }
            } catch (SQLException e) {
                log.error("执行executeUpdate方法异常", e);
                throw new IOException(e);
            } finally {
                try {
                    if (conn != null) {
                        conn.commit(); // 进行connection的提交动作
                    }
                } catch (Exception e) {
                    // nothing
                } finally {
                    for (Map.Entry<KpiType, PreparedStatement> entry : this.map.entrySet()) {
                        try {
                            entry.getValue().close();
                        } catch (SQLException e) {
                            // nothing
                        }
                    }
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (Exception e) {
                            // nothing
                        }
                    }
                }
            }
        }
    }

}