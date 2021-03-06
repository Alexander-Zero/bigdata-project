package com.zero.data.transfer.mr.nu;


import com.zero.data.transfer.model.dim.base.BaseDimension;
import com.zero.data.transfer.model.value.BaseStatsValueWritable;
import com.zero.data.transfer.service.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 自定义的配合自定义output进行具体sql输出的类
 * 
 * @author 马士兵教育
 *
 */
public interface IOutputCollector {

    /**
     * 具体执行统计数据插入的方法
     * 
     * @param conf
     * @param key
     * @param value
     * @param pstmt
     * @param converter
     * @throws SQLException
     * @throws IOException
     */
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException;
}
