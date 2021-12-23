package com.zero.data.transfer.model.value;

import com.zero.data.clean.common.KpiType;
import org.apache.hadoop.io.Writable;

/**
 * 自定义顶级的输出value父类
 * 
 * @author 马士兵教育
 *
 */
public abstract class BaseStatsValueWritable implements Writable {
    /**
     * 获取当前value对应的kpi值
     * 
     * @return
     */
    public abstract KpiType getKpi();
}
