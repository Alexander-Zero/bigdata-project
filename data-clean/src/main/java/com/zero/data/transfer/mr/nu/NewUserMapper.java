package com.zero.data.transfer.mr.nu;

import com.zero.data.clean.common.DateEnum;
import com.zero.data.clean.common.EventLogConstants;
import com.zero.data.clean.common.KpiType;
import com.zero.data.transfer.model.dim.StatsCommonDimension;
import com.zero.data.transfer.model.dim.StatsUserDimension;
import com.zero.data.transfer.model.dim.base.BrowserDimension;
import com.zero.data.transfer.model.dim.base.DateDimension;
import com.zero.data.transfer.model.dim.base.KpiDimension;
import com.zero.data.transfer.model.dim.base.PlatformDimension;
import com.zero.data.transfer.model.value.map.TimeOutputValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @author Alexander Zero
 * @version 1.0.0
 * @date 2021/12/23
 */
@Slf4j
public class NewUserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {

    private StatsUserDimension statsUserDimension = new StatsUserDimension();
    private TimeOutputValue timeOutputValue = new TimeOutputValue();

    private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);

    //代表用户分析模块的统计
    private KpiDimension newInstallUserKpi = new KpiDimension(KpiType.NEW_INSTALL_USER.name);
    //浏览器分析模块的统计
    private KpiDimension newInstallUserOfBrowserKpi = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER.name);


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String uuid = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));

        System.out.println(uuid + "-" + serverTime + "-" + platform);


        if (StringUtils.isBlank(uuid) || StringUtils.isBlank(serverTime) || StringUtils.isBlank(platform)) {
            log.error("uuid&servertime&platform不能为空");
            return;
        }

        long longOfTime = Long.valueOf(serverTime.trim());
        // 设置id为uuid
        timeOutputValue.setId(uuid);
        // 设置时间为服务器时间
        timeOutputValue.setTime(longOfTime);

        DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);

        // 设置date维度
        StatsCommonDimension statsCommonDimension = this.statsUserDimension.getStatsCommon();
        statsCommonDimension.setDate(dateDimension);

        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

        // browser相关的数据
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);

        //空浏览器维度，不考虑浏览器维度
        BrowserDimension defaultBrowser = new BrowserDimension("", "");


        for (PlatformDimension pd : platformDimensions) {
            statsUserDimension.setBrowser(defaultBrowser);
            statsCommonDimension.setKpi(newInstallUserKpi);
            statsCommonDimension.setPlatform(pd);
            context.write(statsUserDimension,timeOutputValue);

            for (BrowserDimension bd : browserDimensions) {
                statsCommonDimension.setKpi(newInstallUserOfBrowserKpi);
                statsUserDimension.setBrowser(bd);
                context.write(statsUserDimension,timeOutputValue);
            }
        }



    }
}



