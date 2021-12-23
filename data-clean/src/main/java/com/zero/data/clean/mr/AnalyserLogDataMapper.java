package com.zero.data.clean.mr;

import com.zero.data.clean.common.EventLogConstants;
import com.zero.data.clean.util.LogParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * @author Alexander Zero
 * @version 1.0.0
 * @date 2021/12/22
 */
@Slf4j
public class AnalyserLogDataMapper extends Mapper<LongWritable, Text, NullWritable, Put> {


    // 主要用于标志，方便查看过滤数据
    private int inputRecords, filterRecords, outputRecords;
    private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private CRC32 crc32 = new CRC32();

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        super.cleanup(context);
        log.info("输入数据:" + this.inputRecords + "；输出数据:" + this.outputRecords
                + "；过滤数据:" + this.filterRecords);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) {

        //处理记录数+1
        this.inputRecords++;
        final String logData = value.toString();
        try {
            final Map<String, String> info = LogParser.parseLog(logData);
            System.out.println(info);

            if (info.isEmpty()) {
                filterRecords++;
                return;
            }

            //事件别名
            final String eventName = info.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME);
            final EventLogConstants.EventEnum event = EventLogConstants.EventEnum.valueOfAlias(eventName);
            switch (event) {
                case LAUNCH:
                case PAGEVIEW:
                case CHARGEREQUEST:
                case CHARGEREFUND:
                case CHARGESUCCESS:
                case EVENT:
                    this.handleData(info, event, context);
                    break;
                default:
                    this.filterRecords++;
                    log.error("该事件无法解析,事件名: " + eventName);

            }


        } catch (Exception e) {
            e.printStackTrace();
            log.error("处理数据异常,数据: " + value);
            filterRecords++;
        }


    }

    private void handleData(Map<String, String> info, EventLogConstants.EventEnum event, Context context) throws IOException, InterruptedException {
        String uuid = info.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
        String memberId = info.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID);
        String serverTime = info.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);
        if (StringUtils.isNotBlank(serverTime)) {
            // 浏览器信息去掉
            info.remove(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT);
            //主键生成规则
            String rowKey = this.generateRowKey(uuid, memberId, event.alias, serverTime);

            Put put = new Put(Bytes.toBytes(rowKey));
            info.forEach((k, v) -> {
                if (StringUtils.isNotBlank(k) && StringUtils.isNotBlank(v)) {
                    put.addColumn(family, Bytes.toBytes(k), Bytes.toBytes(v));
                }
            });
            context.write(NullWritable.get(), put);
            outputRecords++;

        } else {
            this.filterRecords++;
            log.error("数据服务事件为空");
        }


    }

    private String generateRowKey(String uuid, String memberId, String alias, String serverTime) {
        StringBuffer sb = new StringBuffer();
        sb.append(serverTime).append("_");

        //将uuid 和 memberId 还有事件用crc32加密？？
        this.crc32.reset();

        if (StringUtils.isNotBlank(uuid)) {
            this.crc32.update(uuid.getBytes());
        }
        if (StringUtils.isNotBlank(memberId)) {
            this.crc32.update(memberId.getBytes());
        }
        if (StringUtils.isNotBlank(alias)) {
            this.crc32.update(alias.getBytes());
        }

        sb.append(this.crc32.getValue() % 100000000L);
        return sb.toString();

    }
}
