package com.zero.data.transfer.mr.nu;


import com.zero.data.clean.common.DateEnum;
import com.zero.data.clean.common.EventLogConstants;
import com.zero.data.clean.common.GlobalConstants;
import com.zero.data.clean.util.TimeUtil;
import com.zero.data.transfer.model.dim.StatsUserDimension;
import com.zero.data.transfer.model.dim.base.DateDimension;
import com.zero.data.transfer.model.value.map.TimeOutputValue;
import com.zero.data.transfer.model.value.reduce.MapWritableValue;
import com.zero.data.transfer.util.JdbcManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.nio.charset.Charset;
import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Alexander Zero
 * @version 1.0.0
 * @date 2021/12/23
 */
@Slf4j
public class NewUserRunner implements Tool {
    private Configuration conf = null;

    public static void main(String[] args) {

        try {
            ToolRunner.run(new Configuration(true), new NewUserRunner(), args);
        } catch (Exception e) {
            log.error("ִ�д���");
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        final Job job = Job.getInstance(conf, "data sink");

        //���ڴ���
        this.processArgs(conf, strings);

        job.setJarByClass(NewUserRunner.class);

        TableMapReduceUtil.initTableMapperJob(
                initScan(),
                NewUserMapper.class,
                StatsUserDimension.class,
                TimeOutputValue.class,
                job,
                false
        );

        job.setReducerClass(NewUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);
        //��������mysql,��Ҫ
        job.setOutputFormatClass(TransformerOutputFormat.class);

        if (job.waitForCompletion(true)) {
            // ִ�гɹ�, ��Ҫ�������û�
            this.calculateTotalUsers(conf);
            return 0;
        } else {
            return -1;
        }

    }

    //�з�Χ
    private List<Scan> initScan() {
        final Scan scan = new Scan();

        // ��ȡ����ʱ��: yyyy-MM-dd
        String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
        long startDate = TimeUtil.parseString2Long(date);
        long endDate = startDate + GlobalConstants.DAY_OF_MILLISECONDS;

        //���ÿ�ʼ�ͽ�����Χ
        scan.withStartRow(Bytes.toBytes("" + startDate));
        scan.withStopRow(Bytes.toBytes("" + endDate));

        //���ù�������
        FilterList filterList = new FilterList();
        //launch�¼�������
        final SingleColumnValueFilter columnFilter = new SingleColumnValueFilter(
                Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME),
                Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME),
                CompareOperator.EQUAL,
                Bytes.toBytes(EventLogConstants.EventEnum.LAUNCH.alias)
        );

        filterList.addFilter(columnFilter);
        filterList.addFilter(this.getColumnFilter());

        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(EventLogConstants.TABLE_NAME));
        scan.setFilter(filterList);

        return Collections.singletonList(scan);
    }

    private Filter getColumnFilter() {
        String[] columns = new String[]{
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME,
                EventLogConstants.LOG_COLUMN_NAME_UUID,
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION};
        int length = columns.length;
        byte[][] filter = new byte[length][];
        for (int i = 0; i < length; i++) {
            filter[i] = Bytes.toBytes(columns[i]);
        }
        return new MultipleColumnPrefixFilter(filter);
    }

    @Override
    public void setConf(Configuration configuration) {
        //hbase zookeeper(ע��)
        configuration.set("hbase.zookeeper.quorum", "node02,node03,node04");

        //windows�칹ƽ̨
        configuration.set("mapreduce.app-submission.cross-platform", "true");
        configuration.set("mapreduce.framework.name", "local");

//        configuration.addResource("output-collector.xml");
//        configuration.addResource("query-mapping.xml");
//        configuration.addResource("transformer-env.xml");
        this.conf = HBaseConfiguration.create(configuration);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    /**
     * �������
     *
     * @param conf
     * @param args
     */
    private void processArgs(Configuration conf, String[] args) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if ("-d".equals(args[i])) {
                if (i + 1 < args.length) {
                    date = args[++i];
                    break;
                }
            }
        }

        // Ҫ��date��ʽΪ: yyyy-MM-dd
        if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
            // date��һ����Чʱ������
            date = TimeUtil.getYesterday(); // Ĭ��ʱ��������
        }
        System.out.println("----------------------" + date);
        conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
    }


    /**
     * �������û�
     *
     * @param conf
     */
    private void calculateTotalUsers(Configuration conf) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            long date = TimeUtil.parseString2Long(conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
            // ��ȡ�����date dimension
            DateDimension todayDimension = DateDimension.buildDate(date, DateEnum.DAY);
            // ��ȡ�����date dimension
            DateDimension yesterdayDimension = DateDimension.buildDate(date - GlobalConstants.DAY_OF_MILLISECONDS, DateEnum.DAY);
            int yesterdayDimensionId = -1;
            int todayDimensionId = -1;

            // 1. ��ȡʱ��id
            conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
            // ��ȡִ��ʱ��������
            pstmt = conn.prepareStatement("SELECT `id` FROM `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `type` = ? AND `calendar` = ?");
            int i = 0;
            pstmt.setInt(++i, yesterdayDimension.getYear());
            pstmt.setInt(++i, yesterdayDimension.getSeason());
            pstmt.setInt(++i, yesterdayDimension.getMonth());
            pstmt.setInt(++i, yesterdayDimension.getWeek());
            pstmt.setInt(++i, yesterdayDimension.getDay());
            pstmt.setString(++i, yesterdayDimension.getType());
            pstmt.setDate(++i, new Date(yesterdayDimension.getCalendar().getTime()));
            rs = pstmt.executeQuery();
            if (rs.next()) {
                yesterdayDimensionId = rs.getInt(1);
            }

            // ��ȡִ��ʱ�䵱���id
            pstmt = conn.prepareStatement("SELECT `id` FROM `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `type` = ? AND `calendar` = ?");
            i = 0;
            pstmt.setInt(++i, todayDimension.getYear());
            pstmt.setInt(++i, todayDimension.getSeason());
            pstmt.setInt(++i, todayDimension.getMonth());
            pstmt.setInt(++i, todayDimension.getWeek());
            pstmt.setInt(++i, todayDimension.getDay());
            pstmt.setString(++i, todayDimension.getType());
            pstmt.setDate(++i, new Date(todayDimension.getCalendar().getTime()));
            rs = pstmt.executeQuery();
            if (rs.next()) {
                todayDimensionId = rs.getInt(1);
            }

            // 2.��ȡ�����ԭʼ����,�洢��ʽΪ:platformid = totalusers
            Map<String, Integer> oldValueMap = new HashMap<String, Integer>();

            // ��ʼ����stats_user
            if (yesterdayDimensionId > -1) {
                pstmt = conn.prepareStatement("select `platform_dimension_id`,`total_install_users` from `stats_user` where `date_dimension_id`=?");
                pstmt.setInt(1, yesterdayDimensionId);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int totalUsers = rs.getInt("total_install_users");
                    oldValueMap.put("" + platformId, totalUsers);
                }
            }

            // ��ӽ�������û�
            pstmt = conn.prepareStatement("select `platform_dimension_id`,`new_install_users` from `stats_user` where `date_dimension_id`=?");
            pstmt.setInt(1, todayDimensionId);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                int platformId = rs.getInt("platform_dimension_id");
                int newUsers = rs.getInt("new_install_users");
                if (oldValueMap.containsKey("" + platformId)) {
                    newUsers += oldValueMap.get("" + platformId);
                }
                oldValueMap.put("" + platformId, newUsers);
            }

            // ���²���
            pstmt = conn.prepareStatement("INSERT INTO `stats_user`(`platform_dimension_id`,`date_dimension_id`,`total_install_users`) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE `total_install_users` = ?");
            for (Map.Entry<String, Integer> entry : oldValueMap.entrySet()) {
                pstmt.setInt(1, Integer.valueOf(entry.getKey()));
                pstmt.setInt(2, todayDimensionId);
                pstmt.setInt(3, entry.getValue());
                pstmt.setInt(4, entry.getValue());
                pstmt.execute();
            }

            // ��ʼ����stats_device_browser
            oldValueMap.clear();
            if (yesterdayDimensionId > -1) {
                pstmt = conn.prepareStatement("select `platform_dimension_id`,`browser_dimension_id`,`total_install_users` from `stats_device_browser` where `date_dimension_id`=?");
                pstmt.setInt(1, yesterdayDimensionId);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int browserId = rs.getInt("browser_dimension_id");
                    int totalUsers = rs.getInt("total_install_users");
                    oldValueMap.put(platformId + "_" + browserId, totalUsers);
                }
            }

            // ��ӽ�������û�
            pstmt = conn.prepareStatement("select `platform_dimension_id`,`browser_dimension_id`,`new_install_users` from `stats_device_browser` where `date_dimension_id`=?");
            pstmt.setInt(1, todayDimensionId);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                int platformId = rs.getInt("platform_dimension_id");
                int browserId = rs.getInt("browser_dimension_id");
                int newUsers = rs.getInt("new_install_users");
                String key = platformId + "_" + browserId;
                if (oldValueMap.containsKey(key)) {
                    newUsers += oldValueMap.get(key);
                }
                oldValueMap.put(key, newUsers);
            }

            // ���²���
            pstmt = conn.prepareStatement("INSERT INTO `stats_device_browser`(`platform_dimension_id`,`browser_dimension_id`,`date_dimension_id`,`total_install_users`) VALUES(?, ?, ?, ?) ON DUPLICATE KEY UPDATE `total_install_users` = ?");
            for (Map.Entry<String, Integer> entry : oldValueMap.entrySet()) {
                String[] key = entry.getKey().split("_");
                pstmt.setInt(1, Integer.valueOf(key[0]));
                pstmt.setInt(2, Integer.valueOf(key[1]));
                pstmt.setInt(3, todayDimensionId);
                pstmt.setInt(4, entry.getValue());
                pstmt.setInt(5, entry.getValue());
                pstmt.execute();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
