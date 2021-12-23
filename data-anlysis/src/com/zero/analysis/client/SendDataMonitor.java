package com.zero.analysis.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Alexander Zero
 * @version 1.0.0
 * @date 2021/12/21
 */
public class SendDataMonitor {

    /**
     * 日志
     */
    private static final Logger log = Logger.getGlobal();

    /**
     * 队列 存放url地址
     */
    private BlockingQueue<String> queue = new LinkedBlockingDeque<>();

    private static SendDataMonitor monitor = null;

    /**
     * 私有化构造
     */
    private SendDataMonitor() {
    }


    public static void addSendUrl(String url) throws InterruptedException {
        getInstance().queue.put(url);
    }

    private static SendDataMonitor getInstance() {
        if (monitor == null) {
            synchronized (SendDataMonitor.class) {
                if (monitor == null) {
                    monitor = new SendDataMonitor();
                    final Thread thread = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            // 线程中调用具体的处理方法
                            SendDataMonitor.monitor.run();
                        }
                    });

                    thread.start();

                }
            }
        }


        return monitor;
    }

    private void run() {
        while (true) {
            try {
                final String url = queue.take();
                HttpRequestUtil.sendData(url);
            } catch (InterruptedException | IOException e) {
                log.log(Level.WARNING, "发送url异常", e);
            }
        }

    }


    /**
     * 内部类，用户发送数据的http工具类
     *
     * @author root
     */
    public static class HttpRequestUtil {
        /**
         * 具体发送url的方法
         *
         * @param url
         * @throws IOException
         */
        public static void sendData(String url) throws IOException {
            HttpURLConnection con = null;
            BufferedReader in = null;

            try {
                URL obj = new URL(url); // 创建url对象
                con = (HttpURLConnection) obj.openConnection(); // 打开url连接
                // 设置连接参数
                con.setConnectTimeout(5000); // 连接过期时间
                con.setReadTimeout(5000); // 读取数据过期时间
                con.setRequestMethod("GET"); // 设置请求类型为get

                System.out.println("发送url:" + url);
                // 发送连接请求
				in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                // TODO: 这里考虑是否可以
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (Throwable e) {
                    // nothing
                }
                try {
                    con.disconnect();
                } catch (Throwable e) {
                    // nothing
                }
            }
        }
    }
}
