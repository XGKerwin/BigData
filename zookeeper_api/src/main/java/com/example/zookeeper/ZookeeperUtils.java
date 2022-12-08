package com.example.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author guanxin
 * @Date 2022-12-08 22:58
 * @Email aiykerwin@sina.com
 */
public class ZookeeperUtils {

    private static int baseSleepTimeMs = 0;
    private static int maxRetries = 0;
    private static String zookeeperList = null;


    /**
     * 获取配置文件
     */
    static {
        InputStream inputStream = ZookeeperUtils.class.getClassLoader().getResourceAsStream("zk.properties");
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
            baseSleepTimeMs = Integer.parseInt(properties.getProperty("baseSleepTimeMs"));
            maxRetries = Integer.parseInt(properties.getProperty("maxRetries"));
            zookeeperList = properties.getProperty("zookeeperList");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 工具类
     *
     * @param mode  创建一个节点  传入带参数 或者不带参数
     * @param path
     * @param value
     */
    public static void createZk(CreateMode mode, String path, String value) {
        CuratorFramework client = null;
        try {
            ExponentialBackoffRetry retry = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
            client = CuratorFrameworkFactory.newClient(zookeeperList, retry);
            client.start();
            if (mode == CreateMode.PERSISTENT) {
                client.create().creatingParentsIfNeeded().withMode(mode).forPath(path);
            } else {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path, value.getBytes());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (client != null) {
                client.close();
            }
        }


    }


}
