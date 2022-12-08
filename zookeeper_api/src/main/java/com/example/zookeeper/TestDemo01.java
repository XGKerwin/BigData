package com.example.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author guanxin
 * @Date 2022-12-08 22:33
 * @Email aiykerwin@sina.com
 */
public class TestDemo01 {
    private static int baseSleepTimeMs = 0;
    private static int maxRetries = 0;
    private static String zookeeperList = null;

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

    @Test
    public void createZooKeeper() throws Exception {
        // 1 定制一个重试策略
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
        // 2 获取一个客户端
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperList, retry);
        // 3 开启客户端
        client.start();
        // 4 创建节点   1 永久节点不携带数据    2 永久节点携带数据
        int i = 1;
        if (i == 1) {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/guan1");
        } else {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/zzz", "test_zk".getBytes());
        }
        // 5 关闭客户端
        client.close();
    }
}