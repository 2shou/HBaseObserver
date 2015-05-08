package com.gavin.observer;


import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ElasticSearchOperator {

    // 缓冲池容量
    private static final int MAX_BULK_COUNT = 10;
    // 最大提交间隔（秒）
    private static final int MAX_COMMIT_INTERVAL = 60 * 5;

    private static Client client = null;
    private static BulkRequestBuilder bulkRequestBuilder = null;

    private static Lock commitLock = new ReentrantLock();

    static {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", Config.clusterName).build();
        client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(
                        Config.nodeHost, Config.nodePort));
        bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.setRefresh(true);

        Timer timer = new Timer();
        timer.schedule(new CommitTimer(), 10 * 1000, MAX_COMMIT_INTERVAL * 1000);
    }

    /**
     * 判断缓存池是否已满，批量提交
     *
     * @param threshold
     */
    private static void bulkRequest(int threshold) {
        if (bulkRequestBuilder.numberOfActions() > threshold) {
            BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
            if (!bulkResponse.hasFailures()) {
                bulkRequestBuilder = client.prepareBulk();
            }
        }
    }

    /**
     * 加入索引请求到缓冲池
     *
     * @param builder
     */
    public static void addUpdateBuilderToBulk(UpdateRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * 加入删除请求到缓冲池
     *
     * @param builder
     */
    public static void addDeleteBuilderToBulk(DeleteRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * 定时任务，避免RegionServer迟迟无数据更新，导致ElasticSearch没有与HBase同步
     */
    static class CommitTimer extends TimerTask {
        @Override
        public void run() {
            commitLock.lock();
            try {
                bulkRequest(0);
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                commitLock.unlock();
            }
        }
    }

    private static void test() {
        for (int i = 0; i < 10; i++) {
            Map<String, Object> json = new HashMap<String, Object>();
            json.put("field", "test");
            addUpdateBuilderToBulk(client.prepareUpdate(Config.indexName, Config.typeName, String.valueOf(i)).setUpsert(json));
        }
        System.out.println(bulkRequestBuilder.numberOfActions());
    }

    public static void main(String[] args) {
        test();
    }
}
