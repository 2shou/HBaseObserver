package com.gavin.observer;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
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

    private static final int MAX_BULK_COUNT = 10;
    private static final int MAX_COMMIT_INTERVAL = 60 * 5;

    private static Client client = null;
    private static BulkRequestBuilder bulkRequestBuilder = null;

    private static Lock commitLock = new ReentrantLock();

    static {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", Constants.CLUSTER_NAME).build();
        client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(
                        Constants.SERVER_HOST, Constants.SERVER_PORT));
        bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.setRefresh(true);

        Timer timer = new Timer();
        timer.schedule(new CommitTimer(), 10 * 1000, MAX_COMMIT_INTERVAL * 1000);
    }

    public static void addIndexBuilderToBulk(IndexRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            if (bulkRequestBuilder.numberOfActions() >= MAX_BULK_COUNT) {
                BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
                if (!bulkResponse.hasFailures()) {
                    bulkRequestBuilder = client.prepareBulk();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            commitLock.unlock();
        }
    }

    public static void addDeleteBuilderToBulk(DeleteRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            if (bulkRequestBuilder.numberOfActions() >= MAX_BULK_COUNT) {
                BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
                if (!bulkResponse.hasFailures()) {
                    bulkRequestBuilder = client.prepareBulk();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            commitLock.unlock();
        }
    }

    static class CommitTimer extends TimerTask {
        @Override
        public void run() {
            commitLock.lock();
            try {
                if (bulkRequestBuilder.numberOfActions() > 0) {
                    BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
                    if (!bulkResponse.hasFailures()) {
                        bulkRequestBuilder = client.prepareBulk();
                    }
                }
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
            addIndexBuilderToBulk(client.prepareIndex(Constants.INDEX_NAME, Constants.TYPE_NAME, String.valueOf(i)).setSource(json));
        }
        System.out.println(bulkRequestBuilder.numberOfActions());
    }

    public static void main(String[] args) {
        test();
    }
}
