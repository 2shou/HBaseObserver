package com.ucweb.bigdata.observer;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class DataSyncObserver extends BaseRegionObserver {

    private static final Log LOG = LogFactory.getLog(DataSyncObserver.class);
    private static Client client = null;


    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        LOG.info("observer: start");
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", Constants.CLUSTER_NAME).build();
        client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(
                        Constants.SERVER_HOST, Constants.SERVER_PORT));
    }


    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        try {
            LOG.info("observer: post put");
            String indexId = new String(Base64.encodeBase64(put.getRow()));
            NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
            Map<String, Object> json = new HashMap<String, Object>();
            for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
                Cell cell = entry.getValue().get(0);
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                LOG.info(key + " " + value);
                json.put(key, value);
            }
            ElasticSearchOperator.addIndexBuilderToBulk(client.prepareIndex(Constants.INDEX_NAME, Constants.TYPE_NAME, indexId).setSource(json));
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        }
    }

    @Override
    public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> e, final Delete delete, final WALEdit edit, final Durability durability) throws IOException {
        try {
            LOG.info("observer: post delete");
            String indexId = new String(Base64.encodeBase64(delete.getRow()));
            ElasticSearchOperator.addDeleteBuilderToBulk(client.prepareDelete(Constants.INDEX_NAME, Constants.TYPE_NAME, indexId));
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        }
    }

    private static void testGetPutData(String rowKey, String columnFamily, String column, String value) {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
        System.out.println(Bytes.toString(put.getRow()));
        for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
            Cell cell = entry.getValue().get(0);
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void main(String[] args) {
        testGetPutData("111", "cf", "c1", "hello world");
    }
}
