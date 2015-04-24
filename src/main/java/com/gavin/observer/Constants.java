package com.gavin.observer;


public class Constants {
    // ElasticSearch的集群名称
    public static final String CLUSTER_NAME = "elasticsearch";
    // ElasticSearch的host
    public static final String SERVER_HOST = "localhost";
    // ElasticSearch的端口（Java版用的是Transport端口，也就是TCP）
    public static final int SERVER_PORT = 9300;
    // ElasticSearch的索引名称
    public static final String INDEX_NAME = "demo";
    // ElasticSearch的类型名称
    public static final String TYPE_NAME = "hbase-sync";
}
