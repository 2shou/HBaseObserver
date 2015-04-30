package com.gavin.observer;

import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class Config {
    // ElasticSearch的集群名称
    static String clusterName;
    // ElasticSearch的host
    static String nodeHost;
    // ElasticSearch的端口（Java API用的是Transport端口，也就是TCP）
    static int nodePort;
    // ElasticSearch的索引名称
    static String indexName;
    // ElasticSearch的类型名称
    static String typeName;

    public static String getInfo() {
        List<String> fields = new ArrayList<String>();
        try {
            for (Field f : Config.class.getDeclaredFields()) {
                fields.add(f.getName() + "=" + f.get(null));
            }
        } catch (IllegalAccessException ex) {
            ex.printStackTrace();
        }
        return StringUtils.join(fields, ", ");
    }

    public static void main(String[] args) {
        Config.clusterName = "elasticsearch";
        Config.nodeHost = "localhost";
        Config.nodePort = 9300;

        System.out.println(Config.getInfo());
    }
}
