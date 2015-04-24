# 测试环境

- CDH 5.1.0
- HBase 0.98
- ElasticSearch 1.5.0

# 使用Maven打包

```
mvn clean compile assembly:single
```

部署请参照：[通过HBase Observer同步数据到ElasticSearch](http://guoze.me/2015/04/23/hbase-observer-sync-elasticsearch/)