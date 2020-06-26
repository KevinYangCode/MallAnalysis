# 电商离线分析

## 技术
|框架|版本|
|---|---|
|Spark| 2.1.3 |
|Zookeeper| 3.4.14 |
|Kafka| 0.11.0.0 |

## 环境
Windows10：
MySQL

Linux：
Zookeeper、Kafka


## 模块：commons 
配置工具类；常量接口；Spark SQL样例类；MySQL 连接池；其他工具类

## 模块：mock
负责产生模拟数据：

MockDataGenerate：负责生成离线模拟数据并写入 Hive 表；

MockRealTimeData：负责生成实时模拟数据并写入 Kafka 中

## 模块：session
处理数据模块一

主类：SessionStat

## 模块：pageStat
处理数据模块二

主类：PageConvertStat

## 模块：areaStat
处理数据模块三

主类：AreaTop3Stat

## 模块：adversStat
处理数据模块四

主类：AdverStat