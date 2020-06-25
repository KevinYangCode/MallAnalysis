import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
 * @author Y_Kevin
 * @date 2020-06-11 23:37
 */
object AdverStat {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 从故障中恢复
    //    val streamingContext = StreamingContext.getActiveOrCreate(checkpointDir,func)
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafka_brokers: String = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics: String = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // auto.offset.reset
      // latest: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最新的数据开始消费
      // earliest: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最开始的数据开始消费
      // none: 先去Zookeeper获取offset，如果有，直接使用，如果没有，直接报错
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // adRealTimeDStream DStream[RDD RDD RDD ...] RDD[message] message: key value
    val adRealTimeDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )

    // 取出了 DStream里面每一条数据的value值
    // adRealTimeDStream DStream[RDD RDD RDD ...] RDD[String]
    // String:  timestamp  province   city   userId   adId
    val adRealTimeValueDStream: DStream[String] = adRealTimeDStream.map(item => item.value())

    // transform：遍历DStream里每一个RDD
    val adRealTimeFilterDStream: DStream[String] = adRealTimeValueDStream.transform {
      logRDD =>
        // 获取黑名单数据里的userId
        // AdBlacklist: UserId
        val blackListArray: Array[AdBlacklist] = AdBlacklistDAO.findAll()
        val userIdArray: Array[Long] = blackListArray.map(item => item.userid)

        // 对每一个RDD进行过滤，看是否在黑名单了
        logRDD.filter {
          // log : timestamp  province   city   userid   adid
          log =>
            val logSplit: Array[String] = log.split(" ")
            val userId: Long = logSplit(3).toLong
            // 不在黑名单里，则保存下来
            !userIdArray.contains(userId)
        }
    }

    streamingContext.checkpoint("./spark-streaming")

    adRealTimeFilterDStream.checkpoint(Duration(10000))

    // 需求一：实时维护黑名单(某个用户一天点击该广告超过100次,就进入黑名单)
    generateBlackList(adRealTimeFilterDStream)

    // 需求二：各省各城市一天中的广告点击量（累积统计）
    val key2ProvinceCityCountDStream: DStream[(String, Long)] = provinceCityClickStat(adRealTimeFilterDStream)

    // 需求三：统计各省Top3热门广告
    provinceTop3Adver(sparkSession, key2ProvinceCityCountDStream)

    // 需求四：最近一个小时广告点击量统计
    getRecentHourClickCount(adRealTimeFilterDStream)

    //    adRealTimeFilterDStream.foreachRDD(rdd => rdd.foreach(println))

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  // 需求四：最近一个小时广告点击量统计
  def getRecentHourClickCount(adRealTimeFilterDStream: DStream[String]): Unit = {
    val key2TimeMinuteDStream: DStream[(String, Long)] = adRealTimeFilterDStream.map {
      // log : timestamp  province   city   userId   adId
      log =>
        val logSplit: Array[String] = log.split(" ")
        val timeStamp: Long = logSplit(0).toLong
        // yyyyMMddHHmm
        val timeMinute: String = DateUtils.formatTimeMinute(new Date(timeStamp))
        val adId: Long = logSplit(4).toLong

        val key: String = timeMinute + "_" + adId

        (key, 1L)
    }
    val key2WindowDStream: DStream[(String, Long)] = key2TimeMinuteDStream.reduceByKeyAndWindow((a: Long, b: Long) => (a + b), Minutes(60), Minutes(1))

    key2WindowDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val trendArray = new ArrayBuffer[AdClickTrend]()
            for ((key, count) <- items) {
              val keySplit: Array[String] = key.split("_")
              // yyyyMMddHHmm
              val timeMinute: String = keySplit(0)
              val date: String = timeMinute.substring(0, 8)
              val hour: String = timeMinute.substring(8, 10)
              val minute: String = timeMinute.substring(10)
              val adId: Long = keySplit(1).toLong

              trendArray += AdClickTrend(date, hour, minute, adId, count)
            }
            AdClickTrendDAO.updateBatch(trendArray.toArray)
        }
    }
  }

  // 需求三：统计各省Top3热门广告
  def provinceTop3Adver(sparkSession: SparkSession,
                        key2ProvinceCityCountDStream: DStream[(String, Long)]): Unit = {
    // key2ProvinceCityCountDStream : [RDD[(key, count)]]
    // key: date_province_city_adId
    // key2ProvinceCountDStream：[RDD[(newKey, count)]]
    // newKey: date_province_adId
    val key2ProvinceCountDStream: DStream[(String, Long)] = key2ProvinceCityCountDStream.map {
      case (key, count) =>
        val keySplit: Array[String] = key.split("_")
        val date: String = keySplit(0)
        val province: String = keySplit(1)
        val adId: String = keySplit(3)

        val newKey: String = date + "_" + province + "_" + adId
        (newKey, count)
    }

    val key2ProvinceAggrCountDStream: DStream[(String, Long)] = key2ProvinceCountDStream.reduceByKey(_ + _)

    // 获取 Top3
    val top3DStream: DStream[Row] = key2ProvinceAggrCountDStream.transform {
      rdd =>
        // rdd:RDD[(key, count)]
        // key:date_province_adId
        val basicDateRDD: RDD[(String, String, Long, Long)] = rdd.map {
          case (key, count) =>
            val keySplit: Array[String] = key.split("_")
            val date: String = keySplit(0)
            val province: String = keySplit(1)
            val adId: Long = keySplit(2).toLong

            (date, province, adId, count)
        }
        import sparkSession.implicits._
        basicDateRDD.toDF("date", "province", "adId", "count").createOrReplaceTempView("tmp_basic_info")

        val sql: String = "select date, province, adId, count from (" +
          "select date, province, adId, count, row_number() " +
          "over(partition by date, province order by count desc) rank from tmp_basic_info) t " +
          "where rank <= 3"

        sparkSession.sql(sql).rdd
    }

    // 写进MySQL
    top3DStream.foreachRDD {
      // rdd: RDD[Row]
      rdd =>
        rdd.foreachPartition {
          // items : row
          items =>
            val top3Array = new ArrayBuffer[AdProvinceTop3]()
            for (item <- items) {
              val date: String = item.getAs[String]("date")
              val province: String = item.getAs[String]("province")
              val adId: Long = item.getAs[Long]("adId")
              val count: Long = item.getAs[Long]("count")

              top3Array += AdProvinceTop3(date, province, adId, count)
            }
            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
        }
    }
  }

  // 需求二：各省各城市一天中的广告点击量（累积统计）
  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]): DStream[(String, Long)] = {
    // DStream[RDD RDD RDD ...] RDD[String] String -> log:  timestamp  province   city   userid   adid
    val key2ProvinceCityDStream: DStream[(String, Long)] = adRealTimeFilterDStream.map {
      log =>
        val logSplit: Array[String] = log.split(" ")
        val timeStamp: Long = logSplit(0).toLong
        // dateKey: yy-mm-dd
        val dateKey: String = DateUtils.formatDateKey(new Date(timeStamp))
        val province: String = logSplit(1)
        val city: String = logSplit(2)
        val adId: String = logSplit(4)

        val key: String = dateKey + "_" + province + "_" + city + "_" + adId

        (key, 1L)
    }

    // key2StateDStream 某一天一个省的一个城市中某一个广告的点击次数(累积)
    // 聚合 (以Key)
    // key： dateKey_province_city_adId
    // value 1L
    val key2StateDStream: DStream[(String, Long)] = key2ProvinceCityDStream.updateStateByKey[Long] {
      // 聚合相同的key， values 就是 多个1 就是把相同的key 的value值加起来
      // state 是 对应的key的 checkpoint 出去的累加值
      (values: Seq[Long], state: Option[Long]) =>
        var newValue = 0L
        // 先取 当前Key的 state 有没有值，有值就取出来
        if (state.isDefined)
          newValue = state.get
        for (value <- values) {
          newValue += value
        }
        Some(newValue)
    }

    // 写进MySQL
    key2StateDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val adStatArray = new ArrayBuffer[AdStat]()
            // key : date province city adId
            for ((key, count) <- items) {
              val keySplit: Array[String] = key.split("_")
              val date: String = keySplit(0)
              val province: String = keySplit(1)
              val city: String = keySplit(2)
              val adId: Long = keySplit(3).toLong

              adStatArray += AdStat(date, province, city, adId, count)
            }
            AdStatDAO.updateBatch(adStatArray.toArray)
        }
    }

    key2StateDStream
  }

  // 需求一：实时维护黑名单
  def generateBlackList(adRealTimeFilterDStream: DStream[String]): Unit = {
    // 构造 key-value 对
    // DStream[RDD RDD RDD ...] RDD[String] String -> log:  timestamp  province   city   userid   adid
    val key2NumDStream: DStream[(String, Long)] = adRealTimeFilterDStream.map {
      // log:  timestamp  province   city   userid   adid
      log =>
        val logSplit: Array[String] = log.split(" ")
        val timeStamp: Long = logSplit(0).toLong
        // yy-mm-dd
        val dateKey: String = DateUtils.formatDateKey(new Date(timeStamp))
        val userId: Long = logSplit(3).toLong
        val adId: Long = logSplit(4).toLong
        val key: String = dateKey + "_" + userId + "_" + adId

        (key, 1L)
    }

    val key2CountDStream: DStream[(String, Long)] = key2NumDStream.reduceByKey(_ + _)

    // 根据每一个RDD里面的数据，更新用户点击次数
    key2CountDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val clickCountArray = new ArrayBuffer[AdUserClickCount]()

            for ((key, count) <- items) {
              val keySplit: Array[String] = key.split("_")
              val date: String = keySplit(0)
              val userId: Long = keySplit(1).toLong
              val adId: Long = keySplit(2).toLong

              clickCountArray += AdUserClickCount(date, userId, adId, count)
            }

            AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
        }
    }
    // 获取超过阈值的 DStream
    val key2BlackListDStream: DStream[(String, Long)] = key2CountDStream.filter {
      case (key, count) =>
        val keySplit: Array[String] = key.split("_")
        val date: String = keySplit(0)
        val userId: Long = keySplit(1).toLong
        val adId: Long = keySplit(2).toLong

        val clickCount: Int = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adId)
        if (clickCount > 100) {
          true
        } else {
          false
        }
    }

    // 从超过阈值的 DStream 拿userID，去重
    val userIdDStream: DStream[Long] = key2BlackListDStream.map {
      case (key, count) => key.split("_")(1).toLong
    }.transform(rdd => rdd.distinct())

    // 写进MySQL
    userIdDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val userIdArray = new ArrayBuffer[AdBlacklist]()
            for (userId <- items) {
              userIdArray += AdBlacklist(userId)
            }
            AdBlacklistDAO.insertBatch(userIdArray.toArray)
        }
    }
  }

}
