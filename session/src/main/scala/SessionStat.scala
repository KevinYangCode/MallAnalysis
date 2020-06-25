import java.util.{Date, Random, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils.{DateUtils, NumberUtils, ParamUtils, StringUtils, ValidUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.{immutable, mutable}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * @author Y_Kevin
 * @date 2020-05-26 18:16
 */
object SessionStat {

  def main(args: Array[String]): Unit = {

    // 获取筛选条件
    val jsonStr: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    // 获取筛选条件对应的JSONObject
    val taskParam: JSONObject = JSONObject.fromObject(jsonStr)

    // 创建全局唯一的主键
    val taskUUID: String = UUID.randomUUID().toString

    val sparkConf: SparkConf = new SparkConf().setAppName("session").setMaster("local[*]")

    // 创建 sparkSession （包含sparkContext）
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 1、获取原始的动作表数据
    val actionRDD: RDD[UserVisitAction] = getOriActionRDD(sparkSession, taskParam)
    //    println("=======================获取原始的动作表数据 Start=================================")
    //    actionRDD.foreach(println)
    //    println("=======================获取原始的动作表数据 End=================================")

    // 将数据转换为以 session_id 为Key， UserVisitAction 为Value 的RDD
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map(item => (item.session_id, item))
    //    println("=======================sessionId2ActionRDD Start=================================")
    //    sessionId2ActionRDD.foreach(println)
    //    println("=======================sessionId2ActionRDD End=================================")

    // 将同一个 session_id 的数据 聚合 在一起
    val session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()
    //    println("=======================session2GroupActionRDD Start=================================")
    //    session2GroupActionRDD.foreach(println)
    //    println("=======================session2GroupActionRDD End=================================")

    // 进行缓存
    session2GroupActionRDD.cache()

    // 根据session_id 去获取统计的聚合结果 返回(sessionId，FullInfo)
    val sessionId2FullInfoRDD: RDD[(String, String)] = getSessionFullInfo(sparkSession, session2GroupActionRDD)
    //    println("=======================sessionId2FullInfoRDD Start=================================")
    //    sessionId2FullInfoRDD.foreach(println)
    //    println("=======================sessionId2FullInfoRDD End=================================")

    val sessionAccumulator = new SessionAccumulator
    sparkSession.sparkContext.register(sessionAccumulator)

    // 实现根据限制条件对session数据进行过滤,并完成累加器的更新
    val sessionId2FilterRDD: RDD[(String, String)] = getSessionFilteredRDD(taskParam, sessionId2FullInfoRDD, sessionAccumulator)
    //    println("=======================sessionId2FilterRDD Start=================================")
    //    sessionId2FilterRDD.foreach(println)
    //    println("=======================sessionId2FilterRDD End=================================")

    // 获取最终的统计结果
    getSessionRatio(sparkSession, taskUUID, sessionAccumulator.value)

    // 需求二：session 随机抽取
    // sessionId2FilterRDD: RDD[(sessionId, fullInfo)] 一个session 对应一条数据，也就是一个fullInfo
    sessionRandomExtract(sparkSession, taskUUID, sessionId2FilterRDD)


    // 需求三：在符合条件的用户行为数据中,获取点击、下单和支付数量排名前10的品类。在Top10的排序中,按照点击数量、下单数量、支付数量的次序进行排序,即优先考虑点击数量。
    // sessionId2ActionRDD: RDD[(sessionId, action)]
    // sessionId2FilterRDD: RDD[(sessionId, FullInfo)] 符合过滤条件的
    // sessionId2FilterActionRDD: join
    // 获取所有符合过滤条件的action数据
    val sessionId2FilterActionRDD: RDD[(String, UserVisitAction)] = sessionId2ActionRDD.join(sessionId2FilterRDD).map {
      case (sessionId, (action, fullInfo)) => {
        (sessionId, action)
      }
    }
    // 需求三：Top10热门商品统计
    val top10CategoryArray: Array[(SortKey, String)] = top10PopularCategories(sparkSession, taskUUID, sessionId2FilterActionRDD)

    // 需求四：Top10热门商品的Top10活跃session统计
    // sessionId2FilterActionRDD: RDD[(sessionId, UserVisitAction)]
    // top10CategoryArray: Array[(SortKey, String)]
    top10ActionSession(sparkSession, taskUUID, sessionId2FilterActionRDD, top10CategoryArray)


  }

  def top10ActionSession(sparkSession: SparkSession,
                         taskUUID: String,
                         sessionId2FilterActionRDD: RDD[(String, UserVisitAction)],
                         top10CategoryArray: Array[(SortKey, String)]): Unit = {
    // 第一步：过滤出所有点击过Top10品类的action
    // 1、join
    // 方案一：
    //    val cid2CountInfoRDD: RDD[(Long, String)] = sparkSession.sparkContext.makeRDD(top10CategoryArray).map {
    //      case (sortKey, countInfo) => {
    //        val cid: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
    //        (cid, countInfo)
    //      }
    //    }
    //
    //    val cid2ActionRDD: RDD[(Long, UserVisitAction)] = sessionId2FilterActionRDD.map {
    //      case (sessionId, action) =>
    //        val cid: Long = action.click_category_id
    //        (cid, action)
    //    }
    //
    //    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = cid2CountInfoRDD.join(cid2ActionRDD).map {
    //      case (cid, (countInfo, action)) =>
    //        val sid: String = action.session_id
    //        (sid, action)
    //    }

    // 方案二：使用filter
    val cidArray: Array[Long] = top10CategoryArray.map {
      case (sortKey, countInfo) =>
        val cid: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        cid
    }

    // 所有符合过滤条件的，并且点击过Top10热门品类的action
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter {
      case (sessionId, action) =>
        cidArray.contains(action.click_category_id)
    }
    // 按照sessionId进行聚合操作
    val sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()

    // 统计每个session对于它点击过的所有品类的点击次数
    // cid2SessionCountRDD: RDD[(cid, sessionCount)]
    val cid2SessionCountRDD: RDD[(Long, String)] = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) =>
        val categoryCountMap = new mutable.HashMap[Long, Long]
        for (action <- iterableAction) {
          val cid: Long = action.click_category_id
          if (!categoryCountMap.contains(cid)) {
            categoryCountMap += (cid -> 0)
          }
          categoryCountMap.update(cid, categoryCountMap(cid) + 1)
        }
        // 记录了一个session对于它所有点击过的品类的点击次数
        for ((cid, count) <- categoryCountMap)
          yield (cid, sessionId + "=" + count)
    }

    // cid2GroupRDD: RDD[(cid, IterableSessionCount)]
    // cid2GroupRDD 每一条数据都是一个categoryID和它对应的所有点击过它的session对它的点击次数
    val cid2GroupRDD: RDD[(Long, Iterable[String])] = cid2SessionCountRDD.groupByKey()

    // 对session根据点击次数进行排序，封装到case class ，返回Top10Session
    val top10SessionRDD: RDD[Top10Session] = cid2GroupRDD.flatMap {
      case (cid, iterableSessionCount) =>
        // true: item1放在前面
        // false：item2放在前面
        // item: SessionCount  String   "sessionId=count"
        val sortList: immutable.Seq[String] = iterableSessionCount.toList.sortWith((item1, item2) => {
          item1.split("=")(1).toLong > item2.split("=")(1).toLong
        }).take(10)

        val top10Session: immutable.Seq[Top10Session] = sortList.map {
          // item: SessionCount  String   "sessionId=count"
          case item => {
            val sessionId: String = item.split("=")(0)
            val count: Long = item.split("=")(1).toLong
            Top10Session(taskUUID, cid, sessionId, count)
          }
        }
        top10Session
    }

    // 写进Mysql
    import sparkSession.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_session_0528")
      .mode(SaveMode.Append)
      .save()
  }

  def getClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {

    //    val clickFilterRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter {
    //      case (sessionId, action) => action.click_category_id != -1L
    //    }
    // 先进行过滤，把点击行为对应的action保留下来
    val clickFilterRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter(item => item._2.click_category_id != -1L)

    // 进行格式转化，为 reduceByKey 做准备
    val clickNumRDD: RDD[(Long, Long)] = clickFilterRDD.map {
      case (sessionId, action) => (action.click_category_id, 1L)
    }

    clickNumRDD.reduceByKey(_ + _)
  }

  def getOrderCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    val orderFilterRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter(item => item._2.order_category_ids != null)

    val orderNumRDD: RDD[(Long, Long)] = orderFilterRDD.flatMap {
      // action.order_category_ids.split(","): Array[String]
      // action.order_category_ids.split(",").map(item => (item.toLong, 1L))
      // 先将我们的字符串拆分成字符串数组,然后使用map转化数组中的每个元素,
      // 原来我们的每一个元素都是一个string，现在转化为(long, 1L)
      case (sessionId, action) =>
        action.order_category_ids.split(",").map(item => (item.toLong, 1L))
    }

    orderNumRDD.reduceByKey(_ + _)
  }

  def getPayCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    val payFilterRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter(item => item._2.pay_category_ids != null)

    val payNumRDD: RDD[(Long, Long)] = payFilterRDD.flatMap {
      // action.order_category_ids.split(","): Array[String]
      // action.order_category_ids.split(",").map(item => (item.toLong, 1L))
      // 先将我们的字符串拆分成字符串数组,然后使用map转化数组中的每个元素,
      // 原来我们的每一个元素都是一个string，现在转化为(long, 1L)
      case (sessionId, action) =>
        action.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }

    payNumRDD.reduceByKey(_ + _)
  }

  def getFullCount(cid2CidRDD: RDD[(Long, Long)],
                   cid2ClickCountRDD: RDD[(Long, Long)],
                   cid2OrderCountRDD: RDD[(Long, Long)],
                   cid2PayCountRDD: RDD[(Long, Long)]): RDD[(Long, String)] = {
    val cid2ClickInfoRDD: RDD[(Long, String)] = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map {
      case (cid, (categoryId, option)) => {
        val clickCount: Long = if (option.isDefined) option.get else 0
        val aggrCount: String = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount

        (cid, aggrCount)
      }
    }

    val cid2OrderInfoRDD: RDD[(Long, String)] = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map {
      case (cid, (clickCount, option)) => {
        val orderCount: Long = if (option.isDefined) option.get else 0
        val aggrInfo: String = clickCount + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount

        (cid, aggrInfo)
      }
    }

    val cid2PayInfoRDD: RDD[(Long, String)] = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map {
      case (cid, (orderInfo, option)) => {
        val payCount: Long = if (option.isDefined) option.get else 0
        val aggrInfo: String = orderInfo + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount

        (cid, aggrInfo)
      }
    }
    cid2PayInfoRDD
  }

  def top10PopularCategories(sparkSession: SparkSession, taskUUID: String, sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): Array[(SortKey, String)] = {
    // 第一步:获取所有发生过点击、下单、付款的品类
    var cid2CidRDD: RDD[(Long, Long)] = sessionId2FilterActionRDD.flatMap {
      case (sessionId, action) => {
        val categoryBuffer = new ArrayBuffer[(Long, Long)]()

        // 点击行为
        if (action.click_category_id != -1) {
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        } else if (action.order_category_ids != null) {
          for (orderCid <- action.order_category_ids.split(","))
            categoryBuffer += ((orderCid.toLong, orderCid.toLong))
        } else if (action.pay_category_ids != null) {
          for (payCid <- action.pay_category_ids.split(","))
            categoryBuffer += ((payCid.toLong, payCid.toLong))
        }
        categoryBuffer
      }
    }
    // 去重
    cid2CidRDD = cid2CidRDD.distinct()

    // 第二步：统计品类的点击次数; 下单次数; 付款次数
    val cid2ClickCountRDD: RDD[(Long, Long)] = getClickCount(sessionId2FilterActionRDD)
    val cid2OrderCountRDD: RDD[(Long, Long)] = getOrderCount(sessionId2FilterActionRDD)
    val cid2PayCountRDD: RDD[(Long, Long)] = getPayCount(sessionId2FilterActionRDD)

    // 品类的点击次数; 下单次数; 付款次数 连起来
    // cid2FullCountRDD: RDD[(cid, countInfo)]
    // (27,categoryid=27|clickCount=59|orderCount=85|payCount=75)
    val cid2FullCountRDD: RDD[(Long, String)] = getFullCount(cid2CidRDD, cid2ClickCountRDD, cid2OrderCountRDD, cid2PayCountRDD)

    //    cid2FullCountRDD.foreach(println)

    // 实现自定义二次排序key
    val sortKey2FullCountRDD: RDD[(SortKey, String)] = cid2FullCountRDD.map {
      case (cid, countInfo) => {
        val clickCount: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey: SortKey = SortKey(clickCount, orderCount, payCount)

        (sortKey, countInfo)
      }
    }

    val top10CategoryArray: Array[(SortKey, String)] = sortKey2FullCountRDD.sortByKey(ascending = false).take(10)

    val top10CategoryArrayRDD: RDD[(SortKey, String)] = sparkSession.sparkContext.makeRDD(top10CategoryArray)
    val Top10CategoryRDD: RDD[Top10Category] = top10CategoryArrayRDD.map {
      case (sortKey, countInfo) => {
        val cid: Int = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toInt
        val clickCount: Long = sortKey.clickCount
        val orderCount: Long = sortKey.orderCount
        val payCount: Long = sortKey.payCount

        // 封装到case class Top10Category
        Top10Category(taskUUID, cid, clickCount, orderCount, payCount)
      }
    }
    // 写进数据库
    import sparkSession.implicits._
    Top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category_0528")
      .mode(SaveMode.Append)
      .save()

    top10CategoryArray
  }

  def generateRandomIndexList(extractPerDay: Long,
                              daySessionCount: Long,
                              hourCountMap: mutable.HashMap[String, Long],
                              hourListMap: mutable.HashMap[String, ListBuffer[Int]]
                             ): Unit = {
    for ((hour, count) <- hourCountMap) {
      // 获取一个小时要抽取多少条数据
      var hourExrCount: Int = ((count / daySessionCount.toDouble) * extractPerDay).toInt
      // 避免一个小时要抽取的数量超过这个小时的总数
      if (hourExrCount > count) {
        hourExrCount = count.toInt
      }

      val random = new Random()

      hourListMap.get(hour) match {
        case None => {
          hourListMap(hour) = new ListBuffer[Int]
          for (i <- 0 until hourExrCount) {
            var index: Int = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
        }
        case Some(list) => {
          for (i <- 0 until hourExrCount) {
            var index: Int = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
        }
      }
    }
  }

  def sessionRandomExtract(sparkSession: SparkSession, taskUUID: String, sessionId2FilterRDD: RDD[(String, String)]): Unit = {

    // 将原始的sessionld为key的数据转换为以dateHour为Key的数据
    // dateHour2FullInfoRDD: RDD[(dateHour, fullInfo)]
    val dateHour2FullInfoRDD: RDD[(String, String)] = sessionId2FilterRDD.map {
      case (sessionId, fullInfo) => {
        val startTime: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)

        // dateHour: yyyy-MM-dd_HH
        val dateHour: String = DateUtils.getDateHour(startTime)
        (dateHour, fullInfo)
      }
    }

    // 执行countByKey，获取每个小时的session数量
    // hourCountMap: Map[dateHour, count]
    val hourCountMap: collection.Map[String, Long] = dateHour2FullInfoRDD.countByKey()

    // dateHourCountMap:Map[date,Map[(hour, count)]]
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    // 将dateHour为key的Map转换为Map[date, Map[hour, count]]
    for ((dateHour, count) <- hourCountMap) {
      val date: String = dateHour.split("_")(0)
      val hour: String = dateHour.split("_")(1)

      // 先判断 dateHourCountMap: Map[(date,Map[(hour, count)])] 中 Key为 date 是否有值
      dateHourCountMap.get(date) match {
        case None => {
          // 没值，就新建的 Map[(hour, count)] 值
          dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          // 再把 (hour为key，count为value) 的 value 追加到 date为key 的大Map
          dateHourCountMap(date) += (hour -> count)
        }
        // 有值，直接追加
        case Some(map) => dateHourCountMap(date) += (hour -> count)
      }
    }
    // 解决问题一： 一共有多少天 ：dateHourCountMap.size
    //            一天抽取多少条：100 / dateHourCountMap.size
    val extractPerDay: Int = 100 / dateHourCountMap.size

    // 解决问题二： 一共有多少session ：dateHourCountMap(date).values.sum
    // 解决问题三： 一个小时有多少session ：dateHourCountMap(date)(hour)

    // dateHourExtractIndexListMap:Map[(date,Map[(hour, list)])]
    val dateHourExtractIndexListMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()

    // 根据 Map[date, Map[hour, count]] 填充 Map[date, Map[hour, list]]
    for ((date, hourCountMap) <- dateHourCountMap) {
      val dateSessionCount: Long = hourCountMap.values.sum

      dateHourExtractIndexListMap.get(date) match {
        case None => {
          dateHourExtractIndexListMap(date) = new mutable.HashMap[String, ListBuffer[Int]]()
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dateHourExtractIndexListMap(date))
        }
        case Some(map) => {
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dateHourExtractIndexListMap(date))
        }
      }

      // 到目前为止,我们获得了每个小时要抽取的session的index

      // 广播大变量,提升任务性能
      val dateHourExtractIndexListMapBd: Broadcast[mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]] =
        sparkSession.sparkContext.broadcast(dateHourExtractIndexListMap)

      // 对dateHour2FullInfoRDD执行groupByKey，获取每个小时的所有的session数据
      // dateHour2FullInfoRDD: RDD[(dateHour, fullInfo)]
      // dateHour2GroupRDD: RDD[(dateHour, IterableFullInfo)]
      val dateHour2GroupRDD: RDD[(String, Iterable[String])] = dateHour2FullInfoRDD.groupByKey()

      // 对dateHour2GroupRDD执行flatMap操作,完成具体的抽取操作
      val extractsSessionRDD: RDD[SessionRandomExtract] = dateHour2GroupRDD.flatMap {
        case (dateHour, iterableFullInfo) => {
          val date: String = dateHour.split("_")(0)
          val hour: String = dateHour.split("_")(1)

          val extractList: ListBuffer[Int] = dateHourExtractIndexListMapBd.value.get(date).get(hour)
          val extractsSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()

          var index = 0

          for (fullInfo <- iterableFullInfo) {
            if (extractList.contains(index)) {
              val sessionId: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
              val startTime: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
              val searchKeywords: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
              val clickCategories: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

              val extractsSession: SessionRandomExtract = SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategories)
              extractsSessionArrayBuffer += extractsSession
            }
            index += 1
          }
          extractsSessionArrayBuffer
        }
      }
      import sparkSession.implicits._
      extractsSessionRDD.toDF().write
        .format("jdbc")
        .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
        .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
        .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
        .option("dbtable", "session_extract_0528")
        .mode(SaveMode.Append)
        .save()
    }
  }

  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    val session_count: Double = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    // 不同范围访问时长的session个数
    val visitLength_1s_3s: Int = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visitLength_4s_6s: Int = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visitLength_7s_9s: Int = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visitLength_10s_30s: Int = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visitLength_30s_60s: Int = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visitLength_1m_3m: Int = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visitLength_3m_10m: Int = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visitLength_10m_30m: Int = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visitLength_30m: Int = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    // 不同访问步长的session个数
    val stepLength_1_3: Int = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val stepLength_4_6: Int = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val stepLength_7_9: Int = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val stepLength_10_30: Int = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val stepLength_30_60: Int = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val stepLength_60: Int = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    // 访问时长 1s-3s 的session 所占的比例
    val visitLength_1s_3s_ratio: Double = NumberUtils.formatDouble(visitLength_1s_3s / session_count, 2)
    val visitLength_4s_6s_ratio: Double = NumberUtils.formatDouble(visitLength_4s_6s / session_count, 2)
    val visitLength_7s_9s_ratio: Double = NumberUtils.formatDouble(visitLength_7s_9s / session_count, 2)
    val visitLength_10s_30s_ratio: Double = NumberUtils.formatDouble(visitLength_10s_30s / session_count, 2)
    val visitLength_30s_60s_ratio: Double = NumberUtils.formatDouble(visitLength_30s_60s / session_count, 2)
    val visitLength_1m_3m_ratio: Double = NumberUtils.formatDouble(visitLength_1m_3m / session_count, 2)
    val visitLength_3m_10m_ratio: Double = NumberUtils.formatDouble(visitLength_3m_10m / session_count, 2)
    val visitLength_10m_30m_ratio: Double = NumberUtils.formatDouble(visitLength_10m_30m / session_count, 2)
    val visitLength_30m_ratio: Double = NumberUtils.formatDouble(visitLength_30m / session_count, 2)

    val stepLength_1_3_ratio: Double = NumberUtils.formatDouble(stepLength_1_3 / session_count, 2)
    val stepLength_4_6_ratio: Double = NumberUtils.formatDouble(stepLength_4_6 / session_count, 2)
    val stepLength_7_9_ratio: Double = NumberUtils.formatDouble(stepLength_7_9 / session_count, 2)
    val stepLength_10_30_ratio: Double = NumberUtils.formatDouble(stepLength_10_30 / session_count, 2)
    val stepLength_30_60_ratio: Double = NumberUtils.formatDouble(stepLength_30_60 / session_count, 2)
    val stepLength_60_ratio: Double = NumberUtils.formatDouble(stepLength_60 / session_count, 2)

    // 实例 样例类SessionAggrStat
    val stat: SessionAggrStat = SessionAggrStat(taskUUID,
      session_count.toInt,
      visitLength_1s_3s_ratio,
      visitLength_4s_6s_ratio,
      visitLength_7s_9s_ratio,
      visitLength_10s_30s_ratio,
      visitLength_30s_60s_ratio,
      visitLength_1m_3m_ratio,
      visitLength_3m_10m_ratio,
      visitLength_10m_30m_ratio,
      visitLength_30m_ratio,
      stepLength_1_3_ratio,
      stepLength_4_6_ratio,
      stepLength_7_9_ratio,
      stepLength_10_30_ratio,
      stepLength_30_60_ratio,
      stepLength_60_ratio
    )

    val sessionRatioRDD: RDD[SessionAggrStat] = sparkSession.sparkContext.makeRDD(Array(stat))

    // 写进MySQL
    import sparkSession.implicits._
    sessionRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_ratio_0528")
      .mode(SaveMode.Append)
      .save()
  }


  /**
   * 访问时长 累加器加1
   *
   * @param visitLength
   * @param sessionAccumulator
   */
  def calculateVisitLength(visitLength: Long, sessionAccumulator: SessionAccumulator): Unit = {
    if (visitLength >= 1 && visitLength <= 3) {
      sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  /**
   * 累加器加1
   *
   * @param stepLength
   * @param sessionAccumulator
   */
  def calculateStepLength(stepLength: Long, sessionAccumulator: SessionAccumulator): Unit = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionAccumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  /**
   * 3、实现根据限制条件对session数据进行过滤,并完成累加器的更新
   *
   * @param taskParam
   * @param sessionId2FullInfoRDD
   * @return
   */
  def getSessionFilteredRDD(taskParam: JSONObject, sessionId2FullInfoRDD: RDD[(String, String)], sessionAccumulator: SessionAccumulator): RDD[(String, String)] = {

    // 全部的过滤条件
    val startAge: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals: String = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities: String = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex: String = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords: String = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds: String = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    // 把条件拼接成一个大字符串
    var filterInfo: String = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds + "|" else "")

    // 去除字符串拼接时产生最后的字符 “|”
    if (filterInfo.endsWith("\\|")) {
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    }

    sessionId2FullInfoRDD.filter {
      // 拿着每一条 sessionId 对应着 完整的fullInfo 聚合信息与 filterInfo 过滤条件进行比较
      case (sessionId, fullInfo) => {
        var success = true

        // 判断 sessionId对应fullInfo中的AGE 在 filterInfo 过滤条件之间 ，不在就剔除这条 sessionId 的内容 fullInfo
        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        } else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }

        if (success) {
          // 该条数据符合，先给总数进行加1
          sessionAccumulator.add(Constants.SESSION_COUNT)

          // 判断当前SessionId属于哪个访问时长范围，给对应的访问时长范围进行加1
          val visitLength: Long = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          // 判断当前SessionId属于哪个步长范围，给对应的步长范围进行加1
          val stepLength: Long = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong

          calculateVisitLength(visitLength, sessionAccumulator)
          calculateStepLength(stepLength, sessionAccumulator)
        }
        success
      }
    }
  }


  /**
   * 2、聚合数据
   *
   * @param sparkSession
   * @param session2GroupActionRDD
   * @return
   */
  def getSessionFullInfo(sparkSession: SparkSession, session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]): RDD[(String, String)] = {
    // userId2AggInfoRDD: RDD[(userId, aggrInfo)]
    val userId2AggInfoRDD: RDD[(Long, String)] = session2GroupActionRDD.map {
      // 统计一个 sessionId 的 对应的内容(一个sessionId的内容中最早开始时间和最晚开始时间，内容的条数，)
      case (sessionId, iterableAction) =>
        var userId: Long = -1L

        var startTime: Date = null
        var endTime: Date = null

        var stepLength = 0
        val searchKeyswords = new StringBuffer("")
        val clickCategories = new StringBuffer("")

        for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }

          val actionTime: Date = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }

          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }

          val searchKeyword: String = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeyswords.toString.contains(searchKeyword)) {
            searchKeyswords.append(searchKeyword + ",")
          }

          val clickCategoryId: Long = action.click_category_id
          if (clickCategoryId != -1 && clickCategories.toString.contains(clickCategoryId)) {
            clickCategories.append(clickCategoryId + ",")
          }

          stepLength += 1
        }

        val searchKw: String = StringUtils.trimComma(searchKeyswords.toString)
        val clickCg: String = StringUtils.trimComma(clickCategories.toString)

        val visitLength: Long = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo: String = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
        // userId 可以与下面的 user_info表 联表查询使用
        (userId, aggrInfo)
    }
    //    println("=======================userId2AggInfoRDD Start=================================")
    //    userId2AggInfoRDD.foreach(println)
    //    println("=======================userId2AggInfoRDD End=================================")

    val sql = "select * from user_info"
    import sparkSession.implicits._
    val userId2InfoRDD: RDD[(Long, UserInfo)] = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))
    //    println("=======================userId2InfoRDD Start=================================")
    //    userId2InfoRDD.foreach(println)
    //    println("=======================userId2InfoRDD End=================================")

    val sessionId2FullInfoRDD: RDD[(String, String)] = userId2AggInfoRDD.join(userId2InfoRDD).map {
      case (userId, (aggrInfo, userInfo)) => {
        val age: Int = userInfo.age
        val professional: String = userInfo.professional
        val sex: String = userInfo.sex
        val city: String = userInfo.city

        val fullInfo: String = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val sessionId: String = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)
        (sessionId, fullInfo)
      }
    }
    sessionId2FullInfoRDD
  }


  /**
   * 1、获取原始的动作表数据
   *
   * @param sparkSession sparkSession
   * @param taskParam    筛选条件
   * @return UserVisitAction 类型的RDD
   */
  def getOriActionRDD(sparkSession: SparkSession, taskParam: JSONObject): RDD[UserVisitAction] = {
    // 获取筛选条件 （开始时间和结束时间）
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql: String = "select * from user_visit_action where date >= '" + startDate + "' and date <= '" + endDate + "'"

    import sparkSession.implicits._
    // 筛选出在条件内的数据，并返回 UserVisitAction 类型RDD
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }
}
