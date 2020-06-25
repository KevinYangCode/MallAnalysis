import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

/**
 * @author Y_Kevin
 * @date 2020-06-07 16:20
 */
object PageConvertStat {

  def main(args: Array[String]): Unit = {
    // 获取任务限制条件
    val jsonStr: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam: JSONObject = JSONObject.fromObject(jsonStr)

    // 获取唯一主键
    val taskUUID: String = UUID.randomUUID().toString

    // 创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("pageConvert").setMaster("local[*]")

    // 创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 获取用户行为数据
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = getUserVisitAction(sparkSession, taskParam)

    // 获取目标页面切片
    // pageFlowStr: “1,2,3,4,5,6,7”
    val pageFlowStr: String = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    val pageFlowArray: Array[String] = pageFlowStr.split(",")
    // pageFlowArray.slice(0, pageFlowArray.length - 1) : [1,2,3,4,5,6]
    // pageFlowArray.tail : [2,3,4,5,6,7]
    // pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail) : [(1,2),(2,3),..]
    // targetPageSplit : [1_2, 2_3, 3_4,...]
    val targetPageSplit: Array[String] = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail).map {
      case (page1, page2) => page1 + "_" + page2
    }

    // 对sessionId2ActionRDD数据进行 groupByKey 操作
    val sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()

    val pageSplitNumRDD: RDD[(String, Long)] = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) =>
        // 对每个session对应的iterable类型的数据按照时间(action_time)进行排序
        // item1 : Action
        // item2 : Action
        // sortList: List[UserVisitAction]
        val sortList: List[UserVisitAction] = iterableAction.toList.sortWith((item1, item2) => {
          DateUtils.parseTime(item1.action_time).getTime < DateUtils.parseTime(item2.action_time).getTime
        })
        // 取出排序完成的每个action的page_id信息
        // pageList: List[Long] [1,2,3,4,...]
        val pageList: List[Long] = sortList.map {
          case action => action.page_id
        }

        // 把page_id转化为页面切片形式
        // pageList.slice(0, pageList.length - 1) : [1,2,3,...,N-1]
        // pageList.tail : [2,3,4,...,N]
        // pageList.slice(0, pageList.length - 1).zip(pageList.tail) : [(1,2), (2,3), ...]
        // pageSplit : [1_2, 2_3, ...]
        val pageSplit: List[String] = pageList.slice(0, pageList.length - 1).zip(pageList.tail).map {
          case (page1, page2) => page1 + "_" + page2
        }

        // 根据目标页面页面切片,将不存于目标页面切片的所有切片过滤掉
        val pageSplitFilter: List[String] = pageSplit.filter {
          case pageSplit => targetPageSplit.contains(pageSplit)
        }

        // 返回 RDD[(pageSplit, 1L)]
        pageSplitFilter.map {
          case pageSplit => (pageSplit, 1L)
        }
    }

    // 拿到了每一个页面切片的总个数
    // pageSplitCountMap : Map[(pageSplit, count)]
    val pageSplitCountMap: collection.Map[String, Long] = pageSplitNumRDD.countByKey()

    // 获取起始页面page1
    val startPage: Long = pageFlowArray(0).toLong

    // 获取起始页面个数
    val startPageCount: Long = sessionId2ActionRDD.filter {
      case (sessionId, action) => action.page_id == startPage
    }.count()

    // 根据所有的切片个数信息，计算实际的页面切片转化率大小
    getPageConvert(sparkSession, taskUUID, targetPageSplit, startPageCount, pageSplitCountMap)
  }

  /**
   * 根据所有的切片个数信息，计算实际的页面切片转化率大小
   *
   * @param sparkSession
   * @param taskUUID
   * @param targetPageSplit
   * @param startPageCount
   * @param pageSplitCountMap
   */
  def getPageConvert(sparkSession: SparkSession,
                     taskUUID: String,
                     targetPageSplit: Array[String],
                     startPageCount: Long,
                     pageSplitCountMap: collection.Map[String, Long]): Unit = {

    val pageSplitRatio = new mutable.HashMap[String, Double]()
    var lastPageCount: Double = startPageCount.toDouble

    // 1,2,3,4,5,6,7
    // 1_2, 2_3, ...
    for (pageSplit <- targetPageSplit) {
      // 第一次循环： lastPageCount: page1  currentPageSplitCount: page1_page2  结果：page1 跳转到 page2的转化率
      // 下一页的值
      val currentPageSplitCount: Double = pageSplitCountMap.get(pageSplit).get.toDouble
      // 转化率 （page1_page2 / page1）
      val ratio: Double = currentPageSplitCount / lastPageCount
      // 将每一页的转化率存进HashMap
      pageSplitRatio.put(pageSplit, ratio)
      // 当前页的值
      lastPageCount = currentPageSplitCount
    }

    // 遍历出来，以"|"进行拼接
    val convertStr: String = pageSplitRatio.map {
      case (pageSplit, ratio) => pageSplit + "=" + ratio
    }.mkString("|")

    // 封装到 case class
    val pageSplit: PageSplitConvertRate = PageSplitConvertRate(taskUUID, convertStr)

    val pageSplitRatioRDD: RDD[PageSplitConvertRate] = sparkSession.sparkContext.makeRDD(Array(pageSplit))

    import sparkSession.implicits._
    pageSplitRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "page_split_convert_rate0528")
      .mode(SaveMode.Append)
      .save()
  }

  /**
   * 从 user_visit_action 表里读取指定时间范围内的用户行为数据
   *
   * @param sparkSession
   * @param taskParam
   * @return RDD[(sessionId,action)]
   */
  def getUserVisitAction(sparkSession: SparkSession, taskParam: JSONObject): RDD[(String, UserVisitAction)] = {
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql: String = "select * from user_visit_action where date >= '" + startDate + "' and date <= '" + endDate + "'"

    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item => (item.session_id, item))
  }


}
