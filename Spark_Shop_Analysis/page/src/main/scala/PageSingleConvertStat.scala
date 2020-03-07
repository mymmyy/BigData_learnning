import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object PageSingleConvertStat {

  case class PageSplitConvertRate(taskid: String, convertRate: String)

  def main(args: Array[String]): Unit = {
    // 获取任务限制条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    val taskUUID = UUID.randomUUID().toString

    // 创建SparkConf
    val sparkConf = new SparkConf().setAppName("pageConvert").setMaster("local[*]")
    // 创建sparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 获取用户行为数据
    val sessionId2ActionRDD = getUserVisitAction(sparkSession, taskParam);

    val pageFlowStr = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    val pageFlowArray = pageFlowStr.split(",")
    val targetPageSplit = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail).map{
      case(page1, page2) => page1 + "_" + page2
    }

    // sessionId2ActionRDD: RDD[(sessionId, action)]
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()

    // pageSpllitNumRDD: RDD[(String, 1L)]
    val pageSplitNumRDD = sessionId2GroupRDD.flatMap{
      case(sessionId, iterableAction) =>
        val sortList = iterableAction.toList.sortWith((item1, item2) =>{
          DateUtils.parseTime(item1.action_time).getTime < DateUtils.parseTime(item2.action_time).getTime
        })

        val pageList = sortList.map{
          case action => action.page_id
        }

        val pageSplit = pageList.slice(0, pageList.length - 1).zip(pageList.tail).map{
          case(page1, page2) => page1 + "_" + page2
        }
        val pageSplitFilter = pageSplit.filter({
          case pageSplit => targetPageSplit.contains(pageSplit)
        })
        pageSplitFilter.map{
          case pageSplit => (pageSplit, 1L)
        }
    }

    // pageSplitCountMap: Map[(pageSplit, count)]
    val pageSplitCountMap = pageSplitNumRDD.countByKey()
    val startPage = pageFlowArray(0).toLong

    // count(1)
    val startPageCount = sessionId2ActionRDD.filter{
      case(sessionId, action) => action.page_id == startPage
    }.count()

    getPageConvert(sparkSession, taskUUID, targetPageSplit, startPageCount, pageSplitCountMap)
  }

  def getUserVisitAction(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" +
      endDate + "'"

    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item => (item.session_id, item))
  }

  def getPageConvert(sparkSession: SparkSession,
                     taskUUID: String,
                     targetPageSplit: Array[String],
                     startPageCount: Long,
                     ageSplitCountMap: collection.Map[String, Long]): Unit = {
    val pageSplitRatio = new mutable.HashMap[String, Double]()
    var lastPageCount = startPageCount.toDouble

    for(pageSplit <- targetPageSplit){
      val currentPageSplitCount = ageSplitCountMap.get(pageSplit).get.toDouble
      val ratio = currentPageSplitCount / lastPageCount
      pageSplitRatio.put(pageSplit, ratio)
      lastPageCount = currentPageSplitCount
    }
    val convertStr = pageSplitRatio.map{case(pageSplit, ratio) => {pageSplit + "=" + ratio}}.mkString("|")

    val pageSplit = PageSplitConvertRate(taskUUID, convertStr)
    val pageSplitRatioRDD = sparkSession.sparkContext.makeRDD(Array(pageSplit))

    import sparkSession.implicits._
    pageSplitRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate_0308")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

  }

}
