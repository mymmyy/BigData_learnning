package com.mym.practice.spark.shop.session

import java.util.{Date, Random, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object SessionStat {

  def main(args: Array[String]): Unit = {
    // 获取筛选条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    // 获取筛选条件对应JsonObject
    val taskParam = JSONObject.fromObject(jsonStr)

    // 创建全局唯一主键
    val taskUUID = UUID.randomUUID().toString

    // 创建sparkConf
    val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")

    // 创建sparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 获取原始的动作表数据
    val actionRDD = getOriActionRDD(sparkSession, taskParam)

    // sessionId2ActionRDD: RDD[(sessionId, UserVisitAction)]
    val sessionId2ActionRDD = actionRDD.map(item => (item.session_id, item))
    // 将数据进行内存缓存
    sessionId2ActionRDD.persist(StorageLevel.MEMORY_ONLY)
    // session2GroupActionRDD: RDD[(sessionId, iterable_UserVisitAction)]
    // 将数据转换为Session粒度， 格式为<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
    val sessionid2AggrInfoRDD = this.aggregateBySession(sparkSession, sessionId2ActionRDD)

    val sessionAccumulator = new SessionAccumulator
    sparkSession.sparkContext.register(sessionAccumulator, "sessionAccumulator")

    val sessionId2FilterRDD = getSessionFilteredRDD(taskParam, sessionAccumulator, sessionid2AggrInfoRDD)
    sessionId2FilterRDD.persist(StorageLevel.MEMORY_ONLY)
    // sessionid2detailRDD，就是代表了通过筛选的session对应的访问明细数据
    // sessionid2detailRDD是原始完整数据与（用户 + 行为数据）聚合的结果，是符合过滤条件的完整数据
    // sessionid2detailRDD ( sessionId, userAction )
    val sessionid2detailRDD = getSessionid2detailRDD(sessionId2FilterRDD, sessionId2ActionRDD)
    // 对数据进行内存缓存
    sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY)

    sessionid2AggrInfoRDD.foreach(println(_))
    // 需求1
    getSessionRatio(sparkSession, taskUUID, sessionAccumulator.value)

    // 需求2:session随机抽取随机均匀获取Session，之所以业务功能二先计算，是为了通过Action操作触发所有转换操作。
//    randomExtractSession(sparkSession, taskUUID, sessionId2FilterRDD, sessionid2detailRDD)

    // 需求3：获取TOP10热门品类
    // 返回排名前十的品类是为了在业务功能四中进行使用
//    val top10CategoryList = getTop10Category(sparkSession, taskUUID, sessionid2detailRDD)


    // 关闭Spark上下文
//    sparkSession.close()
  }


  /**
    * 需求3：获取top10热门品类
    *
    * @param sparkSession
    * @param taskUUID
    * @param sessionid2detailRDD
    */
  def getTop10Category(sparkSession: SparkSession, taskUUID: String, sessionid2detailRDD: RDD[(String, UserVisitAction)]) = {
    // TODO
  }

  /**
    * 需求二，随机抽取sesison
    *
    * @param sparkSession
    * @param taskUUID
    * @param sessionId2FilterRDD 过滤后的完整数据
    * @param sessionid2detailRDD 过滤后的用户信息
    */
  def randomExtractSession(sparkSession: SparkSession, taskUUID: String, sessionid2AggrInfoRDD: RDD[(String, String)], sessionid2detailRDD: RDD[(String, UserVisitAction)]): Unit = {
    // first，计算每天每小时的session数量，获取< yyyy-MM-dd_HH, aggrInfo >的格式数据
    val time2sessionidRDD = sessionid2AggrInfoRDD.map{case(sessionid, aggrInfo) =>{
      val startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME)
      // 将key改为yyyy-MM-dd_HH的形式（小时粒度）
      val dateHour = DateUtils.getDateHour(startTime)
      (dateHour, aggrInfo)
    }}

    // 得到每天每小时的session数量
    // countByKey()计算每个不同的key有多少数据
    val countMap = time2sessionidRDD.countByKey()

    // second, 使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引，将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
    val dateHourCountMap = mutable.HashMap[String, mutable.HashMap[String, Long]]()
    for((dateHour, count) <- countMap){
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)
      // 通过模式匹配实现if功能
      dateHourCountMap.get(date) match {
        // 对应日期的数据不存在则新增
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long](); dateHourCountMap(date) += (hour -> count)
        case Some(hourCountMap) => hourCountMap += (hour -> count)
      }
    }

    // 按时间比例随机抽取算法，总共要抽取100个session，先按照天数，进行平分
    val extractNumberPerDay = 100 / dateHourCountMap.size

    // dateHourExtractMap[day, [hour, index列表]]
    val dateHourExtractMap = mutable.HashMap[String, mutable.HashMap[String, mutable.ListBuffer[Int]]]()
    val random = new Random()

    def hourExtractMapFunc(hourExtractMap: mutable.HashMap[String, mutable.ListBuffer[Int]], hourCountMap: mutable.HashMap[String, Long], sessionCount: Long): Unit = {
      for((hour, count) <- hourCountMap){
        // 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
        // 就可以计算出，当前小时需要抽取的session数量
        var hourExtractNumber = ((count / sessionCount.toDouble) * extractNumberPerDay).toInt
        if(hourExtractNumber > count){
          hourExtractNumber = count.toInt
        }

        // 仍然通过模式匹配实现，有则追加，无则新建
        hourExtractMap.get(hour) match {
          case None => hourExtractMap(hour) = new mutable.ListBuffer[Int]();
            // 根据数量随机生成下标
            for(i <- 0 to hourExtractNumber){
              var extractIndex = random.nextInt(count.toInt);
              // 一旦随机生成的index已经存在， 重新获取，直到获取到之前没有的index
              while(hourExtractMap(hour).contains(extractIndex)){
                extractIndex = random.nextInt(count.toInt);
              }
              hourCountMap(hour) += extractIndex
            }
          case Some(extractIndexList) => {
            for(i <- 0 to hourExtractNumber){
              var extractIndex = random.nextInt(count.toInt);
              // 一旦随机生成的index已经存在，重新获取，直到获取到之前没有的index
              while(hourExtractMap(hour).contains(extractIndex)){
                extractIndex = random.nextInt(count.toInt)
              }
              hourExtractMap(hour) += (extractIndex)
            }
          }
        }
      }
    }

    // session随机抽取功能
    for ((date, hourCountMap) <- dateHourCountMap){
      // 计算出这一天的session总数
      val sessionCount = hourCountMap.values.sum
      // dateHourExtractMap[天, [hour, 小时列表]]
      dateHourExtractMap.get(date) match {
        case None => dateHourExtractMap(date) = new mutable.HashMap[String, mutable.ListBuffer[Int]]();
          // 更新index
          hourExtractMapFunc(dateHourExtractMap(date), hourCountMap, sessionCount)
        case Some(hourExtractMap) => hourExtractMapFunc(hourExtractMap, hourCountMap, sessionCount)
      }
    }

    /* 至此，index获取完毕 */

    // 将map广播
    val dateHourExtractMapBroadcast = sparkSession.sparkContext.broadcast(dateHourExtractMap)

    // time2sessionidRDD<yyyy-MM-dd_HH, aggrInfo>
    // 执行groupByKey算子，得到<yyyy-MM-dd_HH, (session, aggrInfo)>
    val time2sessionsRDD = time2sessionidRDD.groupByKey()

    // 第三步：遍历每天每小时的session，然后根据随机索引进行抽取，我们用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的数据
    val sessionRandomExtract = time2sessionsRDD.flatMap{case(dateHour, items) =>{
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      // 从广播变量中提取出数据
      val dateHourExtractMap = dateHourExtractMapBroadcast.value
      // 获取指定天对应的指定小时的indexList
      // 当前小时需要的index集合
      val extractIndexList = dateHourExtractMap.get(date).get(hour)

      // index是在外部进行维护
      var index = 0
      val sessionRandomExtractArray = new ArrayBuffer[SessionRandomExtract]()
      // 开始遍历所有的aggrInfo
      for (sessionAggrInfo <- items) {
        // 如果筛选List中包含当前的index，则提取此sessionAggrInfo中的数据
        if (extractIndexList.contains(index)) {
          val sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
          val starttime = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME)
          val searchKeywords = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
          val clickCategoryIds = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
          sessionRandomExtractArray += SessionRandomExtract(taskUUID, sessionid, starttime, searchKeywords, clickCategoryIds)
        }
        // index自增
        index += 1
      }
      sessionRandomExtractArray
    }}

    /*  将抽取的数据保存到MySQL */
    // 引入隐式转换，准备进行RDD向Dataframe的转换
    import sparkSession.implicits._
    // 为了方便地将数据保存到MySQL数据库，将RDD数据转换为Dataframe
    sessionRandomExtract.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_random_extract")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    // 提取抽取出来的数据中的sessionId
    val extractSessionidsRDD = sessionRandomExtract.map(item => (item.sessionid, item.sessionid))

    // 第四步：获取抽取出来的session的明细数据
    // 根据sessionId与详细数据进行聚合
    val extractSessionDetailRDD = extractSessionidsRDD.join(sessionid2detailRDD)

    // 对extractSessionDetailRDD中的数据进行聚合，提炼有价值的明细数据
    val sessionDetailRDD = extractSessionDetailRDD.map { case (sid, (sessionid, userVisitAction)) =>
      SessionDetail(taskUUID, userVisitAction.user_id, userVisitAction.session_id,
        userVisitAction.page_id, userVisitAction.action_time, userVisitAction.search_keyword,
        userVisitAction.click_category_id, userVisitAction.click_product_id, userVisitAction.order_category_ids,
        userVisitAction.order_product_ids, userVisitAction.pay_category_ids, userVisitAction.pay_product_ids)
    }

    // 将明细数据保存到MySQL中
    sessionDetailRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_detail")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  /**
    * 获取通过筛选条件的session的访问明细数据RDD
    *
    * @param sessionid2aggrInfoRDD 已经通过了过滤的完整的数据
    * @param sessionid2actionRDD 用户行为信息
    * @return
    */
  def getSessionid2detailRDD(sessionid2aggrInfoRDD: RDD[(String, String)], sessionid2actionRDD: RDD[(String, UserVisitAction)]): RDD[(String, UserVisitAction)] = {
    sessionid2aggrInfoRDD.join(sessionid2actionRDD).map(item => (item._1, item._2._2))
  }

  /**
    * 需求一：统计比例
    *
    * @param sparkSession
    * @param taskUUID
    * @param value
    */
  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)


    val sessionRatioRDD = sparkSession.sparkContext.makeRDD(Array(stat))
    import sparkSession.implicits._
    sessionRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_ratio_0416")
      .mode(SaveMode.Append)
      .save()
  }


  def calculateVisitLength(visitLength: Long, sessionAccumulator: SessionAccumulator) = {
    if(visitLength >= 1 && visitLength <= 3){
      sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    }else if(visitLength >=4 && visitLength  <= 6){
      sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    }else if (visitLength >= 7 && visitLength <= 9) {
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

  def calculateStepLength(stepLength: Long, sessionStatisticAccumulator: SessionAccumulator) = {
    if(stepLength >=1 && stepLength <=3){
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
    }else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  def getSessionFilteredRDD(taskParam: JSONObject, sessionAccumulator: SessionAccumulator, sessionId2FullInfoRDD: RDD[(String, String)]) = {
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo =
//      (if(startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
//      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if(filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    sessionId2FullInfoRDD.filter{
      case (sessionId, fullInfo) =>
        var success = true

        if(!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)){
          success = false
        }else if(!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)){
          success = false
        }else if(!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)){
          success = false
        }else if(!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)){
          success = false
        }else if(!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)){
          success = false
        }else if(!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)){
          success = false
        }

        if (success) {
          sessionAccumulator.add(Constants.SESSION_COUNT)
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong

          calculateVisitLength(visitLength, sessionAccumulator)
          calculateStepLength(stepLength, sessionAccumulator)
        }
        success
    }
  }

  /**
    * 获取原始处理数据
    *
    * @param sparkSession
    * @param taskParam
    * @return 原始数据RDD
    */
  def getOriActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date >= '" + startDate + "' and date <= '" + endDate + "'"

    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

  /**
    * 得到session的完整统计信息
    *
    * @param sparkSession
    * @param session2GroupActionRDD
    * @return (userId, 聚合信息)
    */
  def aggregateBySession(sparkSession: SparkSession, session2GroupActionRDD: RDD[(String, UserVisitAction)]): RDD[(String, String)] = {

    // 对行为数据按session粒度进行分组
    val sessionid2ActionsRDD = session2GroupActionRDD.groupByKey()

    // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来，<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
    val userid2PartAggrInfoRDD = sessionid2ActionsRDD.map{
      case (sessionId, userVisitActions) =>
        var userId = -1L
        var startTime:Date = null
        var endTime:Date = null
        var stepLength = 0
        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")

        // 实际上这里要对数据说明一下
        // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
        // 其实，只有搜索行为，是有searchKeyword字段的
        // 只有点击品类的行为，是有clickCategoryId字段的
        // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

        // 我们决定是否将搜索词或点击品类id拼接到字符串中去
        // 首先要满足：不能是null值
        // 其次，之前的字符串中还没有搜索词或者点击品类id

        userVisitActions.foreach { userVisitAction =>
          if(userId == -1L){
            userId = userVisitAction.user_id
          }

          val actionTime = DateUtils.parseTime(userVisitAction.action_time)
          if(startTime == null || startTime.after(actionTime)){
            startTime = actionTime
          }
          if(endTime == null || endTime.before(actionTime)){
            endTime = actionTime
          }

          val searchKeyword = userVisitAction.search_keyword
          if(StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)){
            searchKeywords.append(searchKeyword + ",")
          }

          val clickCategoryId = userVisitAction.click_category_id
          if(clickCategoryId != -1 && !clickCategories.toString.contains(clickCategoryId)){
            clickCategories.append(clickCategoryId + ",")
          }

          stepLength += 1
        }

        // searchKeywords.toString.substring(0, searchKeywords.toString.length)
        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)
        val visitLength = (endTime.getTime - startTime.getTime) / 1000
        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userId, aggrInfo)
    }

    // 查询所有用户数据，并映射成<UserId, Row>格式
    import sparkSession.implicits._
    val userid2InfoRDD = sparkSession.sql("select * from user_info").as[UserInfo].rdd.map(item => (item.user_id, item))
    // 将session粒度聚合数据，与用户信息进行join
    val userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD)

    // 对join起来的数据进行拼接，并且返回<sessionid, fullAggrInfo>格式的数据
    val sessionid2FullAggrInfoRDD = userid2FullInfoRDD.map{
      case(uid, (partAggrInfo, userInfo)) => {
        val sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        val fullAggrInfo = partAggrInfo + "|" +
          Constants.FIELD_AGE + "=" + userInfo.age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
          Constants.FIELD_CITY + "=" + userInfo.city + "|" +
          Constants.FIELD_SEX + "=" + userInfo.sex

        (sessionid, fullAggrInfo)
      }
    }
    sessionid2FullAggrInfoRDD
  }

}
