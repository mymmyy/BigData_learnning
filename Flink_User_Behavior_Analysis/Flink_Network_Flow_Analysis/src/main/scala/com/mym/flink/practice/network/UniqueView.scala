package com.mym.flink.practice.network

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


// 定义数据样例类
case class UvCount( windowEnd: Long, uvCount: Long )

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

object UniqueView {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("D:\\GitTest\\BigData_learnning\\Flink_User_Behavior_Analysis\\Flink_Network_Flow_Analysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .apply( new UvCountByWindow() )



    dataStream.print("aggregate")

    env.execute("uv job")
  }


  class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
    override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
      // 定义一个scala set，用于保存所有的数据userId并去重
      var idSet = Set[Long]()
      // 把当前窗口所有数据的ID收集到set中，最后输出set的大小
      for( userBehavior <- input ){
        idSet += userBehavior.userId
      }
      out.collect( UvCount( window.getEnd, idSet.size ) )
    }
  }

}
