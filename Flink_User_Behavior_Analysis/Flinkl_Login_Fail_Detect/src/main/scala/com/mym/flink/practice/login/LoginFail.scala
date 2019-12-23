package com.mym.flink.practice.login

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector



// 输入的登录事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 输出的异常报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取事件数据
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime
      })

    val warningStream = loginEventStream
      .keyBy(_.userId)
      .process(new LoginWarnning())

    warningStream.print()

    env.execute("login warn job")
  }

  class LoginWarnning() extends KeyedProcessFunction[Long, LoginEvent, Warning] {

    // 定义状态，保存2秒内的所有登录失败事件
    lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
      if (value.eventType == "fail") {
        // 如果是失败，判断之前是否有登录失败事件
        val iter = loginFailState.get().iterator()
        if (iter.hasNext) {
          // 如果已经有登录失败事件，就比较事件时间
          val firstFail = iter.next()
          if (value.eventTime < firstFail.eventTime + 2) {
            // 如果两次间隔小于2秒，输出报警
            out.collect(Warning(value.userId, firstFail.eventTime, value.eventTime, "login fail in 2 seconds."))
          }
          // 更新最近一次的登录失败事件，保存在状态里
          loginFailState.clear()
          loginFailState.add(value)
        } else {
          // 如果是第一次登录失败，直接添加到状态
          loginFailState.add(value)
        }
      } else {
        // 如果是成功，清空状态
        loginFailState.clear()
      }
    }
  }

}
