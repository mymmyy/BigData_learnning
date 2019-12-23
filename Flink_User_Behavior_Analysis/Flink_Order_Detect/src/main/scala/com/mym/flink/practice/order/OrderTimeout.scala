package com.mym.flink.practice.order

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
// 定义输入订单事件的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

// 定义输出结果样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 定义一个匹配模式
    val orderPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 把模式应用到stream上，得到一个pattern stream
    val patternStream = CEP.pattern(orderEventStream, orderPattern)

    // 调用select方法，提取事件序列，超时的事件需要做报警处理
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    val resultStream = patternStream.select( orderTimeoutOutputTag, new OrderTimeoutSelect(), new OrderPaySelect())

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout job")
  }

  class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
    override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
      val timeoutOrderId = map.get("begin").iterator().next().orderId
      OrderResult(timeoutOrderId, "timeout")
    }
  }

  class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult]{
    override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
      val payedOrderId = map.get("follow").iterator().next().orderId
      OrderResult(payedOrderId, "payed")
    }
  }

}
