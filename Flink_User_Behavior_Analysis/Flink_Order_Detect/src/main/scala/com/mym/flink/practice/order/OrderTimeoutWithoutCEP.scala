package com.mym.flink.practice.order

import java.util

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTimeoutWithoutCEP {

  val orderTimeOutPutTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("order timeout")

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

    // 定义process function 进行超时检测
//    val timeWarningStream = orderEventStream.process(new OrderTimeoutWarning())
    val timeWarningStream = orderEventStream.process(new OrderMatch())

    timeWarningStream.print("payed")
    timeWarningStream.getSideOutput(orderTimeOutPutTag).print("timeout")
    env.execute("order timeout without cep job")
  }

  class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{

    // 保存pay是否来过的状态
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      // 先取出状态标识位
      val ispayed = isPayedState.value()

      if(value.eventType == "create" && !ispayed){
        // 如果遇到了create事件，并且pay没有来过，注册定时器开始等待
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 15 * 60 * 1000)
      }else if(value.eventType == "pay"){
        // 如果是pay事件，直接把状态改为true
        isPayedState.update(true)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      // 判断isPayed是否为true
      if(isPayedState.value()){
        out.collect(OrderResult(ctx.getCurrentKey, " order payed succ "))
      } else{
        out.collect(OrderResult(ctx.getCurrentKey, " order payed timeout "))
      }
    }
  }

  class OrderMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{

    // 保存pay是否来过的状态
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

    // 保存定时器的时间戳为状态
    lazy val timeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeState-state", classOf[Long]))

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      // 先读取状态
      val isPayed = isPayedState.value()
      val timeTs = timeState.value()

      // 根据事件类型进行分类判断
      if(value.eventType == "create"){
        // 如果是create，就判断是否pay来过
        if(isPayed){
          // 如果已经pay过，输出主流，清空状态
          out.collect(OrderResult(value.orderId, "payed succ"))
          ctx.timerService().deleteEventTimeTimer(timeTs)
          isPayedState.clear()
          timeState.clear()
        }else{
          // 如果没有pay过
          val ts = value.eventTime * 1000L + 15 * 60 * 1000L
          ctx.timerService().registerEventTimeTimer(ts)
          timeState.update(ts)
        }
      }else if(value.eventType == "pay"){
        // 判断create是否来过
        if(timeTs > 0){
          // 如果有定时器，说明已经有create来过
          // 继续判断是否超过了timeout时间
          if(timeTs > value.eventTime * 1000L ){
            // 如果定时器时间没到，则输出成功匹配
            out.collect(OrderResult(value.orderId, "payed succ"))
          }else{
            ctx.output(orderTimeOutPutTag, OrderResult(value.orderId, " payed but timeout"))
          }
          ctx.timerService().deleteEventTimeTimer(timeTs)
          isPayedState.clear()
          timeState.clear()
        }else{
          // pay 先到 更新状态
          isPayedState.update(true)
          ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L)
          timeState.update(value.eventTime * 1000L)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      // 根据状态的值判断哪个数据没来
      if(isPayedState.value()){
        // pay先到
        ctx.output(orderTimeOutPutTag, OrderResult(ctx.getCurrentKey, "already payed but not fount create log"))
      }else{
        // create 到了，没等到pay
        ctx.output(orderTimeOutPutTag, OrderResult(ctx.getCurrentKey, "order timeout"))
      }
      isPayedState.clear()
      timeState.clear()
    }
  }

}
