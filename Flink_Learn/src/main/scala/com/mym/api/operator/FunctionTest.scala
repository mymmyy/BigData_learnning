package com.mym.api.operator

import com.mym.api.source.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, RichFlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
  * 函数
  */
object FunctionTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 读入测试数据
    val streamPath = env.readTextFile("G:\\Dev\\Git\\MYM\\BigData_learnning\\data\\sensor.txt")
    // map
    val dataStream = streamPath.map(
      data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    )
    dataStream.print("原始测试数据：")

    // 使用自定义函数
    val testMyFilterResult = dataStream.filter(new MyTestFilter)
    testMyFilterResult.print("testMyFilterResult")

    // 使用匿名函数
    val noNameFilterResult = dataStream.filter(new FilterFunction[SensorReading] {
      override def filter(t: SensorReading): Boolean = {
        t.id.contains("sensor_1")
      }
    })
    noNameFilterResult.print("noNameFilterResult")

    // 使用匿名函数 Lambda
    val noNameFilterLambdaResult = dataStream.filter(data => data.id.contains("sensor_1"))
    noNameFilterLambdaResult.print("noNameFilterLambdaResult")

    // 富函数
    /*
          “富函数”是 DataStream API 提供的一个函数类的接口，所有 Flink 函数类都
      有其 Rich 版本。它与常规函数的不同在于，可以获取运行环境的上下文，并拥有一
      些生命周期方法，所以可以实现更复杂的功能。
       RichMapFunction
       RichFlatMapFunction
       RichFilterFunction
       …
      Rich Function 有一个生命周期的概念。典型的生命周期方法有：
       open()方法是 rich function 的初始化方法，当一个算子例如 map 或者 filter
      被调用之前 open()会被调用。
       close()方法是生命周期中的最后一个调用的方法，做一些清理工作。
       getRuntimeContext()方法提供了函数的 RuntimeContext 的一些信息，例如函
      数执行的并行度，任务的名字，以及 state 状态
     */
    val richFlatMapResult = dataStream.flatMap(new myFlatMapRich)
    richFlatMapResult.print("richFlatMapResult")

    env.execute()
  }

}

/**
  * 显示定义自定义函数
  */
class MyTestFilter extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = {
    t.id.contains("sensor_1")
  }
}

class myFlatMapRich extends RichFlatMapFunction[SensorReading, (String, Double)]{
  override def flatMap(in: SensorReading, collector: Collector[(String, Double)]): Unit = {
    if (in.temperature > 30) {
      collector.collect((in.id, in.temperature))
    }
  }

  /**
    * 富函数 open
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = super.open(parameters)

  /**
    * 富函数 close
    */
  override def close(): Unit = {
    super.close()
    // 可以做一些资源关闭操作
  }
}
