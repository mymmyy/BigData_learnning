package com.mym.api.operator

import com.mym.api.source.SensorReading
import org.apache.flink.streaming.api.scala._

/**
  * 转换算子
  */
object TransformTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读入数据
    val streamPath = env.readTextFile("G:\\Dev\\Git\\MYM\\BigData_learnning\\data\\sensor.txt")

    // Transform操作
    // map
    val dataStream = streamPath.map(
      data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    )
    dataStream.print("after map")

    // flatMap
    val flatMapData = List("f", "a b", "c d").flatMap(line => line.split(" "))
    println(flatMapData)

    // filter
    val filterData = flatMapData.filter(data => data != "f")
    println(filterData)

    // keyBy
    val temperatureKeyBy = dataStream.keyBy("id").sum("temperature")
    temperatureKeyBy.print("temperatureKeyBy")
    // keyBy 聚合操作
    val reduceDataStream = dataStream.keyBy("id").reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))
    reduceDataStream.print("reduceDataStream")
    // 分流，根据温度是否大于30度划分
    val splitStream = dataStream.split(sensorData => {
      if(sensorData.temperature > 30) Seq("high") else Seq("low")
    })
    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")
    val allTempStream = splitStream.select("high", "low")
    highTempStream.print("highTempStream")
    lowTempStream.print("lowTempStream")
    allTempStream.print("allTempStream")

    // 合并流
    val warningStream = highTempStream.map(sensorData => (sensorData.id, sensorData.timestamp))
    warningStream.print("warningStream")
    val connectedStreams = warningStream.connect(lowTempStream)
    println("connectedStreams:" + connectedStreams)

    val coMapStream = connectedStreams.map(
      warningData => (warningData._1 ,warningData._2, "high temperature warning"),
      lowData => ( lowData.id, "healthy" )
    )
    coMapStream.print("coMapStream")

    val unionStream = highTempStream.union(lowTempStream)
    unionStream.print("unionStream")


    // 函数

    env.execute()
  }

}
