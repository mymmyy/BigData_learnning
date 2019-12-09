package com.mym.api.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.kafka.common.serialization.StringDeserializer

import scala.util.Random

// 定义数据样例
case class SensorReading(id: String, timestamp: Long, temperature: Double)

/**
  * 数据源测试 by mym
  */
object SourceTest {

  def main(args: Array[String]):Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 从集合中读取数据
    val streamList = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_2", 1547718201, 15.80018327300259),
      SensorReading("sensor_3", 1547718202, 6.80018327300259),
      SensorReading("sensor_4", 1547718205, 38.80018327300259)
    ))

    streamList.print("streamList")

    // 从文件读取数据
    val streamFile = env.readTextFile("G:\\Dev\\Git\\MYM\\BigData_learnning\\data\\worldcount.txt")
    streamFile.print("streamFile")

    // 从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.1.105:9092")
    properties.setProperty("group.id", "mym-test-consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val streamKafka = env
      .addSource(new FlinkKafkaConsumer010[String]("mym-sensor", new SimpleStringSchema(), properties))
    streamKafka.print("streamKafka")

    // 自定义source
    val streamDefSource = env.addSource(new MySensorSource())
    streamDefSource.print("streamDefSource")
    env.execute()
  }

}

/**
  * 自定义source
  */
class MySensorSource extends SourceFunction[SensorReading]{
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    var curTemp = 1.to(10).map(i => ("sensor_" + i, 66 + rand.nextGaussian() * 20))

    while(running){
      // 更新字段值
      curTemp = curTemp.map(t => (t._1, t._2  + rand.nextGaussian()))

      // 获取时间戳
      val curTime = System.currentTimeMillis()

      curTemp.foreach(t => sourceContext.collect(SensorReading(t._1, curTime, t._2)))

      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = {
    running = false
  }

  var running: Boolean = true
}
