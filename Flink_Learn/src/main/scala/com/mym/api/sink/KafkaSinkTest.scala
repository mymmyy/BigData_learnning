package com.mym.api.sink

import java.util.Properties

import com.mym.api.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object KafkaSinkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 读入测试数据
    val streamPath = env.readTextFile("G:\\Dev\\Git\\MYM\\BigData_learnning\\data\\sensor.txt")
    // map
    val sourceStream = streamPath.map(
      data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    )
    sourceStream.print("原始测试数据：")

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

//    val inputStream = env.addSource(new FlinkKafkaConsumer010[String]("sensor", new SimpleStringSchema(), properties))
    // Transform操作
    val dataStream = sourceStream.map(data => data.toString)

    // sink
    dataStream.addSink( new FlinkKafkaProducer010[String]("mym-sink", new SimpleStringSchema(), properties))
    dataStream.print("send to kafka")

    env.execute("kafka sink test")
  }

}
