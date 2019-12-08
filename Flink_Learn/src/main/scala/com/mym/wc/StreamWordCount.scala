package com.mym.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args);
    val host: String = params.get("host");
    val port: Int = params.getInt("port")

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    var testDataStream : DataStream[scala.Predef.String] = null
    if (host.nonEmpty) {
      testDataStream = env.socketTextStream(host, port)
    } else {
      testDataStream = env.socketTextStream("192.168.31.201", 7777)
    }

    // 逐一读取数据，分词后进行wc
    val wordCountDataStream = testDataStream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty).startNewChain()
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    wordCountDataStream.print().setParallelism(1)

    // 执行任务
    env.execute("Stream word count job")
  }
}
