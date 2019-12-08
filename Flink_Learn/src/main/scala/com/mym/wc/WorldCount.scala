package com.mym.wc

import org.apache.flink.api.scala._

object WorldCount {

  def main(args: Array[String]):Unit = {
    // 创建一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读
    // 取数据
    val inputPath = "D:\\GitTest\\BigData_learnning\\data\\worldcount.txt"
    val inputDataSet = env.readTextFile(inputPath)

    // 切分数据得到word，然后在对word部分进行分组聚合
    val worldCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    worldCountDataSet.print()
  }
}
