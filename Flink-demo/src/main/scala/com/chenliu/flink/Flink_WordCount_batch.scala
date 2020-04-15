package com.chenliu.flink

import org.apache.flink.api.scala._

object Flink_WordCount_batch {

  def main(args: Array[String]): Unit = {

    //1.创建环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2.读取文件
    val inputDS: DataSet[String] = environment.readTextFile("input")

    //3.对每一行数据进行扁平化
    val wordDS: DataSet[String] = inputDS.flatMap(_.split(" "))

    //4.对单词进行结构变化
    val wordToOneDS: DataSet[(String, Int)] = wordDS.map((_,1))

    //5.对单词进行分组
    //0代表(String,Int)中的第0位也就是String
    val groupByWordDS: GroupedDataSet[(String, Int)] = wordToOneDS.groupBy(0)

    //6.对分组之后的kv进行求和操作
    val wordToSumDS: AggregateDataSet[(String, Int)] = groupByWordDS.sum(1)

    //7.打印结果
    wordToSumDS.print()

  }


}
