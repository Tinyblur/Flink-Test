package com.chenliu.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object Flink_WordCount_unbounded {

  def main(args: Array[String]): Unit = {

    //1.创建环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    environment.setParallelism(2)

    //2.读取文件
    val inputDS: DataStream[String] = environment.socketTextStream("hadoop102",9999)

    //3.对每一行数据进行扁平化
    val wordDS: DataStream[String] = inputDS.flatMap(_.split(" "))

    //4.对单词进行结构变化
    val wordToOneDS: DataStream[(String, Int)] = wordDS.map((_,1)).setParallelism(4)

    //5.对单词进行分组
    //流处理中可以根据key进行分流操作
    val groupByWordDS: KeyedStream[(String, Int), String] = wordToOneDS.keyBy(_._1)

    //6.对分组之后的kv进行求和操作
    val wordToSumDS: DataStream[(String, Int)] = groupByWordDS.sum(1).setParallelism(1)

    //7.打印结果
    wordToSumDS.print()

    //8.Flink是一个流数据处理框架，并且是一个事件驱动的框架
    environment.execute()

  }


}
