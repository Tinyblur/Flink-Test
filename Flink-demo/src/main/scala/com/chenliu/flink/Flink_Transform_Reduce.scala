package com.chenliu.flink

import org.apache.flink.streaming.api.scala._

object Flink_Transform_Reduce {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDS: DataStream[(String, Int)] = env.fromCollection(List(("a",1),("b",7),("a",9),("b",17)))

    val keyByDS: KeyedStream[(String, Int), String] = inputDS.keyBy(_._1)

    val reduceDS: DataStream[(String, Int)] = keyByDS.reduce(
      (a, b) => {
        (a._1, a._2 + b._2)
      }
    )
    reduceDS.print("reduce")

    env.execute()

  }

}
