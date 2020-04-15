package com.chenliu.flink

import org.apache.flink.streaming.api.scala._

object Flink_Transform_split {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDS: DataStream[(String, Int)] = env.fromCollection(List(("a",1),("b",7),("a",9),("b",17)))

    val splitStream: SplitStream[(String, Int)] = inputDS.split(
      t => {
        if ("a".equals(t._1)) {
          Seq("a")
        } else {
          Seq("b")
        }
      }
    )

    val aDS: DataStream[(String, Int)] = splitStream.select("a")
    val bDS: DataStream[(String, Int)] = splitStream.select("b")
//    splitStream.print("split")

    aDS.print("a")
    bDS.print("b")
    env.execute()

  }

}
