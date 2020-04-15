package com.chenliu.flink

import org.apache.flink.streaming.api.scala._

object Flink_Transform_Connect {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDS1: DataStream[(String, Int)] = env.fromCollection(List(("a", 1), ("b", 47), ("a", 9)))

    val inputDS2: DataStream[(String, Int)] = env.fromCollection(List(("a", 13), ("b", 7), ("b", 17)))

    val connectDS: ConnectedStreams[(String, Int), (String, Int)] = inputDS1.connect(inputDS2)

    //此处map的时候要想让x,y能识别出类型必须先补全结构：
//    x=>{
//
//    },
//    y=>{
//
//    }
    val value: DataStream[(String, Int)] = connectDS.map(
      x => {
        (x._1,x._2*2)
      },
      y => {
        (y._1,y._2*3)
      }
    )

    value.print("CoMap")

    env.execute()

  }

}
