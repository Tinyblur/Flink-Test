package com.chenliu.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Flink_Window_TimeWindow {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDS: DataStream[String] = env.socketTextStream("hadoop102",9999)

    val wordDS: DataStream[String] = inputDS.flatMap(_.split(" "))

    val valueDS: DataStream[(String, Int)] = wordDS.map((_, 1)).keyBy(_._1).timeWindow(Time.seconds(3)).reduce(
      (a, b) => {
        (a._1, a._2 + b._2)
      }
    )

    valueDS.print("TimeWindow")

    env.execute()

  }
}
