package com.chenliu.flink

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

object Flink_Window_Function_ProcessFunction {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDS: DataStream[String] = env.socketTextStream("hadoop102",9999)

    val wordDS: DataStream[String] = inputDS.flatMap(_.split(" "))

    val valueDS: DataStream[String] = wordDS.map((_, 1)).keyBy(_._1).timeWindow(Time.seconds(3)).process(
      //IN, OUT, KEY, W <: Window
      new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
        override def process(
                              key: String,
                              context: Context,
                              elements: Iterable[(String, Int)], //窗口中所有相同key的数据
                              out: Collector[String] //用于将数据输出到Sink
                            ): Unit = {
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          out.collect("窗口启动时间:" + sdf.format(new Date(context.window.getStart)))
          out.collect("窗口结束时间:" + sdf.format(new Date(context.window.getEnd)))
          out.collect("窗口内容为:" + elements.toList)
          out.collect("============================")

        }
      }
    )

    valueDS.print("TimeWindow")

    env.execute()

  }
}
