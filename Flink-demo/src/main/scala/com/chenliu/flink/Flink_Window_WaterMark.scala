package com.chenliu.flink

import java.text.SimpleDateFormat
import java.util.Date

import com.chenliu.bean.Sensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object Flink_Window_WaterMark {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputDS: DataStream[String] = env.socketTextStream("hadoop102",9999)

    val sensorDS: DataStream[Sensor] = inputDS.map(
      t => {
        val str: Array[String] = t.split(",")
        Sensor(str(0), str(1).toLong, str(2).toDouble)
      }
    )

    val markDS: DataStream[Sensor] = sensorDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.seconds(3)) {
        override def extractTimestamp(element: Sensor): Long = {
          element.ts * 1000L
        }
      }
    )

    val resultDS: DataStream[String] = markDS.keyBy(_.id).timeWindow(Time.seconds(5)).apply(
      (str, timeWindow, iter, collect: Collector[String]) => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        collect.collect(s"window:[${sdf.format(new Date(timeWindow.getStart))}--${sdf.format(new Date(timeWindow.getEnd))})")
        collect.collect(s"内容:${iter.mkString(",")}")
      }
    )

    markDS.print("mark>>>")

    resultDS.print("result>>>")

    env.execute()

  }
}
