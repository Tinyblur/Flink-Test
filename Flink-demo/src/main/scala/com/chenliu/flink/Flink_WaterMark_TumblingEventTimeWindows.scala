package com.chenliu.flink

import java.text.SimpleDateFormat
import java.util.Date

import com.chenliu.bean.Sensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object Flink_WaterMark_TumblingEventTimeWindows {

  def main(args: Array[String]): Unit = {

    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置Flink的时间语义为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置并行度
    env.setParallelism(1)

    //2.读取文件
    val inputDS: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    val mapDS: DataStream[Sensor] = inputDS.map(
      line => {
        val word: Array[String] = line.split(",")
        Sensor(word(0), word(1).toLong, word(2).toDouble)
      }
    )

    //从mapDS中设定时间戳和水位线标记
    //1.从数据中抽取数据作为事件时间
    //2.设定水位线标记watermark,这个标记一般是在上述事件时间的基础上进行推迟
    //当前设置为3说明在当前窗口的基础上推迟3秒进行计算
    val markDS: DataStream[Sensor] = mapDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.seconds(3)) {
        //抽取事件时间，以毫秒为单位
        override def extractTimestamp(element: Sensor): Long = {
          element.ts * 1000
        }
      }
    )

    //timestamp - (timestamp - offset + windowSize) % windowSize
    //1549044128 - 3
    val windowDS: DataStream[String] = markDS.keyBy(_.id)
//      .timeWindow(Time.seconds(5))
//        .window(TumblingEventTimeWindows.of(Time.seconds(5000)))
        //session窗口的划分以当前数据为开始，加3秒为结束
        .window(EventTimeSessionWindows.withGap(Time.seconds(3)))
      .allowedLateness(Time.seconds(2))
      .apply(
        //对窗口进行数据处理
        //key:分流的key
        //window：当前使用窗口的类型
        //iter:窗口中的数据集
        //out：输出
        (key, window, iter, out: Collector[String]) => {
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          out.collect(s"窗口时间：${sdf.format(new Date(window.getStart))}-${sdf.format(new Date(window.getEnd))},数据：${iter.mkString(",")}")
        }
      )

    markDS.print("mark>>>")
    windowDS.print("result>>>")


    //8.Flink是一个流数据处理框架，并且是一个事件驱动的框架
    env.execute()

  }


}
