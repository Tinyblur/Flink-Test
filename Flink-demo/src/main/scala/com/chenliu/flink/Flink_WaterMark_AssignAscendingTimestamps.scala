package com.chenliu.flink

import java.text.SimpleDateFormat
import java.util.Date

import com.chenliu.bean.Sensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object Flink_WaterMark_AssignAscendingTimestamps{

  def main(args: Array[String]): Unit = {

    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置Flink的时间语义为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置并行度
    env.setParallelism(1)

    //设置获取watermark的周期进行自定义
    env.getConfig.setAutoWatermarkInterval(5000)

    //2.读取文件
    val inputDS: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    val mapDS: DataStream[Sensor] = inputDS.map(
      line => {
        val word: Array[String] = line.split(",")
        Sensor(word(0), word(1).toLong, word(2).toDouble)
      }
    )

    //从mapDS中设定时间戳和水位线标记
    //有序的情况，即当前的时间是25的话那么25之前的数据就都已经提交
    val markDS: DataStream[Sensor] = mapDS.assignAscendingTimestamps(_.ts * 1000)

    val outputTag = new OutputTag[Sensor]("lateData")

    // TODO timestamp - (timestamp - offset + windowSize) % windowSize
    //1549044128 - 3
    val windowDS: DataStream[String] = markDS.keyBy(_.id).timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(2))
        .sideOutputLateData(outputTag)
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

    //获取迟到数据
    val lataDataDS: DataStream[Sensor] = windowDS.getSideOutput(outputTag)

    markDS.print("mark>>>")
    windowDS.print("result>>>")
    lataDataDS.print("lateData>>>")


    //8.Flink是一个流数据处理框架，并且是一个事件驱动的框架
    env.execute()

  }


}
