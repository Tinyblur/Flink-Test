package com.chenliu.flink

import java.text.SimpleDateFormat
import java.util.Date

import com.chenliu.bean.Sensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object Flink_ProcessFunctionAPI_Keyed {

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

    val wmDS: DataStream[Sensor] = mapDS.assignAscendingTimestamps(_.ts * 1000)

    val keyByDS: KeyedStream[Sensor, String] = wmDS.keyBy(_.id)

    //每来一条数据就触发一次
    val processDS: DataStream[String] = keyByDS.process(new KeyedProcessFunction[String, Sensor, String] {
     override def processElement(
                                       value: Sensor, //输入数据
                                       ctx: KeyedProcessFunction[String, Sensor, String]#Context, //上下文环境
                                       out: Collector[String] //输出
                                     ): Unit = {
        ctx.getCurrentKey
//        ctx.output()将数据采集到测输出流
        ctx.timerService().currentProcessingTime()
        ctx.timerService().currentWatermark()
        out.collect("keyedProcess" + ctx.timestamp())
      }
    })

    processDS.print("process>>>")


    //8.Flink是一个流数据处理框架，并且是一个事件驱动的框架
    env.execute()

  }


}
