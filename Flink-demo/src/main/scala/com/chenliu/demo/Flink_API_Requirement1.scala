package com.chenliu.demo

import com.chenliu.bean.Sensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment,_}
import org.apache.flink.util.Collector

object Flink_API_Requirement1 {

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

    //TODO 监控水位传感器的水位值，如果水位值在5S之内(Event time)连续上升，则报警。
    val processDS: DataStream[String] = keyByDS.process(
      new KeyedProcessFunction[String, Sensor, String] {

        //水位值
        private var currentHeight = 0L
        //定时器
        private var alarmTimer = 0L


        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor, String]#OnTimerContext, out: Collector[String]): Unit = {
          out.collect(s"水位传感器:${ctx.getCurrentKey}连续5s水位上升,当前时间:${ctx.timerService().currentWatermark()}")
        }

        override def processElement(
                                     value: Sensor, //输入数据
                                     ctx: KeyedProcessFunction[String, Sensor, String]#Context, //上下文环境
                                     out: Collector[String] //输出
                                   ): Unit = {
          if (value.height > currentHeight){
            //如果水位值大于上一次的水位，准备触发定时器
            if (alarmTimer == 0){
              alarmTimer = value.ts * 1000 + 5000
              ctx.timerService().registerEventTimeTimer(alarmTimer)
            }
          }else {
            //如果水位值小于上一次的水位，删除定时器
            ctx.timerService().deleteEventTimeTimer(alarmTimer)
            alarmTimer = value.ts * 1000 + 5000
            ctx.timerService().registerEventTimeTimer(alarmTimer)
          }
          currentHeight = value.height.toLong
        }
      })

    wmDS.print("water>>>")
    processDS.print("process>>>")


    env.execute()

  }

}
