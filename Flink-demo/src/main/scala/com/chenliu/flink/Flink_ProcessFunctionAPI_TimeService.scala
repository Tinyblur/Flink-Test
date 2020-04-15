package com.chenliu.flink

import com.chenliu.bean.Sensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Flink_ProcessFunctionAPI_TimeService {

  def main(args: Array[String]): Unit = {

    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置Flink的时间语义为EventTime
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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

//    val wmDS: DataStream[Sensor] = mapDS.assignAscendingTimestamps(_.ts * 1000)

    val keyByDS: KeyedStream[Sensor, String] = mapDS.keyBy(_.id)

    val processDS: DataStream[String] = keyByDS.process(new KeyedProcessFunction[String, Sensor, String] {


      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor, String]#OnTimerContext, out: Collector[String]): Unit = {
        //定时器在指定的时间点触发该方法的执行
        out.collect("timer execute...")
      }

      //每来一条数据就触发一次
      override def processElement(
                                   value: Sensor, //输入数据
                                   ctx: KeyedProcessFunction[String, Sensor, String]#Context, //上下文环境
                                   out: Collector[String] //输出
                                 ): Unit = {
//        ctx.output()将数据采集到测输出流
        ctx.timerService().currentProcessingTime()
        out.collect("keyedProcess" + ctx.timestamp())
        //注册定时器
        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime())
      }
    })

    processDS.print("process>>>")


    //8.Flink是一个流数据处理框架，并且是一个事件驱动的框架
    env.execute()

  }


}
