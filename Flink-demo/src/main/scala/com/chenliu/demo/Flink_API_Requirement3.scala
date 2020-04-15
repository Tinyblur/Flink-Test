package com.chenliu.demo

import com.chenliu.bean.Sensor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object Flink_API_Requirement3 {

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
//        private var currentHeight = 0L
        private var currentHeight : ValueState[Long] = _
        //定时器
//        private var alarmTimer = 0L
        private var alarmTimer : ValueState[Long] = _

        //给有状态的数据类型进行初始化，有两种方式
        //1.延迟加载：lazy
        //2.open：生命周期

        override def open(parameters: Configuration): Unit = {
          currentHeight = getRuntimeContext.getState(
            new ValueStateDescriptor[Long]("currentHeight",classOf[Long])
          )
          alarmTimer = getRuntimeContext.getState(
            new ValueStateDescriptor[Long]("alarmTimer",classOf[Long])
          )
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor, String]#OnTimerContext, out: Collector[String]): Unit = {
          out.collect(s"水位传感器:${ctx.getCurrentKey}连续5s水位上升,当前时间:${ctx.timerService().currentWatermark()}")
        }

        override def processElement(
                                     value: Sensor, //输入数据
                                     ctx: KeyedProcessFunction[String, Sensor, String]#Context, //上下文环境
                                     out: Collector[String] //输出
                                   ): Unit = {

          //获取状态类型的属性的值
//          currentHeight.value()


          if (value.height >  currentHeight.value()){
            //如果水位值大于上一次的水位，准备触发定时器
            if (alarmTimer.value() == 0){
              //更新有状态的属性的值
              alarmTimer.update(value.ts * 1000 + 5000)
//              alarmTimer = value.ts * 1000 + 5000
              ctx.timerService().registerEventTimeTimer(alarmTimer.value())
            }
          }else {
            //如果水位值小于上一次的水位，删除定时器
            ctx.timerService().deleteEventTimeTimer(alarmTimer.value())
//            alarmTimer = value.ts * 1000 + 5000
            alarmTimer.update(value.ts * 1000 + 5000)
            ctx.timerService().registerEventTimeTimer(alarmTimer.value())
          }
          currentHeight.update(value.height.toLong)
        }
      })

    wmDS.print("water>>>")
    processDS.print("process>>>")


    env.execute()

  }

}
