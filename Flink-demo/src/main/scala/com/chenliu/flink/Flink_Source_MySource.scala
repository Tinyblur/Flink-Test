package com.chenliu.flink

import com.chenliu.bean.Sensor
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object Flink_Source_MySource {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val mySourceDS: DataStream[Sensor] = env.addSource(new MySource)

    mySourceDS.print("mySource")

    env.execute()

  }

  class MySource extends SourceFunction[Sensor] {

    //设置一个标志
    var flag: Boolean = true

    override def cancel(): Unit = {
      flag = false
    }

    override def run(sourceContext: SourceFunction.SourceContext[Sensor]): Unit = {

      while (flag) {
        //采集数据
        sourceContext.collect(
          Sensor(
            "mySource",
            892323,
            new Random().nextInt(10).toDouble
          )
        )
        Thread.sleep(2000)
      }
    }
  }

}
