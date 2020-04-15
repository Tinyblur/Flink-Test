package com.chenliu.flink

import com.chenliu.bean.Sensor
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object Flink_Source_MySource_01 {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val mysourceDS: DataStream[Sensor] = env.addSource(new MySource01)

    mysourceDS.print("mySource")

    env.execute()

  }

  class MySource01 extends SourceFunction[Sensor]{

    var flag :Boolean = true

    override def cancel(): Unit = {
      flag = false
    }

    override def run(ctx: SourceFunction.SourceContext[Sensor]): Unit = {

      while (flag){
        ctx.collect(Sensor("mySource",19328424,new Random().nextInt(10)))
        Thread.sleep(2000)
      }

    }
  }

}
