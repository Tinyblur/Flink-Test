package com.chenliu.flink

import com.chenliu.bean.Sensor
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object Flink_UDF_RichFunctions {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDS: DataStream[String] = env.readTextFile("input/sensor.txt")

    val richMapDS: DataStream[Sensor] = inputDS.map(new MyRichMap)

    richMapDS.print("richMap")

    env.execute()

  }

  class MyRichMap extends RichMapFunction[String,Sensor] {
    override def map(value: String): Sensor = {
      val str: Array[String] = value.split(",")
      Sensor(str(0),str(1).toLong,str(2).toDouble)
    }

    override def open(parameters: Configuration): Unit = super.open(parameters)

    override def close(): Unit = super.close()
  }

}
