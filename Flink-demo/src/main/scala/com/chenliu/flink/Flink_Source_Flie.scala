package com.chenliu.flink

import com.chenliu.bean.Sensor
import org.apache.flink.streaming.api.scala._

object Flink_Source_Flie {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //从文件读取每一行数据
    val lineDS: DataStream[String] = env.readTextFile("input/sensor.txt")

    val sensorDS: DataStream[Sensor] = lineDS.map(
      line => {
        val str: Array[String] = line.split(",")
        Sensor(str(0), str(1).toLong, str(2).toDouble)
      }
    )

    sensorDS.print("File")

    env.execute()

  }

}
