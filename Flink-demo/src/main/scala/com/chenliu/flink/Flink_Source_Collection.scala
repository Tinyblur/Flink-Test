package com.chenliu.flink

import com.chenliu.bean.Sensor
import org.apache.flink.streaming.api.scala._

object Flink_Source_Collection {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //从集合获取数据
    val collectionDS: DataStream[Sensor] = env.fromCollection(
      List(Sensor("collection", 1001, 1),
        Sensor("collection", 1002, 2)
      )
    )

    collectionDS.print("collect")

    env.execute()

  }

}
