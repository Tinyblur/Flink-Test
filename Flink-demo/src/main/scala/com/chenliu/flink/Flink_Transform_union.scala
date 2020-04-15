package com.chenliu.flink

import org.apache.flink.streaming.api.scala._

object Flink_Transform_union {

  def main(args: Array[String]): Unit = {
    /**
      * union 和connect的区别
      * 1.union的两个DataStream的类型必须相同
      * 2.connect只能操作两个流，union可以操作多个
      */

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDS1: DataStream[(String, Int)] = env.fromCollection(List(("a",1),("b",7)))

    val inputDS2: DataStream[(String, Int)] = env.fromCollection(List(("c",9),("d",17)))

    val unionDS: DataStream[(String, Int)] = inputDS1.union(inputDS2)

    unionDS.print("union")

    env.execute()

  }

}
