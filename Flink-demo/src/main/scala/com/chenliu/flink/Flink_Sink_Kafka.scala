package com.chenliu.flink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object Flink_Sink_Kafka {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDS: DataStream[String] = env.fromCollection(List("a","b","c"))

    inputDS.addSink(new FlinkKafkaProducer011[String]("hadoop102:9092","sensor",new SimpleStringSchema()))

    env.execute()

  }

}
