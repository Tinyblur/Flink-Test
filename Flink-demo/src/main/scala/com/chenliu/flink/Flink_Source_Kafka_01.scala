package com.chenliu.flink

import java.util.Properties

import com.chenliu.bean.Sensor
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object Flink_Source_Kafka_01 {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //从kafka读取数据
    val kafkaDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))
    val sensorDS: DataStream[Sensor] = kafkaDS.map(
      line => {
        val str: Array[String] = line.split(" ")
        Sensor(str(0), str(1).toLong, str(2).toDouble)
      }
    )
    sensorDS.print("kafka")

    env.execute()

  }

}
