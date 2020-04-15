package com.chenliu.flink.common

import java.util.Properties

import com.chenliu.flink.utils.FlinkStreamEnv
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * 通用数据访问特质
  */
trait TDao {

  //从文件读取
  def readTextFile(implicit path:String): DataStream[String] ={
    FlinkStreamEnv.get().readTextFile(path)
  }

  //从kafka读取
  def readKafka(): DataStream[String] ={
    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    FlinkStreamEnv.get().addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))
  }

  //读取socket网络数据
  def readSocket(): Unit ={

  }

}
