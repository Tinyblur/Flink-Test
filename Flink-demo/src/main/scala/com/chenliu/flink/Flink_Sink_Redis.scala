package com.chenliu.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object Flink_Sink_Redis {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDS: DataStream[String] = env.fromCollection(List("a","b","c"))

    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()

    inputDS.addSink(new RedisSink[String](config,new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET,"sensor")
      }

      override def getValueFromData(t: String): String = {
        t
      }

      override def getKeyFromData(t: String): String = {
        t
      }
    }))

    env.execute()

  }

}
