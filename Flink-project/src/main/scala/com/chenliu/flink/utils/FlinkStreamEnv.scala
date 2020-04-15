package com.chenliu.flink.utils

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//Flink的流环境
object FlinkStreamEnv {
  
  private val envLocal = new ThreadLocal[StreamExecutionEnvironment]

  /**
    * 初始化
    */
  def init(): StreamExecutionEnvironment ={

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    envLocal.set(env)

    env
  }

  /**
    * 获取环境
    */
  def get(): StreamExecutionEnvironment ={

    val env: StreamExecutionEnvironment = envLocal.get()

    if (env != null){
      env
    }else{
      init()
    }

  }

  /**
    * 触发执行
    */
  def execute(): Unit ={

    get().execute("application")

  }

  /**
    * 从当前的线程清除env
    */
  def clear(): Unit ={
    envLocal.remove()
  }

}
