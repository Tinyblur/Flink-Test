package com.chenliu.flink.common

import com.chenliu.flink.bean.UserBehavior
import org.apache.flink.streaming.api.scala._

/**
  * 通用服务特质
  */
trait TService {

  //获取dao
  def getDao:TDao

  //分析
  def analyses():Any

  /**
    * 获取用户行为的封装数据
    * @return
    */
  protected def getUserBehaviorDS: DataStream[UserBehavior] ={
    val inputDS: DataStream[String] = getDao.readTextFile("input/UserBehavior.csv")

    //将数据转换为样例类
    val userBehaviorDS: DataStream[UserBehavior] = inputDS.map(t => {
      val dataStr: Array[String] = t.split(",")
      UserBehavior(dataStr(0).toLong,
        dataStr(1).toLong,
        dataStr(2).toLong,
        dataStr(3),
        dataStr(4).toLong
      )
    })
    userBehaviorDS
  }

}
