package com.chenliu.flink.service

import com.chenliu.flink.bean.LoginEvent
import com.chenliu.flink.common.{TDao, TService}
import com.chenliu.flink.dao.LoginFailAnalysesDao
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

class LoginFailAnalysesService extends TService {

  private val loginFailAnalysesDao = new LoginFailAnalysesDao

  override def getDao: TDao = loginFailAnalysesDao

  def analysesNomal(): DataStream[String] ={
    val dataDS: DataStream[String] = loginFailAnalysesDao.readTextFile("input/LoginLog.csv")

    val loginDataDS: DataStream[LoginEvent] = dataDS.map(
      t => {
        val str: Array[String] = t.split(",")
        LoginEvent(
          str(0).toLong,
          str(1),
          str(2),
          str(3).toLong
        )
      }
    )

    //设置eventTime
    val timeDS: DataStream[LoginEvent] = loginDataDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.minutes(1)) {
        override def extractTimestamp(element: LoginEvent): Long = {
          element.eventTime * 1000
        }
      }
    )

    //将登录失败的过滤出来
    val result: DataStream[String] = timeDS.filter(_.eventType == "fail").keyBy(_.userId).process(
      //I,O
      new KeyedProcessFunction[Long, LoginEvent, String] {

        private var lastLoginEvent: ValueState[LoginEvent] = _

        override def open(parameters: Configuration): Unit = {
          lastLoginEvent = getRuntimeContext.getState(
            new ValueStateDescriptor[LoginEvent]("lastLoginEvent", classOf[LoginEvent])
          )
        }

        override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, String]#Context, out: Collector[String]): Unit = {
          val lastEvent = lastLoginEvent.value()
          if (lastEvent != null){
            if (value.eventTime - lastEvent.eventTime <= 2) {
              out.collect(value.userId + "在连续两秒内登录失败两次")
            }
          }
          lastLoginEvent.update(value)
        }
      }
    )
    result
  }

  def analysesWithCEP(): DataStream[String] ={
    val dataDS: DataStream[String] = loginFailAnalysesDao.readTextFile("input/LoginLog.csv")

    val loginDataDS: DataStream[LoginEvent] = dataDS.map(
      t => {
        val str: Array[String] = t.split(",")
        LoginEvent(
          str(0).toLong,
          str(1),
          str(2),
          str(3).toLong
        )
      }
    )

    //设置eventTime
    val timeDS: DataStream[LoginEvent] = loginDataDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.minutes(1)) {
        override def extractTimestamp(element: LoginEvent): Long = {
          element.eventTime * 1000
        }
      }
    )

    val keyByUserIdKS: KeyedStream[LoginEvent, Long] = timeDS.keyBy(_.userId)

    val patternData: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      //定义时间范围
      .within(Time.seconds(2))

    //应用规则
    val userLogPS: PatternStream[LoginEvent] = CEP.pattern(keyByUserIdKS,patternData)

    userLogPS.select(
      t => {
        t.toString()
      }
    )

  }

  override def analyses(): DataStream[String] = {
//    analysesNomal()
    analysesWithCEP()
  }
}
