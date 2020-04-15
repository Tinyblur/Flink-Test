package com.chenliu.flink.service

import com.chenliu.flink.bean
import com.chenliu.flink.common.{TDao, TService}
import com.chenliu.flink.dao.UniqueVisitorAnalysesDao
import com.chenliu.flink.function.{UniqueVisitorAnalysesByBloomFilterWindowFunction, UniqueVisitorAnalysesWindowFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class UniqueVisitorAnalysesService extends TService {

  private val uniqueVisitorAnalysesDao = new UniqueVisitorAnalysesDao

  override def getDao: TDao = {
    uniqueVisitorAnalysesDao
  }

  def uvAnalyses(): DataStream[String] = {
    //获取用户行为
    val userBehaviorDS: DataStream[bean.UserBehavior] = getUserBehaviorDS

    //抽取时间戳和水位线标记
    val waterDS: DataStream[bean.UserBehavior] = userBehaviorDS.assignAscendingTimestamps(_.timestamp * 1000)

    //进行结构转换
    val userIdToOneDS: DataStream[(Long, Int)] = waterDS.map(t => {
      (t.userId, 1)
    })

    //设定窗口范围
    val dataWS: AllWindowedStream[(Long, Int), TimeWindow] = userIdToOneDS.timeWindowAll(Time.hours(1))

    //判断一个窗口中不重复的用户个数
    val result: DataStream[String] = dataWS.process(new UniqueVisitorAnalysesWindowFunction)

    result
  }

  def uvAnalysesByBloomFilter(): DataStream[String] ={
    //获取用户行为
    val userBehaviorDS: DataStream[bean.UserBehavior] = getUserBehaviorDS

    //抽取时间戳和水位线标记
    val waterDS: DataStream[bean.UserBehavior] = userBehaviorDS.assignAscendingTimestamps(_.timestamp * 1000)

    //进行结构转换
    val userIdToOneDS: DataStream[(Long, Int)] = waterDS.map(t => {
      (t.userId, 1)
    })

    //设定窗口范围
    val dataWS: AllWindowedStream[(Long, Int), TimeWindow] = userIdToOneDS.timeWindowAll(Time.hours(1))

    //通过触发器来实现数据来一条处理一条
    val result: DataStream[String] = dataWS.trigger(
      new Trigger[(Long, Int), TimeWindow]() {
        //与时间无关，我们的数据来一条就处理一条，所以直接continue
        override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
          TriggerResult.CONTINUE
        }

        //与时间无关，我们的数据来一条就处理一条，所以直接continue
        override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
          TriggerResult.CONTINUE
        }

        override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
        }

        //当数据进入到窗口的操作
        override def onElement(element: (Long, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
          //计算并且将计算完的数据清除
          TriggerResult.FIRE_AND_PURGE
        }
      }
    ).process(new UniqueVisitorAnalysesByBloomFilterWindowFunction)

    result

  }

  override def analyses(): DataStream[String] = {
    //UV统计(常规方式)
//    uvAnalyses()

    //使用布隆过滤器的nv统计
    uvAnalysesByBloomFilter()
  }
}
