package com.chenliu.flink.service

import com.chenliu.flink.bean
import com.chenliu.flink.common.{TDao, TService}
import com.chenliu.flink.dao.PageViewAnalysesDao
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream,_}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class PageViewAnalysesService extends TService{

  private val pageViewAnalysesDao = new PageViewAnalysesDao

  override def getDao: TDao = pageViewAnalysesDao

  override def analyses(): DataStream[(String, Int)] = {

    val dataDS: DataStream[bean.UserBehavior] = getUserBehaviorDS

    val asTDS: DataStream[bean.UserBehavior] = dataDS.assignAscendingTimestamps(_.timestamp * 1000)

    val filterDS: DataStream[bean.UserBehavior] = asTDS.filter(_.behavior == "pv")

    val mapDS: DataStream[(String, Int)] = filterDS.map(_ => {
      ("pv", 1)
    })

    val keyByKS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)

    val pvToOneWS: WindowedStream[(String, Int), String, TimeWindow] = keyByKS.timeWindow(Time.hours(1))

    val sum: DataStream[(String, Int)] = pvToOneWS.sum(1)

    sum

  }
}
