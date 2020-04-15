package com.chenliu.flink.service

import com.chenliu.flink.bean
import com.chenliu.flink.common.{TDao, TService}
import com.chenliu.flink.dao.HotItemAnalysesDao
import com.chenliu.flink.function.{HotItemAggregateFunction, HotItemProcessFunction, HotItemWindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream,_}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


/**
  * 热门商品分析
  */
class HotItemAnalysesService extends TService {

  private val hotItemAnalysesDao = new HotItemAnalysesDao

  override def getDao(): TDao = {
    hotItemAnalysesDao
  }

  override def analyses(): DataStream[String] = {

    //获取用户行为数据
    val userBehaviorDS: DataStream[bean.UserBehavior] = getUserBehaviorDS

    //设置eventTime和水位线
    val timeDS: DataStream[bean.UserBehavior] = userBehaviorDS.assignAscendingTimestamps(_.timestamp * 1000)

    //将数据进行清洗，保留点击数
    val filterDS: DataStream[bean.UserBehavior] = timeDS.filter(_.behavior == "pv")

    //将相同的商品聚合在一起
    val keyByItemIdKS: KeyedStream[bean.UserBehavior, Long] = filterDS.keyBy(_.itemId)

    //设定时间窗口
    val dataWS: WindowedStream[bean.UserBehavior, Long, TimeWindow] = keyByItemIdKS.timeWindow(Time.hours(1), Time.minutes(5))

    //聚合数据
    //单一数据处理
    //    dataWS.aggregate()
    //全量数据处理
    //    dataWS.process()

    //聚合数据并将数据转换为可排序的形式
    //aggregate的第一个参数表示聚合
    //第二个参数表时当前窗口的处理函数
    //第一个函数的处理结果会作为参数传给第二个函数
    val hicDS: DataStream[bean.HotItemClick] = dataWS.aggregate(
      new HotItemAggregateFunction,
      new HotItemWindowFunction
    )

    //对数据根据窗口进行分组
    val hicKS: KeyedStream[bean.HotItemClick, Long] = hicDS.keyBy(_.windowEndTime)

    //对聚合之后的数据进行排序,对上一次的结果进行全量操作
    val result: DataStream[String] = hicKS.process(new HotItemProcessFunction)

    result

  }

}
