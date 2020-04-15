package com.chenliu.flink.service

import com.chenliu.flink.bean
import com.chenliu.flink.common.{TDao, TService}
import com.chenliu.flink.dao.AppMarketAnalysesDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AppMarketAnalysesService extends TService{

  private val appMarketAnalysesDao = new AppMarketAnalysesDao

  override def getDao: TDao = appMarketAnalysesDao

  override def analyses(): DataStream[String] = {

    val dataStream: DataStream[bean.MarketingUserBehavior] = appMarketAnalysesDao.mockData()

    val waterDS: DataStream[bean.MarketingUserBehavior] = dataStream.assignAscendingTimestamps(_.timestamp)

    //对数据进行统计，转换结构
    val mapDS: DataStream[(String, Int)] = waterDS.map(
      t => {
        (t.channel + "_" + t.behavior, 1)
      }
    )

    val result: DataStream[String] = mapDS.keyBy(_._1).timeWindow(Time.minutes(1), Time.seconds(5)).process(
      new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
          out.collect(context.window.getStart + "-" + context.window.getEnd + ",安装量为：" + elements.size)
        }
      }
    )
    result

  }
}
