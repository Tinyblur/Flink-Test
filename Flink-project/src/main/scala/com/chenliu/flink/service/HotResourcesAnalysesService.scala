package com.chenliu.flink.service

import java.text.SimpleDateFormat

import com.chenliu.flink.bean
import com.chenliu.flink.bean.ApacheLog
import com.chenliu.flink.common.{TDao, TService}
import com.chenliu.flink.dao.HotResourcesAnalysesDao
import com.chenliu.flink.function.{HotResourcesAggregateFunction, HotResourcesKeyedProcessFunction, HotResourcesWindowFunction, MyAggregateFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class HotResourcesAnalysesService extends TService{

  private val hotResourcesAnalysesDao = new HotResourcesAnalysesDao

  override def getDao: TDao = hotResourcesAnalysesDao

  override def analyses():DataStream[String]= {
    //读取数据
    val logDS: DataStream[String] = hotResourcesAnalysesDao.readTextFile("input/apache.log")

    //转换为log对象
    val mapLogDS: DataStream[ApacheLog] = logDS.map(log => {
      val logArr: Array[String] = log.split(" ")

      val sdf = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")

      ApacheLog(
        logArr(0),
        logArr(1),
        sdf.parse(logArr(3)).getTime,
        logArr(5),
        logArr(6)
      )
    })

    val atwDS: DataStream[ApacheLog] = mapLogDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.minutes(1)) {
        override def extractTimestamp(element: ApacheLog): Long = {
          element.eventTime
        }
      }
    )

    //根据资源访问路径进行分组
    val logKS: KeyedStream[ApacheLog, String] = atwDS.keyBy(_.url)

    //增加窗口数据范围
    val logWS: WindowedStream[ApacheLog, String, TimeWindow] = logKS.timeWindow(Time.minutes(10),Time.seconds(5))

    //将数据进行聚合改变结构
    val aggDS: DataStream[bean.HotResourceClick] = logWS.aggregate(
      new MyAggregateFunction[ApacheLog],
      new HotResourcesWindowFunction
    )

    //将数据根据windowEndTime来进行分组
    val hrcKS: KeyedStream[bean.HotResourceClick, Long] = aggDS.keyBy(_.windowEndTime)

    val result: DataStream[String] = hrcKS.process(new HotResourcesKeyedProcessFunction)

    result

  }
}
