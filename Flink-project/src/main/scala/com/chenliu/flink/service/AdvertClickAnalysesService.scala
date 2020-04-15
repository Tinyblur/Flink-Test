package com.chenliu.flink.service

import java.lang

import com.chenliu.flink.bean.{AdClickLog, CountByProvince}
import com.chenliu.flink.common.{TDao, TService}
import com.chenliu.flink.dao.AdvertClickAnalysesDao
import com.chenliu.flink.function.AdvClickKeyedProcessFunction
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, _}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AdvertClickAnalysesService extends TService {

  private val advertClickAnalysesDao = new AdvertClickAnalysesDao

  override def getDao: TDao = advertClickAnalysesDao

  def analysesAdv(): DataStream[String] = {

    val dataDS: DataStream[String] = advertClickAnalysesDao.readTextFile("input/AdClickLog.csv")

    //将数据进行封装
    val logData: DataStream[AdClickLog] = dataDS.map(
      t => {
        val str: Array[String] = t.split(",")
        AdClickLog(
          str(0).toLong,
          str(1).toLong,
          str(2),
          str(3),
          str(4).toLong
        )
      }
    )

    //抽取时间戳和水位线标记   1511658000 -->10位，为秒
    val timeDS: DataStream[AdClickLog] = logData.assignAscendingTimestamps(_.timestamp * 1000)

    //转换结构
    val proAndAdToOne: DataStream[(String, Long)] = timeDS.map(
      t => {
        (t.province + "_" + t.adId, 1L)
      }
    )

    //分组，开窗
    val countByProDS: DataStream[CountByProvince] = proAndAdToOne.keyBy(_._1).timeWindow(Time.hours(1), Time.seconds(5)).aggregate(
      //IN, ACC, OUT
      new AggregateFunction[(String, Long), Long, Long] {
        override def add(value: (String, Long), accumulator: Long): Long = {
          accumulator + 1L
        }

        override def createAccumulator(): Long = 0L

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a + b
      },
      //IN, OUT, KEY, W
      new WindowFunction[Long, CountByProvince, String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
          val str: Array[String] = key.split("_")
          out.collect(CountByProvince(
            window.getEnd.toString,
            str(0),
            str(1).toLong,
            input.iterator.next()
          ))
        }
      }
    )

    //按照windowEnd进行分组
    val dataKS: KeyedStream[CountByProvince, String] = countByProDS.keyBy(_.windowEnd)

    val result: DataStream[String] = dataKS.process(
      //<K, I, O>
      new KeyedProcessFunction[String, CountByProvince, String] {
        override def processElement(value: CountByProvince, ctx: KeyedProcessFunction[String, CountByProvince, String]#Context, out: Collector[String]): Unit = {
          //          if (value.count >100){
          //            val outputTag = new OutputTag[CountByProvince]("blackList")
          //            ctx.output(outputTag,value)
          //          }else{
          //          }
          out.collect("省份：" + value.province + ",广告：" + value.adId + ",点击：" + value.count)
        }
      }
    )

    //    val outputTag = new OutputTag[CountByProvince]("blackList")
    //    val blackListDS: DataStream[CountByProvince] = result.getSideOutput(outputTag)
    //    blackListDS.print("blackList>>>")
    result

  }
  def analysesAdv4BlackList(): DataStream[String] = {

    val dataDS: DataStream[String] = advertClickAnalysesDao.readTextFile("input/AdClickLog.csv")

    //将数据进行封装
    val logData: DataStream[AdClickLog] = dataDS.map(
      t => {
        val str: Array[String] = t.split(",")
        AdClickLog(
          str(0).toLong,
          str(1).toLong,
          str(2),
          str(3),
          str(4).toLong
        )
      }
    )

    //抽取时间戳和水位线标记   1511658000 -->10位，为秒
    val timeDS: DataStream[AdClickLog] = logData.assignAscendingTimestamps(_.timestamp * 1000)

    //转换结构
    val proAndAdToOne: DataStream[(String, Long)] = timeDS.map(
      t => {
        (t.province + "_" + t.adId, 1L)
      }
    )

    //分组，开窗
    val logKS: KeyedStream[(String, Long), String] = proAndAdToOne.keyBy(_._1)

    val blackListDS: DataStream[(String, Long)] = logKS.process(new AdvClickKeyedProcessFunction)

    //黑名单的测输出流
    val outputTag = new OutputTag[(String, Long)]("blackList")
    blackListDS.getSideOutput(outputTag)

    //正常数据流
    val countByProDS: DataStream[CountByProvince] = blackListDS.keyBy(_._1).timeWindow(Time.hours(1), Time.seconds(5)).aggregate(
      //IN, ACC, OUT
      new AggregateFunction[(String, Long), Long, Long] {
        override def add(value: (String, Long), accumulator: Long): Long = {
          accumulator + 1L
        }

        override def createAccumulator(): Long = 0L

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a + b
      },
      //IN, OUT, KEY, W
      new WindowFunction[Long, CountByProvince, String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
          val str: Array[String] = key.split("_")
          out.collect(CountByProvince(
            window.getEnd.toString,
            str(0),
            str(1).toLong,
            input.iterator.next()
          ))
        }
      }
    )

    //按照windowEnd进行分组
    val dataKS: KeyedStream[CountByProvince, String] = countByProDS.keyBy(_.windowEnd)

    val result: DataStream[String] = dataKS.process(
      //<K, I, O>
      new KeyedProcessFunction[String, CountByProvince, String] {
        override def processElement(value: CountByProvince, ctx: KeyedProcessFunction[String, CountByProvince, String]#Context, out: Collector[String]): Unit = {
          out.collect("省份：" + value.province + ",广告：" + value.adId + ",点击：" + value.count)
        }
      }
    )
    result
  }


  override def analyses(): DataStream[String] = {

    //    analysesAdv()

    /**
      * 广告黑名单处理
      */
    analysesAdv4BlackList()
  }
}
