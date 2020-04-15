package com.chenliu.flink.dao

import com.chenliu.flink.bean.MarketingUserBehavior
import com.chenliu.flink.common.TDao
import com.chenliu.flink.utils.FlinkStreamEnv
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

class AppMarketAnalysesDao extends TDao{
  /**
    * 生成模拟数据
    */
  def mockData(): DataStream[MarketingUserBehavior] ={
    //创建自定义数据源
    val env: StreamExecutionEnvironment = FlinkStreamEnv.get()

    val mockData: DataStream[MarketingUserBehavior] = env.addSource(
      new SourceFunction[MarketingUserBehavior] {
        var flag = true

        override def cancel(): Unit = {
          flag = false
        }

        override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
          while (flag) {
            ctx.collect(MarketingUserBehavior(
              1,
              "INSTALL",
              "XIAOMI",
              System.currentTimeMillis()
            ))
            Thread.sleep(500)
          }
        }
      }
    )
    mockData
  }



}
