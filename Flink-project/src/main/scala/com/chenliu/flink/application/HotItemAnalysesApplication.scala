package com.chenliu.flink.application

import com.chenliu.flink.common.TApplication
import com.chenliu.flink.controller.HotItemAnalysesController

/**
  * 实时热门商品统计
  */
object HotItemAnalysesApplication extends App with TApplication{

  //启动应用
  start{
    //执行控制器
    val hotItemAnalysesController = new HotItemAnalysesController
    hotItemAnalysesController.execute()
  }
}
