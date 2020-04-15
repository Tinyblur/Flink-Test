package com.chenliu.flink.application

import com.chenliu.flink.common.TApplication
import com.chenliu.flink.controller.HotResourcesAnalysesController

/**
  * 热门资源浏览量排行
  */
object HotResourcesAnalysesApplication extends App with TApplication {

  start {

    val hotResourcesAnalysesController = new HotResourcesAnalysesController

    hotResourcesAnalysesController.execute()

  }

}
