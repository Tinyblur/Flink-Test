package com.chenliu.flink.application

import com.chenliu.flink.common.TApplication
import com.chenliu.flink.controller.AppMarketAnalysesController

object AppMarketAnalysesApplication extends App with TApplication{

  start{

    val appMarketAnalysesController = new AppMarketAnalysesController

    appMarketAnalysesController.execute()

  }

}
