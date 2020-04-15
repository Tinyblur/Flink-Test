package com.chenliu.flink.application

import com.chenliu.flink.common.TApplication
import com.chenliu.flink.controller.AdvertClickAnalysesController

object AdvertClickAnalysesApplication extends App with TApplication{

  start{

    val advertClickAnalysesController = new AdvertClickAnalysesController

    advertClickAnalysesController.execute()

  }

}
