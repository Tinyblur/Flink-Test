package com.chenliu.flink.application

import com.chenliu.flink.common.TApplication
import com.chenliu.flink.controller.LoginFailAnalysesController

object LoginFailAnalysesApplication extends App with TApplication{

  start{

    val loginFailAnalysesController = new LoginFailAnalysesController

    loginFailAnalysesController.execute()

  }

}
