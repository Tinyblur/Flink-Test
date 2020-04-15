package com.chenliu.flink.application

import com.chenliu.flink.common.TApplication
import com.chenliu.flink.controller.UniqueVisitorAnalysesController

object UniqueVisitorAnalysesApplication extends App with TApplication{

    start {
        val controller = new UniqueVisitorAnalysesController
        controller.execute()
    }
}
