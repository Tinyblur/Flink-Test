package com.chenliu.flink.application

import com.chenliu.flink.common.TApplication
import com.chenliu.flink.controller.PageViewAnalysesController

/**
  * 页面访问量统计分析
  */
object PageViewAnalysesApplication extends App with TApplication{

    start {
        val controller = new PageViewAnalysesController
        controller.execute()
    }
}
