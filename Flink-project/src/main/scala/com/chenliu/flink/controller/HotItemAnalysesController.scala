package com.chenliu.flink.controller

import com.chenliu.flink.common.TController
import com.chenliu.flink.service.HotItemAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

/**
  * 热门商品分析控制器
  */
class HotItemAnalysesController extends TController{

  private val hotItemAnalysesService = new HotItemAnalysesService

  override def execute(): Unit = {

     val resultDS:DataStream[String]= hotItemAnalysesService.analyses()

    resultDS.print()

  }

}
