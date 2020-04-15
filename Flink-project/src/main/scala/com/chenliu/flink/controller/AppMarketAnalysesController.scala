package com.chenliu.flink.controller

import com.chenliu.flink.bean
import com.chenliu.flink.common.TController
import com.chenliu.flink.service.AppMarketAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

class AppMarketAnalysesController extends TController{

  private val appMarketAnalysesService = new AppMarketAnalysesService

  override def execute(): Unit = {

    val result: DataStream[String] = appMarketAnalysesService.analyses()

    result.print
  }
}
