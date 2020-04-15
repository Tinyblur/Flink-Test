package com.chenliu.flink.controller

import com.chenliu.flink.common.TController
import com.chenliu.flink.service.AdvertClickAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

class AdvertClickAnalysesController extends TController{

  private val advertClickAnalysesService = new AdvertClickAnalysesService

  override def execute(): Unit = {
    val result: DataStream[String] = advertClickAnalysesService.analyses()
    result.print
  }
}
