package com.chenliu.flink.controller

import com.chenliu.flink.common.TController
import com.chenliu.flink.service.HotResourcesAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

class HotResourcesAnalysesController extends TController{

  private val hotResourcesAnalysesService = new HotResourcesAnalysesService

  override def execute(): Unit = {

    val result:DataStream[String] = hotResourcesAnalysesService.analyses()

    result.print()
  }
}
