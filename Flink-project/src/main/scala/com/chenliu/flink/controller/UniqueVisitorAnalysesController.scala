package com.chenliu.flink.controller

import com.chenliu.flink.common.TController
import com.chenliu.flink.service.UniqueVisitorAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

class UniqueVisitorAnalysesController extends TController{

  private val uniqueVisitorAnalysesService = new UniqueVisitorAnalysesService

  override def execute(): Unit = {

    val result: DataStream[String] = uniqueVisitorAnalysesService.analyses()
    result.print

  }
}
