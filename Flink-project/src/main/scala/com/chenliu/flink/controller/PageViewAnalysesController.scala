package com.chenliu.flink.controller

import com.chenliu.flink.common.TController
import com.chenliu.flink.service.PageViewAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

class PageViewAnalysesController extends TController{

  private val pageViewAnalysesService = new PageViewAnalysesService

  override def execute(): Unit = {
    val result: DataStream[(String, Int)] = pageViewAnalysesService.analyses()
    result.print
  }
}
