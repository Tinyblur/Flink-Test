package com.chenliu.flink.controller

import com.chenliu.flink.common.TController
import com.chenliu.flink.service.LoginFailAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

class LoginFailAnalysesController extends TController{

  private val loginFailAnalysesService = new LoginFailAnalysesService

  override def execute(): Unit = {
      val result: DataStream[String] = loginFailAnalysesService.analyses()

    result.print()
  }
}
