package com.chenliu.flink

import java.util

import com.chenliu.bean.Sensor
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object Flink_Sink_Elasticsearch {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDS: DataStream[String] = env.readTextFile("input/sensor.txt")

    val sensorDS: DataStream[Sensor] = inputDS.map(
      t => {
        val str: Array[String] = t.split(",")
        Sensor(str(0), str(1).toLong, str(2).toDouble)
      }
    )

    val httpHosts = new java.util.ArrayList[HttpHost]()

    httpHosts.add(new HttpHost("hadoop102",9200))

    val elasticsearchSinkBuilder = new ElasticsearchSink.Builder[Sensor](httpHosts, new ElasticsearchSinkFunction[Sensor] {
      override def process(t: Sensor, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        println("成功保存:" + t)
        val json = new util.HashMap[String, String]()
        json.put("data", t.toString)
        val indexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(json)
        requestIndexer.add(indexRequest)
        println("saved successfully")
      }
    })

    sensorDS.addSink(elasticsearchSinkBuilder.build())

    env.execute()

  }

}
