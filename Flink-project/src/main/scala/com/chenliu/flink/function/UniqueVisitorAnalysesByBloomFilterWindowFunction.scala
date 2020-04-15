package com.chenliu.flink.function

import java.lang

import com.chenliu.flink.utils.MockBloomFilter
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

class UniqueVisitorAnalysesByBloomFilterWindowFunction
  extends ProcessAllWindowFunction[(Long, Int), String, TimeWindow] {

  private var jedis: Jedis = _


  override def open(parameters: Configuration): Unit = {
    jedis = new Jedis("hadoop102", 6379)
  }

  override def process(context: Context, elements: Iterable[(Long, Int)], out: Collector[String]): Unit = {

    //获取位图的key,每个窗口的位图是不一样的
    val bitMapKey = context.window.getEnd

    //获取用户ID，因为只有一条数据，所以可以直接迭代取元组的第一个
    val id: Long = elements.iterator.next()._1

    //获取用户ID在位图中的位置（偏移量）
    val offset: Long = MockBloomFilter.offset(id.toString, 25)

    //根据偏移量判断用户id在redis位图中是否存在
    //设置值
    //    jedis.setbit()
    //取值
    val flag: lang.Boolean = jedis.getbit(bitMapKey.toString, offset)

    if (flag) {
      //如果位图中已经存在，那么什么都不做

    } else {
      //如果位图中不存在，那么增加uv数量
      //更新状态
      jedis.setbit(bitMapKey.toString, offset,true)
      //增加uv数量
      val count: String = jedis.hget("uvcount",bitMapKey.toString)

      var uvCount:Long = 0

      if (count != null && "" != count){
        uvCount = count.toLong
      }
      jedis.hset("uvcount",bitMapKey.toString,(uvCount+1).toString)
    }

    out.collect("userId:" + id)
  }
}
