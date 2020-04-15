package com.chenliu.flink.utils

/**
  * 模拟布隆过滤器
  * 使用redis作为位图 key：id  field：offset  value:0或1
  * 使用当前对象对位图进行定位处理
  */
object MockBloomFilter {

  //位图的容量
  //redis位图最大为512M 512 * 1024 * 1024 * 8
  val cap = 1 << 27

  //将数据进行散列后计算位图偏移量
  def offset(s: String, seed: Int): Long = {
    var hash = 0

    for (c <- s){
      hash = hash *seed + c
    }

    hash & (cap -1)
  }

}
