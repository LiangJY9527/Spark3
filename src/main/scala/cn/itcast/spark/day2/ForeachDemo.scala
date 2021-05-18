package cn.itcast.spark.day2

import org.apache.spark.{SparkConf, SparkContext}

object ForeachDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ForeachDemo").setMaster("")
    val sc = new SparkContext(conf)
    sc.stop()
  }
}
