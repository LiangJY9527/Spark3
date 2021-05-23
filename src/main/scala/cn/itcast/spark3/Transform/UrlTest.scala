package cn.itcast.spark3.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UrlTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("UrlTest").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("D:\\ztest\\urlLog")

    val value: RDD[String] = rdd1.map(_.split(" ")(6))
    rdd1.collect().foreach(println)

    sc.stop()
  }
}
