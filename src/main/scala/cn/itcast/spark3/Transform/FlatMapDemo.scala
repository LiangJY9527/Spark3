package cn.itcast.spark3.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))
    val rdd2: RDD[String] = sc.makeRDD(List("hello word", "scala apark"))
    val rdd3: RDD[Any] = sc.makeRDD(List(List(1, 2), 5, List(3, 4)))

    val flatMapRdd: (List[Int] => TraversableOnce[Nothing]) => RDD[Nothing] = rdd1.flatMap(_)
    val strRdd: RDD[String] = rdd2.flatMap(_.split(" "))

    rdd3.flatMap {
      case list: List[Int] => list
      case dat => List(dat)
    }

    sc.stop()
  }
}
