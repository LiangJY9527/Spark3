package cn.itcast.spark3.Transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CogroupDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 3), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("a", 6)))

    //cogroup ：connect + group r分组并且连接
    val rdd3: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    rdd3.collect().foreach(println)

    sc.stop()
  }
}
