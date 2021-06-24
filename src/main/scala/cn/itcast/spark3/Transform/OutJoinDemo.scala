package cn.itcast.spark3.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OutJoinDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 3), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("d", 6)))

//    val rdd3: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
    val rdd3: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)
    rdd3.collect().foreach(println)
    sc.stop()
  }
}
