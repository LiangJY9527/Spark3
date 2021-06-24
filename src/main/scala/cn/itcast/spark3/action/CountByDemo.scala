package cn.itcast.spark3.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CountByDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd1: RDD[Int] = sc.makeRDD(List(3,2,1,4,1,3),2)

    val valueMap: collection.Map[Int, Long] = rdd1.countByValue()
    println(valueMap)

    val kvRdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",1),("a",1),("c",1)))

    val kvMap: collection.Map[String, Long] = kvRdd.countByKey()
    println(kvMap)

    sc.stop()
  }
}
