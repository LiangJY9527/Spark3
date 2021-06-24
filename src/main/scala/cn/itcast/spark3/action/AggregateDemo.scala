package cn.itcast.spark3.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AggregateDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd1: RDD[Int] = sc.makeRDD(List(3,2,11,4,1,6),2)

    /**
      * aggregateByKey：初始值只会参与分区内的计算
      * aggregate：初始值参与分区内的计算，并且还参与分区间的计算
      */
    val sum: Int = rdd1.aggregate(10)(_+_,_+_)
    println(sum)
    val foldSum: Int = rdd1.fold(10)(_+_)
    println(foldSum)

    sc.stop()
  }
}
