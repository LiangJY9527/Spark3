package cn.itcast.spark3.Transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RepartitionDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CoalesceDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    //coalesce算子可以扩大分区，但是如果不进行shuffle操作，是没有意义的，不起作用
    //所以，如果想要实现扩大分区的效果，需要使用shuffle操作
    //扩大分区：repartition
    val rdd2: RDD[Int] = rdd1.repartition(4)

    rdd2.saveAsTextFile("output")

    sc.stop()
  }
}
