package cn.itcast.spark3.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GlomDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    /**
      * glom:将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
      */
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),2)

    val glomRdd: RDD[Array[Int]] = rdd1.glom()
    glomRdd.collect().foreach(arr =>{
      println(arr.mkString(","))
    })
    //将每个分区的最大值累加
    val sum: Int = glomRdd.map(_.max).reduce(_+_)
    println(sum)
    sc.stop()
  }
}
