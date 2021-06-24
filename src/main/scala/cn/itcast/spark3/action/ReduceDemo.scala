package cn.itcast.spark3.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd1: RDD[Int] = sc.makeRDD(List(3,2,11,4,1,6))

    //reduce
    val i: Int = rdd1.reduce(_+_)
    println(i)

    //collect：方法会将不同分区的数据按照分区顺序采集到driver端内存中，形成数组
    val ints: Array[Int] = rdd1.collect()
    println(ints.mkString(","))

    //count：数据源中数据的个数
    val l: Long = rdd1.count()
    println(l)
    //获取数据源中数据的第一个
    val first: Int = rdd1.first()
    println(first)

    //take：获取N个数据
    val threeNun: Array[Int] = rdd1.take(3)
    println(threeNun.mkString(","))

    //takeOrdered：数据排序后，取N个数据
    val orderNum: Array[Int] = rdd1.takeOrdered(3)
    println(orderNum.mkString(","))
    sc.stop()
  }
}
