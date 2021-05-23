package cn.itcast.spark3.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupByDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("UrlTest").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),2)
    //根据奇数和偶数进行分组
    val rdd2: RDD[(Int, Iterable[Int])] = rdd1.groupBy(_%2)
    rdd2.collect().foreach(println)

    val strRdd: RDD[String] = sc.makeRDD(List("hello","spark","scala","hadoop"),2)
    val strRdd2: RDD[(Char, Iterable[String])] = strRdd.groupBy(_.charAt(0))
    strRdd2.collect().foreach(println)

    sc.stop()
  }
}
