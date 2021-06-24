package cn.itcast.spark3.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountFuncs {

  //groupBy
  def wordcount1(sc: SparkContext) = {
    val rdd1: RDD[String] = sc.makeRDD(List("hello word", "hello spark", "hello scala"))
    val rdd2: RDD[(String, Iterable[String])] = rdd1.flatMap(_.split(" ")).groupBy(word => word)
    val rdd3: RDD[(String, Int)] = rdd2.mapValues(it => it.size)
  }

  //groupByKey
  def wordcount2(sc: SparkContext) = {
    val rdd1: RDD[String] = sc.makeRDD(List("hello word", "hello spark", "hello scala"))
    val rdd2: RDD[(String, Iterable[Int])] = rdd1.flatMap(_.split(" ")).map((_, 1)) groupByKey()
    val rdd3: RDD[(String, Int)] = rdd2.mapValues(it => it.size)
  }

  //reduceByKey
  def wordcount3(sc: SparkContext) = {
    val rdd1: RDD[String] = sc.makeRDD(List("hello word", "hello spark", "hello scala"))
    val rdd2: RDD[(String, Int)] = rdd1.flatMap(_.split(" ")).map((_, 1)) reduceByKey (_ + _)
  }

  //aggregateByKey
  def wordcount4(sc: SparkContext) = {
    val rdd1: RDD[String] = sc.makeRDD(List("hello word", "hello spark", "hello scala"))
    val rdd2: RDD[(String, Int)] = rdd1.flatMap(_.split(" ")).map((_, 1)).aggregateByKey(0)(_ + _, _ + _)
  }
  //foldByKey
  def wordcount5(sc: SparkContext) = {
    val rdd1: RDD[String] = sc.makeRDD(List("hello word", "hello spark", "hello scala"))
    val rdd2: RDD[(String, Int)] = rdd1.flatMap(_.split(" ")).map((_, 1)).foldByKey(0)(_ + _)
  }
  //combineByKey
  def wordcount6(sc: SparkContext) = {
    val rdd1: RDD[String] = sc.makeRDD(List("hello word", "hello spark", "hello scala"))
    val rdd2: RDD[(String, Int)] = rdd1.flatMap(_.split(" ")).map((_, 1)).combineByKey(x=>x,
      (x: Int,y)=>x+y,
      (m,n) => m+n
    )
  }
  //countByKey
  def wordcount7(sc: SparkContext) = {
    val rdd1: RDD[String] = sc.makeRDD(List("hello word", "hello spark", "hello scala"))
    val rdd2: collection.Map[String, Long] = rdd1.flatMap(_.split(" ")).map((_, 1)).countByKey()
  }

  //countByValue
  def wordcount8(sc: SparkContext) = {
    val rdd1: RDD[String] = sc.makeRDD(List("hello word", "hello spark", "hello scala"))
    val rdd2: collection.Map[String, Long] = rdd1.flatMap(_.split(" ")).countByValue()
  }
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    wordcount1(sc)

    sc.stop()
  }
}
