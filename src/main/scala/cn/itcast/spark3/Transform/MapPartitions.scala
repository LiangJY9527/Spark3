package cn.itcast.spark3.Transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MapPartitions {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),2)
    /**
      * mapPartitions: 可以以分区为单位进行数据转换操作，但是会将整个分区的数据加载到内存进行引用，
      * 会长时间占用年内存，内存有限时不推荐使用
      */
    val value: RDD[Int] = rdd1.mapPartitions(it =>{
      println(">>>>>>>>>")
      it.map(_*2)
    })
    //求每个分区的最大值
    val maxRdd: RDD[Int] = rdd1.mapPartitions(it => {
      //      it.toList.sortBy(x => x).take(1).iterator
      List(it.max).iterator
    })
    //保留特定分区
    val itRdd: RDD[Int] = rdd1.mapPartitionsWithIndex((index, it) => {
      if (index == 1) it
      else Nil.iterator
    })
    //打印数据所在的分区
    val partRdd: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex((index, it) => {
      it.map((_, index))
    })
    value
    value.collect().foreach(println)
    sc.stop()
  }
}
