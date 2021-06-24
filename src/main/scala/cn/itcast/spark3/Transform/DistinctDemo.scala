package cn.itcast.spark3.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DistinctDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DistinctDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,1,2))
    //底层实现与scala的distinct不同
    //rdd的distinct实现：map((_,null)).reduceByKey((x,_) => x,numPartitions).map(_._1)
    val rdd2: RDD[Int] = rdd1.distinct()

    println(rdd2.collect().toBuffer)

    sc.stop()
  }
}
