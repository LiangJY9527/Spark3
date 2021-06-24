package cn.itcast.spark3.Transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CoalesceDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CoalesceDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    //coalesce，缩减分区，方法默认不会将分区的数据打乱重新组合
    //这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
    //如果想让数据均衡可以进行shuffle处理
//    val rdd2: RDD[Int] = rdd1.coalesce(1)
    val rdd2: RDD[Int] = rdd1.coalesce(1,true)

    rdd2.saveAsTextFile("output")

    sc.stop()
  }
}
