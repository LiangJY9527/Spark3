package cn.itcast.spark3.Transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SampleDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10),2)

    /**
      * 第一个参数：抽取数据后是否将数据放回，true（放回），false（不放回）
      * 第二个参数：数据源中，每条数据被抽取的概率
      * 第三个参数：抽取数据时，随机算法的种子，不传，默认为当前系统时间
      */
    println(rdd1.sample(false, 0.4, 1).collect().mkString(","))

    sc.stop()
  }
}
