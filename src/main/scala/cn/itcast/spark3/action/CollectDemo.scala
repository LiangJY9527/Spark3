package cn.itcast.spark3.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CollectDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))

    //行动算子：所谓行动算子，其实就是触发作业执行的方法
    //底层代码调用的是环境对象的runjob方法
    //底层代码会创建activeJob，并提交执行
//    rdd1.collect()

    sc.stop()
  }
}
