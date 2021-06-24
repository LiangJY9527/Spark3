package cn.itcast.spark3.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ReduceByKeyDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("d",3),("d",4)))

    //reduceByKey：相同key的数据进行value数据的聚合
    //scala语言中一般的聚合操作都是两两聚合，spark基于scala开发，所以他也是两两聚合
    val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_+_)

    rdd2.collect().foreach(println)

    sc.stop()
  }
}
