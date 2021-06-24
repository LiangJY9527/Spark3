package cn.itcast.spark3.Transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object JoinDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 3), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("a", 6)))

    //join：两个不同的数据源，相同的key的value会连接在一起，形成元组
    //如果两个数据源中的key没有匹配上，那么数据不会出现在这个结果中
    //如果两个数据源中key有多个相同，会依次匹配，可能会出现笛卡尔集，数据量会集合性增长，会导致性能降低
    val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    rdd3.collect().foreach(println)
    sc.stop()
  }
}
