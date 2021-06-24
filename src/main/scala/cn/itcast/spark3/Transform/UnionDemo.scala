package cn.itcast.spark3.Transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object UnionDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CoalesceDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3,4,5,6))
    //TODO 算子 双value型操作

    //交集：[4,3]
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))
    //并集:[1,2,3,4,3,4,5,6]
    val rdd4: RDD[Int] = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))
    //差集:[2,1]
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))
    //拉链:[(1,3),(2,4),(3,5),(4,6)]
    //拉链操作：要求两个数据源分区数量保持一致，并且分区中的数据量保持一致
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))
    sc.stop()
  }
}
