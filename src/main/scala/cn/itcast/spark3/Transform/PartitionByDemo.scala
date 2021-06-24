package cn.itcast.spark3.Transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PartitionByDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("PartitionByDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val rdd2: RDD[(Int, Int)] = rdd1.map((_,1))
    //partitionBy：根据指定的规则对数据进行分区
    rdd2.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")
    rdd2.collect().foreach(println)

    sc.stop()
  }
}
