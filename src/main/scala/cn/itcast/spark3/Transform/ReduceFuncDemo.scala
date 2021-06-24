package cn.itcast.spark3.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceFuncDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 3), ("d", 3), ("d", 4), ("d", 2), ("a", 2)), 2)

    /**
      * reduceByKey:
      *     combineByKeyWithClassTag[V](
      *     (v: V) => v, //第一个值不会参与计算
      *     func, //分区内的计算规则
      *     func, //分区间的计算规则
      *     partitioner)
      *
      * aggregateByKey：
      *   combineByKeyWithClassTag[U](
      *   (v: V) => cleanedSeqOp(createZero(), v), //初始值和第一个key的value值进行的数据操作
      *   cleanedSeqOp, //分区内的计算规则
      *   combOp, //分区间的计算规则
      *   partitioner)
      *
      * foldByKey：
      *   combineByKeyWithClassTag[V](
      *   (v: V) => cleanedFunc(createZero(), v), //初始值和第一个key的value值进行的数据操作
      *   cleanedFunc, //分区内的计算规则
      *   cleanedFunc, //分区间的计算规则
      *   partitioner)
      *
      * combineByKey：
      *     combineByKeyWithClassTag(
      *     createCombiner, //相同key的第一条数据进行的处理
      *     mergeValue,     //表示分区内的数据的处理函数
      *     mergeCombiners) //表示分区间的数据的处理函数
      */

    rdd1.reduceByKey(_ + _)
    rdd1.aggregateByKey(0)(_ + _, _ + _)
    rdd1.foldByKey(0)(_ + _)
    rdd1.combineByKey(x => x, (x: Int, y) => x + y, (m: Int, n: Int) => m + n)

    sc.stop()
  }
}
