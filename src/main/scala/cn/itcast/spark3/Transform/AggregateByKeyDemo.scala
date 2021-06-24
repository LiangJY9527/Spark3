package cn.itcast.spark3.Transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AggregateByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("d", 3), ("d", 4), ("a", 2)), 2)

    //aggregateByKey：将数据根据不同规则进行分区内计算和分区间计算
    /**
      * aggregateByKey存在柯里化，有两个参数列表
      * 第一个参数列表，需要传递一个参数表示为初始值，主要用于碰见第一个key时，和value进行分区内计算
      * 第二个参数列表需要传递两个参数
      * 第一个参数表示分区内的计算规则
      * 第二个参数表示分区间的计算规则
      */
    val rdd2: RDD[(String, Int)] = rdd1.aggregateByKey(5)(math.max(_, _), _ + _)
    rdd2.collect().foreach(println)

    //如果集合计算时，如果分区内和分区间的计算规则相同，spark提供了简单的方法
    val rdd3: RDD[(String, Int)] = rdd1.foldByKey(0)(_ + _)
    rdd3.collect().foreach(println)

    //利用aggregateByKey，combineByKey求平均值
    val rdd4: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 3), ("d", 3), ("d", 4), ("d", 2), ("a", 2)), 2)
    val rdd5: RDD[(String, (Int, Int))] = rdd4.aggregateByKey((0, 0))(
      (num, value) => (num._1 + value, num._2 + 1),
      (num1, num2) => (num1._1 + num2._1, num1._2 + num2._2)
    )
    //    val rdd6: RDD[(String, Int)] = rdd5.mapValues(value => value._1/value._2)
    val rdd6: RDD[(String, Int)] = rdd5.mapValues {
      case (num, count) => num / count
    }
    rdd6.collect().foreach(println)

    //利用combineByKey求平均值
    /**
      * combineByKey有三个参数：
      * 第一个参数表示：将相同key的第一个数据结构进行转换，
      * 第二个参数表示：分区内的计算规则
      * 第三个参数表示：分区间的计算规则
      */
    val rdd7: RDD[(String, (Int, Int))] = rdd4.combineByKey(
      x => (x, 1),
      (tup: (Int, Int), num: Int) => (tup._1 + num, tup._2 + 1),
      (tup1: (Int, Int), tup2: (Int, Int)) => (tup1._1 + tup2._1, tup1._2 + tup2._2)
    )
    val rdd8: RDD[(String, Int)] = rdd7.mapValues(value => value._1 / value._2)
    rdd8.collect().foreach(println)
    sc.stop()
  }
}
