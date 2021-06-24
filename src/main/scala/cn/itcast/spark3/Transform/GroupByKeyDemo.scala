package cn.itcast.spark3.Transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GroupByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("d",3),("d",4)))

    //groupByKey：将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
    //元组中的第一个元素就是key
    //元组中的第二个元素就是相同key的value集合

    /**
      * reduceByKey与groupByKey的区别：
      * 从shuffle的角度：reduceByKey与groupByKey都存在shuffle的操作，但是reduceByKey可以在shuffle前对分区内相同的key的数据进行预聚合，这样可以减少落盘的数据量，
      * 而groupBykey只是进行分组，不存在减少数据量的问题，所以reduceByKey的性能比较高
      * 从功能角度：reduceByKey其实包含分组和聚合的功能，而groupByKey只包含分组，不能聚合
      */

    val rdd2: RDD[(String, Iterable[Int])] = rdd1.groupByKey()

    val rdd3: RDD[(String, Iterable[(String, Int)])] = rdd1.groupBy(_._1)

    rdd2.collect().foreach(println)

    sc.stop()
  }
}
