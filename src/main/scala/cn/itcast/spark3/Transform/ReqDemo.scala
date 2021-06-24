package cn.itcast.spark3.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReqDemo {
  def main(args: Array[String]): Unit = {
    //案例实操
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    //获取数据源：时间戳，省份，城市，用户，广告
    val dataRdd: RDD[String] = sc.textFile("D:\\ztest\\gg\\")
    val rdd1: RDD[((String, String), Int)] = dataRdd.map(line => {
      val files: Array[String] = line.split(" ")
      ((files(1), files(4)), 1)
    })
    //聚合
    val rdd2: RDD[((String, String), Int)] = rdd1.reduceByKey(_+_)

    //结构转化
    val rdd3: RDD[(String, (String, Int))] = rdd2.map {
      case ((prov, gg), num) => (prov, (gg, num))
    }
    //根据省份进行分组
    val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.groupByKey()
    //组内排序，降序获取前三名
    val rdd5: RDD[(String, List[(String, Int)])] = rdd4.mapValues {
      case it => it.toList.sortBy(_._2).reverse.take(3)
    }

    rdd5.collect().foreach(println)

    sc.stop()
  }
}
