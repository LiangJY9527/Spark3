package cn.itcast.spark3.Transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SortByDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CoalesceDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("1",2),("11",3),("2",5)),2)
    //sortBy方法可以根据指定规则对源数据中的数据进行排序，默认为升序，第二个参数可以指定
    //sortBy默认情况下，不会改变分区。但是中间存在shuffle操作
    val rdd2: RDD[(String, Int)] = rdd1.sortBy(_._1.toInt,false)

    rdd2.collect().foreach(println)


    sc.stop()
  }
}
