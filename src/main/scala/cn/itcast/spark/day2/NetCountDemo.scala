package cn.itcast.spark.day2

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable
//自定义分区partition
object NetCountDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NetCountDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("D:\\sparkTest\\net\\itcast.log").map(line =>{
      val files = line.split("\t")
//      val url = new URL(files(1))
      (files(1),1)
    }).reduceByKey(_+_)
    val rdd2 = rdd1.map(x => {
      val url = new URL(x._1)
      (url.getHost,(x._1,x._2))
    })
    val ins = rdd2.keys.distinct().collect()
    val hostPartation = new HostPartation(ins)
    val rdd3 = rdd2.partitionBy(hostPartation).mapPartitions(it =>{
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })
    println(rdd3.collect().toBuffer)
    rdd3.saveAsTextFile("D:\\sparkTest\\netOut\\")
    sc.stop()
  }
}
//自定义分区:决定数据放在哪个分区里面
class HostPartation(ins: Array[String]) extends Partitioner{
  val parMap = new mutable.HashMap[String ,Int]()
  var count = 0
  for(host <- ins){
    parMap.+= (host->count)
    count += 1
  }
  override def numPartitions: Int = ins.length

  override def getPartition(key: Any): Int = {
    parMap.getOrElse(key.toString,0)
  }
}