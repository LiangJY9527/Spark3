package cn.itcast.spark.day3

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object MyPrefer{
  implicit def girlToOrdered = new Ordering[Girl]() {
    override def compare(x: Girl, y: Girl): Int = {
      if(x.faceValue == y.faceValue ) y.age - x.age
      else x.faceValue - y.faceValue
    }
  }
}

object CustSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustSort").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("liuyf",90,28,1),("angelbaby",90,30,2),("hanikezi",95,24,3)))
    import MyPrefer._
    val rdd2 = rdd1.sortBy(li =>Girl(li._2,li._3),false)
    println(rdd2.collect().toBuffer)
    sc.stop()
  }
}
//方法一
/*
case class Girl(faceValue: Int,age: Int) extends Ordered[Girl] with Serializable {
  override def compare(that: Girl): Int = {
    if(this.faceValue == that.faceValue ) that.age - this.age
    else this.faceValue - that.faceValue
  }
}*/
//方法二：
case class Girl(faceValue: Int,age: Int) extends Serializable
