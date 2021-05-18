package cn.itcast.spark3.SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object SqlUDAFDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SqlUDFDemo").setMaster("local[2]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df: DataFrame = spark.read.json("src/main/datas/user.txt")
    import spark.implicits._
//    df.createOrReplaceTempView("user")

    //早期强类型版本，不支持在sql中使用强类型UDAF函数
//    spark.udf.register("myAvg",functions.udaf(new MyAvgUDAF))
//    spark.sql("select myAvg(age) from user").show()

    val ds: Dataset[User] = df.as[User]
    val column: TypedColumn[User, Long] = new MyAvgUDAF().toColumn
    ds.select(column).show()

    spark.close()
  }

  /**强类型
    * 自定义聚合函数类：计算年龄的平均值
    * IN:输入的数据类型 User
    * BUF:缓冲区的数据类型 Buff
    * OUT:输出的数据类型 Long
    */
  case class User(username: String,age: Long)
  case class Buff(var total: Long,var count: Long)
  class  MyAvgUDAF extends Aggregator[User,Buff,Long]{
    //缓冲区初始化
    override def zero: Buff = {
      Buff(0L,0L)
    }
    //根据输入数据更新缓冲区
    override def reduce(b: Buff, a: User): Buff = {
      b.total += a.age
      b.count += 1
      b
    }
    //合并缓冲区
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total += b2.total
      b1.count += b2.count
      b1
    }
    //计算结果
    override def finish(reduction: Buff): Long = {
      reduction.total/reduction.count
    }
    //缓冲区的编码操作 固定写法
    override def bufferEncoder: Encoder[Buff] = Encoders.product
    //缓冲区的解码操作 固定写法
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
