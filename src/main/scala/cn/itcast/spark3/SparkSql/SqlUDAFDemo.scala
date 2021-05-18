package cn.itcast.spark3.SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SqlUDAFDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SqlUDFDemo").setMaster("local[2]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df: DataFrame = spark.read.json("src/main/datas/user.txt")

    df.createOrReplaceTempView("user")
    spark.udf.register("myAvg",new MyAvgUDAF)

    spark.sql("select myAvg(age) from user").show()

    spark.close()
  }
}
//弱类型：自定义聚合函数：计算年龄的平均值
class MyAvgUDAF extends UserDefinedAggregateFunction{
  //输入数据的结构
  override def inputSchema: StructType = {
    StructType{
      Array(
        StructField("age",LongType)
      )
    }
  }
  //缓冲区数据的结构：buffer
  override def bufferSchema: StructType = {
    StructType{
      Array(
        StructField("total",LongType),
        StructField("count",LongType)
      )
    }
  }
  //函数计算结果的数据类型：Out
  override def dataType: DataType = DoubleType
  //函数的稳定性：true
  override def deterministic: Boolean = true
  //缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L

//    buffer.update(0,0)
//    buffer.update(1,0L)
  }
  //根据输入的值来更新缓冲区
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0,buffer.getLong(0)+ input.getLong(0))
    buffer.update(1,buffer.getLong(1)+1)
  }
  //缓冲区数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0,buffer1.getLong(0)+buffer2.getLong(0))
    buffer1.update(1,buffer1.getLong(1)+buffer2.getLong(1))
  }
  //计算平均值
  override def evaluate(buffer: Row): Any = {
    println((buffer.getLong(0)*1.0)/(buffer.getLong(1)*1.0))
    (buffer.getLong(0)*1.0)/(buffer.getLong(1)*1.0)
  }
}