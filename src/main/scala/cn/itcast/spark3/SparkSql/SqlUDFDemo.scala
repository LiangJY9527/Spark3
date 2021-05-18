package cn.itcast.spark3.SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
//自定义方法/函数
object SqlUDFDemo {
  val prefixName = (userName: String) =>{
    "Name: "+userName
  }
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SqlUDFDemo").setMaster("local[2]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df: DataFrame = spark.read.json("src/main/datas/user.txt")
    //注册自定义函数
    spark.udf.register("prefixName",prefixName)
    df.createOrReplaceTempView("user")
    spark.sql("select age,prefixName(username) from user").show()

    spark.close()
  }
}
