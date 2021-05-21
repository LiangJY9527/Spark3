package cn.itcast.spark3.SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//sparksession 读取mysql
object SqlJDBC {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SqlJDBC").setMaster("local[2]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123")
      .option("dbtable", "user").load()
//    df.show()

    df.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123")
      .option("dbtable", "user_spark")
        .mode(SaveMode.Append).save()

    spark.close()
  }
}
