package cn.itcast.spark3.SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//spark sql 连接外置hive
/**
  * 1,拷贝hive-site.xml文件到classpath
  * 2.启用hive的支持
  * 3，增加对应的依赖（包括mysql）
  *
  *
  * spark 客户端访问hive的工具，spark-sql,spark thrifserver
  * spark thrifserver: 1，启动：sbin/start-thrifserver.sh     2，bin/beeline -u jdbc:hive2://linux:10000 -n root
  */
object Sql_hive {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.sql("show tables").show()

    spark.close()
  }
}
