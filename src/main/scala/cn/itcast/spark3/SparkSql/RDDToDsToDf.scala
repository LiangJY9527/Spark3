package cn.itcast.spark3.SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
object SqlDemo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SqlDemo1").setMaster("local[2]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    //dataframe
    val df: DataFrame = spark.read.json("src/main/datas/user.txt")
//    df.show()

    //dataframe => SQL
//    df.createOrReplaceTempView("user")
//    spark.sql("select * from user").show()
//    spark.sql("select username,age from user").show()
//    spark.sql("select avg(age) from user").show()

    //dataframe => DSL
    //在使用Dataframe时,如果涉及到转换操作,需要引入转换规则spark.implicits._
//    df.select("username","age").show()
//    df.select($"age"+1).show()

    //dataset
    //dataframe是特定泛型的dataset
//    val seq: Seq[Int] = Seq(1,2,3,4,5)
//    val ds: Dataset[Int] = seq.toDS()
//    ds.show()

    //RDD <=>DataFrame
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",25)))
    val df1: DataFrame = rdd.toDF("id","name","age")
    val rddR: RDD[Row] = df1.rdd

    //DataFrame <=> DataSet
    val dataS: Dataset[User] = df1.as[User]
    val frame: DataFrame = dataS.toDF()

    //rdd <=> DataSet
    //rdd有数据无结构 + 结构 = dataset
    val dataSet: Dataset[User] = rdd.map {
      case (x, y, z) => User(x, y, z)
    }.toDS()
    val rddUser = dataSet.rdd



    spark.close()
  }
}
case class User(id: Int,name: String,age: Int)