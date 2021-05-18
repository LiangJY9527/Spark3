package cn.itcast.spark.day2

import org.apache.spark.{SparkConf, SparkContext}

object LACDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LACDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("D:\\sparkTest\\jz\\").map(line =>{
      val txt = line.split(",")
      val phone = txt(0)
      val flag = txt(3)
      val time = if(flag == "1") -txt(1).toLong else txt (1).toLong
      val lacFlag = txt(2)
      ((phone,lacFlag),time)
    })
    val rdd2 = rdd1.reduceByKey(_+_).map(x=>(x._1._1,x._1._2,x._2)).groupBy(_._1)
    val rdd3 = rdd2.mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(2)
    })
    val rdd4 = rdd3.flatMap(x =>{
      x._2.map(y => {
        (y._2,(y._1,y._3))
      })
    })

    val rdd5 = sc.textFile("D:\\sparkTest\\flag\\lac_info.txt").map(line =>{
      val files = line.split(",")
      val lac = files(0)
      val x = files(1)
      val y = files(2)

      (lac,(x,y))
    })
    val rdd6 = rdd4.join(rdd5).groupBy(_._2._1._1)
/*    map(temp =>{
      (temp._2._1._1,temp._1,temp._2._1._2,temp._2._2._1,temp._2._2._1)
    }).groupBy(_._1)*/
    println(rdd6.collect().toBuffer)
    sc.stop()
  }
}
