package com.jj.batch

import com.jj.log.Logging
import org.apache.spark.{SparkConf, SparkContext}


object BasicApp extends App with Logging {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("BasicApp")
  val sc = new SparkContext(sparkConf)

  val dataRdd = {
    val data = Seq(
      (1, 2, 3, 5),
      (4, 3, 2, 3),
      (1, 2, 3, 6),
      (1, 2, 3, 2),
      (6, 9, 0, 9))
    sc.parallelize(data)
  } filter {
    case (c1, c2, c3, c4) => c1 == 1 || c1 == 2 || c1 == 3 || c1 == 4
  } groupBy {
    case (c1, c2, c3, c4) => c1
  }


  dataRdd.collect().foreach(println)
}

