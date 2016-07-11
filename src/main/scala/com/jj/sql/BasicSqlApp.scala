package com.jj.sql

import com.jj.log.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object BasicSqlApp extends App with Logging {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName(BasicSqlApp.getClass.getSimpleName)
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._


  val json = sqlContext.read.json("src/main/resources/data/account.json")

  json.printSchema()

  val jsonDs = json.as[Accounts]

  jsonDs.map { ds =>
    FlatAccount(ds.account.city, ds.account.state, ds.account.zip, ds.assetClass, ds.contractType, ds.strikePrice, ds.tradeId, ds.transType)
  }.collect().foreach(println)
}


case class Account(city: String, state: String, zip: String)
case class Accounts(account: Account, assetClass: String, contractType: String, strikePrice: String, tradeId: String, transType: String)
case class FlatAccount(city: String, state: String, zip: String, assetClass: String, contractType: String, strikePrice: String, tradeId: String, transType: String)
