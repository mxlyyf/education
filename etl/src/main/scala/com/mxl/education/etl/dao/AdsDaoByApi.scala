package com.mxl.education.etl.dao

import com.mxl.education.etl.bean.QueryResult
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{KeyValueGroupedDataset, SaveMode, SparkSession}

object AdsDaoByApi {
	//加载用户宽表: dws_member
	def queryMemberWideTable(spark: SparkSession, dt: String) = {
		import spark.implicits._
		spark.sql("select uid,ad_id,memberlevel,register,appregurl,regsource,regsourcename,adname," +
			s"siteid,sitename,vip_level,cast(paymoney as decimal(10,4)) as paymoney,dt,dn from dws.dws_member where dt=${dt}")
			.as[QueryResult]
			.cache()
	}

	//需求4：使用Spark DataFrame Api统计通过各注册跳转地址(appregurl)进行注册的用户数,有时间的再写Spark Sql
	def queryRegisterAppregurlnum(spark: SparkSession, dt: String) = {
		import spark.implicits._
		val ds: KeyValueGroupedDataset[String, (String, Int)] = queryMemberWideTable(spark, dt).map(result => {
			(result.appregurl + "_" + result.dn + "_" + result.dt, 1)
		}).groupByKey(_._1)

		ds.mapValues(_._2).reduceGroups(_ + _).map {
			case (key, n) =>
				val arr = key.split("_")
				(arr(0), arr(1), arr(2), n)
		}.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_appregurlnum")
	}

	//需求5：使用Spark DataFrame Api统计各所属网站（sitename）的用户数,有时间的再写Spark Sql
	def queryRegisterSitenamenum(spark: SparkSession, dt: String) = {

	}

	//需求6：使用Spark DataFrame Api统计各所属平台的（regsourcename）用户数,有时间的再写Spark Sql
	def queryRegisterResourcenamenum(spark: SparkSession, dt: String) = {

	}

	//需求7：使用Spark DataFrame Api统计通过各广告跳转（adname）的用户数,有时间的再写Spark Sql
	def queryRegisterAdnamenum(spark: SparkSession, dt: String) = {

	}

	//需求8：使用Spark DataFrame Api统计各用户级别（memberlevel）的用户数,有时间的再写Spark Sql
	def queryRegisterMemberlevelnum(spark: SparkSession, dt: String) = {

	}

	//需求9：使用Spark DataFrame Api统计各分区网站、用户级别下(website、memberlevel)的top3用户,有时间的再写Spark Sql
	def queryRegisterTop3member(spark: SparkSession, dt: String) = {
		import org.apache.spark.sql.functions._
		queryMemberWideTable(spark, dt)
			.withColumn("rownum", row_number().over(Window.partitionBy("dn", "memberlevel").orderBy(desc("paymoney"))))
			.where("rownum<4").orderBy("memberlevel", "rownum")
			.select("uid", "memberlevel", "register", "appregurl", "regsourcename", "adname", "sitename", "vip_level", "paymoney", "rownum", "dt", "dn")
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_top3memberpay")
	}

}
