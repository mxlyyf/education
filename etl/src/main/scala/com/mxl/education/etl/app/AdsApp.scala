package com.mxl.education.etl.app

import com.mxl.education.etl.dao.AdsDao
import org.apache.spark.sql.SparkSession

object AdsApp {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder()
			.master("local[2]")
			.appName("ETLApp")
			.enableHiveSupport()
			.config("hive.exec.dynamici.partition", true) //开启动态分区
			.config("hive.exec.dynamic.partition.mode", "nonstrict") //开启动态分区
			//.config("hive.exec.dynamic.partition.mode", "nonstrict")//snappy压缩
			//.config("spark.sql.warehouse.dir", "hdfs://hadoop101:9000/user/hive/warehouse/dwd")
			.getOrCreate()
		val sc = spark.sparkContext

		//需求4.统计通过各注册跳转地址(appregurl)进行注册的用户数
		AdsDao.queryRegisterAppregurlnum(spark, "20190722").show()
		//需求5.统计各所属网站（sitename）的用户数
		AdsDao.queryRegisterSitenamenum(spark, "20190722").show()
		//需求6.统计各所属平台的（regsourcename）用户数
		AdsDao.queryRegisterResourcenamenum(spark, "20190722").show()
		//需求7.统计通过各广告跳转（adname）的用户数
		AdsDao.queryRegisterAdnamenum(spark, "20190722").show()
		//需求8.统计各用户级别（memberlevel）的用户数
		AdsDao.queryRegisterMemberlevelnum(spark, "20190722").show()
		//需求9.统计各分区网站、用户级别下(website、memberlevel)的top3用户
		AdsDao.queryRegisterTop3member(spark,"20190722").show()
		spark.close()
	}
}
