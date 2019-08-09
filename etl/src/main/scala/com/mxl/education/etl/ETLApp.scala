package com.mxl.education.etl

import org.apache.spark.rdd.RDD
import com.alibaba.fastjson._
import com.mxl.education.etl.bean.DwdMember
import com.mxl.education.etl.dao.HiveDao
import com.mxl.education.etl.util.DesensitizedUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object ETLApp {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder()
			.master("local[2]")
			.appName("ETLApp")
			.enableHiveSupport()
			.config("hive.exec.dynamici.partition", true) //开启动态分区
			.config("hive.exec.dynamic.partition.mode", "nonstrict") //开启动态分区
			//.config("hive.exec.dynamic.partition.mode", "nonstrict")//snappy压缩
			.config("spark.sql.warehouse.dir", "hdfs://hadoop101:9000/user/hive/warehouse/dwd")
			.getOrCreate()
		val sc = spark.sparkContext

		//HiveDao.DwdBaseAd(spark, sc)
		//		HiveDao.DwdBaseWebsite(spark, sc)
		//		HiveDao.DwdMember(spark,sc)
		//		HiveDao.DwdMemberRegtype(spark, sc)
		//HiveDao.DwdPcentermempaymoney(spark, sc)
		//HiveDao.DwdVipLevel(spark, sc)

		spark.close()
	}

}
