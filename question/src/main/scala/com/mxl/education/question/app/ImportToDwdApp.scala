package com.mxl.education.question.app

import com.mxl.education.question.dao.ImportToDwdDao
import com.mxl.education.question.util.HiveUtil
import org.apache.spark.sql.SparkSession

object ImportToDwdApp {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder()
			.master("local[2]")
			.appName("ETLApp")
			.enableHiveSupport()
			.getOrCreate()
		HiveUtil.openDynamicPartition(spark) //开启动态分区

		val sc = spark.sparkContext

		//		ImportToDwdDao.dwdQzChapter(spark, sc)
		//		ImportToDwdDao.dwdQzChapterList(spark, sc)
		ImportToDwdDao.dwdQzPoint(spark, sc)
		spark.close()
	}
}
