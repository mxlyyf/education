package com.mxl.education.question.app

import com.mxl.education.question.dao.ToDwsDao
import com.mxl.education.question.util.HiveUtil
import org.apache.spark.sql.SparkSession

object DwsApp {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder()
			.master("local[2]")
			.appName("DwsApp")
			.enableHiveSupport()
			.getOrCreate()
		HiveUtil.openDynamicPartition(spark) //开启动态分区

		val sc = spark.sparkContext

		//		ToDwsDao.dwsQzChapter(spark, "20190722")
		//		ToDwsDao.dwsQzCourse(spark, "20190722")
		ToDwsDao.dwsQzMajor(spark, "20190722")

		spark.close()
	}
}
