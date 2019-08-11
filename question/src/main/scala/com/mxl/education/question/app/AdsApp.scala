package com.mxl.education.question.app

import com.mxl.education.question.dao.ToAdsDao
import com.mxl.education.question.util.HiveUtil
import org.apache.spark.sql.SparkSession

object AdsApp {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder()
			.master("local[2]")
			.appName("DwsApp")
			.enableHiveSupport()
			.getOrCreate()
		HiveUtil.openDynamicPartition(spark) //开启动态分区

		val sc = spark.sparkContext

		//ToAdsDao.adsPaperAvgtimeandscore(spark, "20190722") //4
		//ToAdsDao.adsPaperMaxdetail(spark, "20190722")       //5
		//		ToAdsDao.adsTop3Userdetail(spark, "20190722")       //6
		ToAdsDao.adsLow3Userdetail(spark, "20190722") //7
		//ToAdsDao.adsPaperScoresegmentUser(spark, "20190722")//8
		//ToAdsDao.adsUserPaperDetail(spark, "20190722")      //9
		//ToAdsDao.adsUserQuestionDetail(spark, "20190722")   //10

		spark.close()
	}
}
