package com.mxl.education.question.dao

import com.alibaba.fastjson.JSON
import com.mxl.education.question.bean.{DwdQzChapter, DwdQzChapterList, DwdQzPoint}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object ImportToDwdDao {

	def dwdQzChapter(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzChapter.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzChapter])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_chapter`")
	}

	def dwdQzChapterList(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzChapterList.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzChapterList])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_chapter_list`")
	}

	def dwdQzPoint(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzPoint.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzPoint])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_point`")
	}

}
