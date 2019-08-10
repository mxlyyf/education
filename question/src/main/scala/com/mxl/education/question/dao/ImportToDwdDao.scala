package com.mxl.education.question.dao

import com.alibaba.fastjson.JSON
import com.mxl.education.question.bean._
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

	def dwdQzPointQuestion(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzPointQuestion.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzPointQuestion])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_point_question`")
	}

	def dwdQzSiteCourse(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzSiteCourse.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzSiteCourse])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_site_course`")
	}

	def dwdQzCourse(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzCourse.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzCourse])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_course`")
	}

	def dwdQzCourseEduSubject(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzCourseEduSubject.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzCourseEduSubject])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_course_edusubject`")
	}

	def dwdQzWebsite(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzWebsite.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzWebsite])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_website`")
	}

	def dwdQzMajor(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzMajor.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzMajor])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_major`")
	}

	def dwdQzBusiness(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzBusiness.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzBusiness])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_business`")
	}

	def dwdQzPaperView(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzPaperView.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzPaperView])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_paper_view`")
	}

	def dwdQzCenterPaper(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzCenterPaper.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzCenterPaper])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_center_paper`")
	}

	def DwdQzPaper(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzPaper.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzPaper])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_paper`")
	}

	def dwdQzCenter(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzCenter.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzCenter])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_center`")
	}

	def dwdQzQuestion(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzQuestion.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzQuestion])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_question`")
	}

	def dwdQzQuestionType(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzQuestionType.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzQuestionType])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_question_type`")
	}

	def dwdQzMemberPaperQuestion(spark: SparkSession, sc: SparkContext) = {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/QzMemberPaperQuestion.log")
		jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdQzMemberPaperQuestion])
		}).toDF()
			.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dwd`.`dwd_qz_member_paper_question`")
	}

}
