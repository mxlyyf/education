package com.mxl.education.etl.dao

import com.alibaba.fastjson.JSON
import com.mxl.education.etl.bean._
import com.mxl.education.etl.util.DesensitizedUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveDao {
	def DwdBaseAd(spark: SparkSession, sc: SparkContext) {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/baseadlog.log")

		val df: DataFrame = jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdBaseAd])
		}).toDF()

		df.registerTempTable("tmpBaseAd")

		val sql = "insert overwrite table `dwd`.`dwd_base_ad` partition(dn) select * from tmpBaseAd"
		spark.sql(sql)

	}

	def DwdBaseWebsite(spark: SparkSession, sc: SparkContext) {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/baswewebsite.log")

		val df: DataFrame = jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdBaseWebsite])
		}).toDF()

		df.registerTempTable("tmpBaseWebsite")

		val sql = "insert overwrite table `dwd`.`dwd_base_website` partition(dn) select * from tmpBaseWebsite"
		spark.sql(sql)

	}

	def DwdMember(spark: SparkSession, sc: SparkContext) {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/member.log")

		val df: DataFrame = jsonLines.map(line => {
			val member: DwdMember = JSON.parseObject(line, classOf[DwdMember])
			member.fullname = DesensitizedUtils.chineseName(member.fullname)
			member.password = DesensitizedUtils.password(member.password)
			member.phone = DesensitizedUtils.mobilePhone(member.phone)
			member
		}).toDF()

		df.registerTempTable("tmpMember")

		val sql = "insert overwrite table `dwd`.`dwd_base_ad` partition(dn) select * from tmpMember"
		spark.sql(sql)

	}

	def DwdMemberRegtype(spark: SparkSession, sc: SparkContext) {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/memberRegtype.log")

		val df: DataFrame = jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdMemberRegtype])
		}).toDF()

		df.registerTempTable("tmpMemberRegtype")

		val sql = "insert overwrite table dwd.dwd_member_regtype partition(dt,dn) select * from tmpMemberRegtype"
		spark.sql(sql)

	}

	def DwdPcentermempaymoney(spark: SparkSession, sc: SparkContext) {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/pcentermempaymoney.log")

		val df: DataFrame = jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdPcentermempaymoney])
		}).toDF()

		df.registerTempTable("tmpPcentermempaymoney")

		val sql = "insert overwrite table `dwd`.`dwd_pcentermempaymoney` partition(dt,dn) select * from tmpPcentermempaymoney"
		spark.sql(sql)

	}

	def DwdVipLevel(spark: SparkSession, sc: SparkContext) {
		import spark.implicits._
		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/pcenterMemViplevel.log")

		val df: DataFrame = jsonLines.map(line => {
			JSON.parseObject(line, classOf[DwdVipLevel])
		}).toDF()

		df.registerTempTable("tmpVipLevel")

		val sql = "insert overwrite table `dwd`.`dwd_vip_level` partition(dn) select * from tmpVipLevel"
		spark.sql(sql)

	}


}
