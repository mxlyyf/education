package com.mxl.education.etl

import org.apache.spark.rdd.RDD
import com.alibaba.fastjson._
import com.mxl.education.etl.bean.DwdMember
import com.mxl.education.etl.util.DesensitizedUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object ETLUtil {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder()
			.master("local[2]")
			.appName("ETLUtil")
			.enableHiveSupport()
			.config("hive.exec.dynamici.partition",true)
			.config("hive.exec.dynamic.partition.mode","nonstrict")
			.config("spark.sql.warehouse.dir", "hdfs://hadoop101:9000/user/hive/warehouse/dwd")
			.getOrCreate()
		val sc = spark.sparkContext

		val jsonLines: RDD[String] = sc.textFile("hdfs://192.168.213.101:9000/warehouse/education/ods/member.log")


		val memberRDD: RDD[DwdMember] = jsonLines.map(line => {
			val member: DwdMember = JSON.parseObject(line, classOf[DwdMember])
			member.fullname = DesensitizedUtils.chineseName(member.fullname)
			member.password = DesensitizedUtils.password(member.password)
			member.phone = DesensitizedUtils.mobilePhone(member.phone)
			member
		})
		memberRDD.cache()

		import spark.implicits._
		val memberDF: DataFrame = memberRDD.toDF()

		memberDF.registerTempTable("tmpMember")

		val sql = "insert overwrite table dwd.dwd_member partition(dt,dn) select * from tmpMember"
		spark.sql(sql)

		spark.close()
	}

}
