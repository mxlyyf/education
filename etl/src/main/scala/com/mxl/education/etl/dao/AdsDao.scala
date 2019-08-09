package com.mxl.education.etl.dao

import org.apache.spark.sql.SparkSession

object AdsDao {

	//需求4：使用Spark DataFrame Api统计通过各注册跳转地址(appregurl)进行注册的用户数,有时间的再写Spark Sql
	def queryRegisterAppregurlnum(spark: SparkSession, dt: String) = {
		spark.sql(s"select appregurl,dn,dt,count(*) ct from dws.dws_member where dt=${dt} group by appregurl,dn,dt")
	}

	//需求5：使用Spark DataFrame Api统计各所属网站（sitename）的用户数,有时间的再写Spark Sql
	def queryRegisterSitenamenum(spark: SparkSession, dt: String) = {
		spark.sql(s"select sitename,dn,dt,count(*) ct from dws.dws_member where dt=${dt} group by sitename,dn,dt")
	}

	//需求6：使用Spark DataFrame Api统计各所属平台的（regsourcename）用户数,有时间的再写Spark Sql
	def queryRegisterResourcenamenum(spark: SparkSession, dt: String) = {
		spark.sql(s"select regsourcename,dn,dt,count(*) ct from dws.dws_member where dt=${dt} group by regsourcename,dn,dt")
	}

	//需求7：使用Spark DataFrame Api统计通过各广告跳转（adname）的用户数,有时间的再写Spark Sql
	def queryRegisterAdnamenum(spark: SparkSession, dt: String) = {
		spark.sql(s"select adname,dn,dt,count(*) ct from dws.dws_member where dt=${dt} group by adname,dn,dt")
	}

	//需求8：使用Spark DataFrame Api统计各用户级别（memberlevel）的用户数,有时间的再写Spark Sql
	def queryRegisterMemberlevelnum(spark: SparkSession, dt: String) = {
		spark.sql(s"select memberlevel,dn,dt,count(*) ct from dws.dws_member where dt=${dt} group by memberlevel,dn,dt")
	}

	//需求9：使用Spark DataFrame Api统计各分区网站、用户级别下(website、memberlevel)的top3用户,有时间的再写Spark Sql
	def queryRegisterTop3member(spark: SparkSession, dt: String) = {
		spark.sql("select *from(select uid,ad_id,memberlevel,register,appregurl,regsource" +
			",regsourcename,adname,siteid,sitename,vip_level,cast(paymoney as decimal(10,4)),row_number() over" +
			s" (partition by dn,memberlevel order by cast(paymoney as decimal(10,4)) desc) as rownum,dn from dws.dws_member where dt='${dt}') " +
			" where rownum<4 order by memberlevel,rownum")
	}
}
