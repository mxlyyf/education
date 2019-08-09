package com.mxl.education.etl.dao

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object WideTableDao {

	def DwsMember(spark: SparkSession, sc: SparkContext) {
		import spark.implicits._
		val selectSql =
			"""
				|select member.uid,member.ad_id,member.fullname,member.iconurl,member.lastlogin,member.mailaddr,member.memberlevel,member.password,
				|member.paymoney,member.phone,member.qq,member.register,member.regupdatetime,member.unitname,member.userip,member.zipcode,
				|regtype.appkey,regtype.appregurl,regtype.bdp_uuid,regtype.createtime,regtype.domain,regtype.isranreg,regtype.regsource,
				|regtype.regsourcename,
				|ad.adname,
				|website.siteid,website.sitename,website.siteurl,website.`delete`,website.createtime,website.creator,
				|level.vip_id,level.vip_level,level.start_time,level.end_time,level.last_modify_time,level.max_free,
				|level.min_free,level.next_level,level.operator,member.dt,member.dn
				|from dwd.dwd_member member
				|left join dwd.dwd_member_regtype regtype on member.uid = regtype.uid and member.dn = regtype.dn and member.dt = regtype.dt
				|left join dwd.dwd_base_ad ad on member.ad_id = ad.adid and member.dn = member.dn
				|left join dwd.dwd_pcentermempaymoney pay on member.uid = pay.uid and member.dn = pay.dn and member.dt = pay.dt
				|left join dwd.dwd_base_website website on pay.siteid = website.siteid and pay.dn = website.dn
				|left join dwd.dwd_vip_level level on pay.vip_id = level.vip_id and pay.dn = level.dn
			""".stripMargin

		val frame: DataFrame = spark.sql(selectSql)

		frame.registerTempTable("tmp")

		spark.sql("insert overwrite table `dws`.`dws_member` partition(dt,dn) select * from tmp")
	}

}
