package com.mxl.education.etl.dao

import com.mxl.education.etl.bean.{DwsMemberZipper, DwsMemberZipperResult}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object WideTableDao {

	//用户宽表
	def DwsMember(spark: SparkSession, dt: String) {
		import spark.implicits._
		/*val selectSql =
			"""
				|select member.uid,member.ad_id,member.fullname,member.iconurl,member.lastlogin,member.mailaddr,member.memberlevel,member.password,
				|member.paymoney,member.phone,member.qq,member.register,member.regupdatetime,member.unitname,member.userip,member.zipcode,
				|regtype.appkey,regtype.appregurl,regtype.bdp_uuid,regtype.createtime as reg_createtime,regtype.domain,regtype.isranreg,regtype.regsource,
				|regtype.regsourcename,
				|ad.adname,
				|website.siteid,website.sitename,website.siteurl,website.`delete` site_delete,website.createtime site_createtime,website.creator site_creator,
				|level.vip_id,level.vip_level,level.start_time vip_start_time,level.end_time vip_end_time,level.last_modify_time vip_last_modify_time,
				|level.max_free vip_max_free,level.min_free vip_min_free,level.next_level vip_next_level,level.operator vip_operator,member.dt,member.dn
				|from dwd.dwd_member member
				|left join dwd.dwd_member_regtype regtype on member.uid = regtype.uid and member.dn = regtype.dn and member.dt = regtype.dt
				|left join dwd.dwd_base_ad ad on member.ad_id = ad.adid and member.dn = member.dn
				|left join dwd.dwd_pcentermempaymoney pay on member.uid = pay.uid and member.dn = pay.dn and member.dt = pay.dt
				|left join dwd.dwd_base_website website on pay.siteid = website.siteid and pay.dn = website.dn
				|left join dwd.dwd_vip_level level on pay.vip_id = level.vip_id and pay.dn = level.dn
			""".stripMargin*/

		val selectSql = "select uid,first(ad_id),first(fullname),first(iconurl),first(lastlogin)," +
			"first(mailaddr),first(memberlevel),first(password),sum(cast(paymoney as decimal(10,4))),first(phone),first(qq)," +
			"first(register),first(regupdatetime),first(unitname),first(userip),first(zipcode)," +
			"first(appkey),first(appregurl),first(bdp_uuid),first(reg_createtime),first(domain)," +
			"first(isranreg),first(regsource),first(regsourcename),first(adname),first(siteid),first(sitename)," +
			"first(siteurl),first(site_delete),first(site_createtime),first(site_creator),first(vip_id),max(vip_level)," +
			"min(vip_start_time),max(vip_end_time),max(vip_last_modify_time),first(vip_max_free),first(vip_min_free),max(vip_next_level)," +
			"first(vip_operator),dt,dn from" +
			"(select a.uid,a.ad_id,a.fullname,a.iconurl,a.lastlogin,a.mailaddr,a.memberlevel," +
			"a.password,e.paymoney,a.phone,a.qq,a.register,a.regupdatetime,a.unitname,a.userip," +
			"a.zipcode,a.dt,b.appkey,b.appregurl,b.bdp_uuid,b.createtime as reg_createtime,b.domain,b.isranreg,b.regsource," +
			"b.regsourcename,c.adname,d.siteid,d.sitename,d.siteurl,d.delete as site_delete,d.createtime as site_createtime," +
			"d.creator as site_creator,f.vip_id,f.vip_level,f.start_time as vip_start_time,f.end_time as vip_end_time," +
			"f.last_modify_time as vip_last_modify_time,f.max_free as vip_max_free,f.min_free as vip_min_free," +
			"f.next_level as vip_next_level,f.operator as vip_operator,a.dn " +
			s"from dwd.dwd_member a left join dwd.dwd_member_regtype b on a.uid=b.uid " +
			"and a.dn=b.dn left join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn left join " +
			" dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn left join dwd.dwd_pcentermempaymoney e" +
			s" on a.uid=e.uid and a.dn=e.dn left join dwd.dwd_vip_level f on e.vip_id=f.vip_id and e.dn=f.dn where a.dt='${dt}')r  " +
			"group by uid,dn,dt"

		val frame: DataFrame = spark.sql(selectSql)

		frame.registerTempTable("tmp")

		spark.sql("insert overwrite table `dws`.`dws_member` partition(dt,dn) select * from tmp")
	}

	//拉链表
	def DwsMemberZipper(spark: SparkSession, dt: String) {
		import spark.implicits._ //隐式转换

		//1.查询当天增量数据
		val dayResult = spark.sql(s"select a.uid,sum(cast(a.paymoney as decimal(10,4))) as paymoney,max(b.vip_level) as vip_level," +
			s"from_unixtime(unix_timestamp('$dt','yyyyMMdd'),'yyyy-MM-dd') as start_time,'9999-12-31' as end_time,first(a.dn) as dn " +
			" from dwd.dwd_pcentermempaymoney a join " +
			s"dwd.dwd_vip_level b on a.vip_id=b.vip_id and a.dn=b.dn where a.dt='$dt' group by uid").as[DwsMemberZipper]

		//2.查询历史拉链表数据
		val historyResult = spark.sql("select * from dws.dws_member_zipper").as[DwsMemberZipper]

		//两份数据根据用户id进行聚合 对end_time进行重新修改
		val reuslt = dayResult.union(historyResult).groupByKey(item => item.uid + "_" + item.dn)
			.mapGroups { case (key, iters) =>
				val keys = key.split("_")
				val uid = keys(0)
				val dn = keys(1)
				val list = iters.toList.sortBy(item => item.start_time) //对开始时间进行排序
				if (list.size > 1 && "9999-12-31".equals(list(list.size - 2).end_time)) {
					//如果存在历史数据 需要对历史数据的end_time进行修改
					//获取历史数据的最后一条数据
					val oldLastModel = list(list.size - 2)
					//获取当前时间最后一条数据
					val lastModel = list(list.size - 1)
					oldLastModel.end_time = lastModel.start_time
					lastModel.paymoney = list.map(item => BigDecimal.apply(item.paymoney)).sum.toString
				}
				DwsMemberZipperResult(list)
			}.flatMap(_.list).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member_zipper") //重组对象打散 刷新拉链表
	}

}
