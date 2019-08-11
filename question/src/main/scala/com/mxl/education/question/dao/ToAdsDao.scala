package com.mxl.education.question.dao

import org.apache.spark.sql.{SaveMode, SparkSession}

object ToAdsDao {
	//需求4：基于宽表统计各试卷平均耗时、平均分，先使用Spark Sql 完成指标统计，再使用Spark DataFrame Api
	def adsPaperAvgtimeandscore(spark: SparkSession, dt: String) = {
		val sql =
			s"""
				 |	select a.paperviewid,a.paperviewname,
				 |		cast(avg(a.score) as decimal(4,1)) avgscore,
				 |		cast(avg(a.spendtime) as decimal(10,1)) avgspendtime,
				 |		a.dt,a.dn
				 |	from dws.dws_user_paper_detail a where a.dt=${dt}
				 |	group by a.paperviewid,a.paperviewname,a.dt,a.dn
			""".stripMargin
		spark.sql(sql).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_paper_avgtimeandscore")
	}

	//需求5：统计各试卷最高分、最低分，先使用Spark Sql 完成指标统计，再使用Spark DataFrame Api
	def adsPaperMaxdetail(spark: SparkSession, dt: String) = {
		val sql =
			s"""
				 |	select a.paperviewid,a.paperviewname,
				 |		cast(max(a.score) as decimal(4,1)) maxscore,
				 |		cast(min(a.score) as decimal(10,1)) minscore,
				 |		a.dt,a.dn
				 |	from dws.dws_user_paper_detail a where a.dt=${dt}
				 |	group by a.paperviewid,a.paperviewname,a.dt,a.dn
			""".stripMargin
		spark.sql(sql).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_paper_maxdetail")
	}

	//需求6：按试卷分组统计每份试卷的前三用户详情，先使用Spark Sql 完成指标统计，再使用Spark DataFrame Api
	def adsTop3Userdetail(spark: SparkSession, dt: String) = {
		val sql =
			s"""
				 | select b.userid,
				 |		b.paperviewid,
				 |		b.paperviewname,
				 |		b.chaptername,
				 |		b.pointname,
				 |		b.sitecoursename,
				 |		b.coursename,
				 |		b.majorname,
				 |		b.shortname,
				 |		b.papername,
				 |		b.score,
				 |		b.rk,
				 |		b.dt,
				 |		b.dn
				 |	from(
				 |		select
				 |		a.userid,
				 |		a.paperviewid,
				 |		a.paperviewname,
				 |		a.chaptername,
				 |		a.pointname,
				 |		a.sitecoursename,
				 |		a.coursename,
				 |		a.majorname,
				 |		a.shortname,
				 |		a.papername,
				 |		a.score,
				 |		a.dt,
				 |		a.dn,
				 |		dense_rank() over(partition by a.paperviewid,a.dt,a.dn order by a.score desc) rk
				 |		from dws.dws_user_paper_detail a where a.dt=${dt}
				 |	) b where b.rk < 4
			""".stripMargin
		spark.sql(sql).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_top3_userdetail")
	}

	//需求7：按试卷分组统计每份试卷的倒数前三的用户详情，先使用Spark Sql 完成指标统计，再使用Spark DataFrame Api
	def adsLow3Userdetail(spark: SparkSession, dt: String) = {
		val sql =
			s"""
				 | select b.userid,
				 |		b.paperviewid,
				 |		b.paperviewname,
				 |		b.chaptername,
				 |		b.pointname,
				 |		b.sitecoursename,
				 |		b.coursename,
				 |		b.majorname,
				 |		b.shortname,
				 |		b.papername,
				 |		b.score,
				 |		b.rk,
				 |		b.dt,
				 |		b.dn
				 |	from(
				 |		select
				 |		a.userid,
				 |		a.paperviewid,
				 |		a.paperviewname,
				 |		a.chaptername,
				 |		a.pointname,
				 |		a.sitecoursename,
				 |		a.coursename,
				 |		a.majorname,
				 |		a.shortname,
				 |		a.papername,
				 |		a.score,
				 |		a.dt,
				 |		a.dn,
				 |		dense_rank() over(partition by a.paperviewid,a.dt,a.dn order by a.score) rk
				 |		from dws.dws_user_paper_detail a where a.dt=${dt}
				 |	) b where b.rk < 4
			""".stripMargin
		spark.sql(sql).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_low3_userdetail")
	}

	//需求8：统计各试卷各分段的用户id，分段有0-20,20-40,40-60，60-80,80-100
	def adsPaperScoresegmentUser(spark: SparkSession, dt: String) = {
		val sql =
			s"""
				 |	select b.paperviewid,b.paperviewname,b.score_segment,b.userids,b.dt,b.dn
				 |	from(
				 |		select a.paperviewid,a.paperviewname,'0-20' score_segment,collect_set(a.userid) userids,a.dt,a.dn
				 |		from dws.dws_user_paper_detail a where a.score >= 0 and a.score < 20 and a.dt=${dt}
				 |		group by a.paperviewid,a.paperviewname,a.dt,a.dn
				 |			union all
				 |		select a.paperviewid,a.paperviewname,'20-40' score_segment,collect_set(a.userid) userids,a.dt,a.dn
				 |		from dws.dws_user_paper_detail a where a.score >= 20 and a.score < 40 and a.dt=${dt}
				 |		group by a.paperviewid,a.paperviewname,a.dt,a.dn
				 |			union all
				 |		select a.paperviewid,a.paperviewname,'40-60' score_segment,collect_set(a.userid) userids,a.dt,a.dn
				 |		from dws.dws_user_paper_detail a where a.score >= 40 and a.score < 60 and a.dt=${dt}
				 |		group by a.paperviewid,a.paperviewname,a.dt,a.dn
				 |				union all
				 |		select a.paperviewid,a.paperviewname,'60-80' score_segment,collect_set(a.userid) userids,a.dt,a.dn
				 |		from dws.dws_user_paper_detail a where a.score >= 60 and a.score < 80 and a.dt=${dt}
				 |		group by a.paperviewid,a.paperviewname,a.dt,a.dn
				 |				union all
				 |		select a.paperviewid,a.paperviewname,'80-100' score_segment,collect_set(a.userid) userids,a.dt,a.dn
				 |		from dws.dws_user_paper_detail a where a.score >= 80 and a.score < 100 and a.dt=${dt}
				 |		group by a.paperviewid,a.paperviewname,a.dt,a.dn
				 |	) b order by b.paperviewid,b.score_segment
			""".stripMargin
		spark.sql(sql).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_paper_scoresegment_user")
	}

	//需求9：统计试卷未及格的人数，及格的人数，试卷的及格率 及格分数60
	def adsUserPaperDetail(spark: SparkSession, dt: String) = {
		val sql =
			s"""
				 |	select b.paperviewid,b.paperviewname,sum(unpasscount) unpasscount, sum(passcount) passcount,
				 |		cast( (sum(passcount)/(sum(unpasscount)+sum(passcount))) as decimal(4,2) ) rate,b.dt,b.dn
				 |	from(
				 |		select a.paperviewid,a.paperviewname,count(1) unpasscount, 0 passcount, a.dt,a.dn
				 |		from dws.dws_user_paper_detail a where a.score < 60 and a.dt=${dt}
				 |		group by a.paperviewid,a.paperviewname,a.dt,a.dn
				 |			union all
				 |		select a.paperviewid,a.paperviewname,0 unpasscount, count(1) passcount, a.dt,a.dn
				 |		from dws.dws_user_paper_detail a where a.score >= 60 and a.dt=${dt}
				 |		group by a.paperviewid,a.paperviewname,a.dt,a.dn
				 |	) b group by b.paperviewid,b.paperviewname,b.dt,b.dn
			""".stripMargin
		spark.sql(sql).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_user_paper_detail")
	}

	//需求10：统计各题的错误数，正确数，错题率
	def adsUserQuestionDetail(spark: SparkSession, dt: String) = {
		val sql =
			s"""
				 |
				 |	where a.dt=${dt}
			""".stripMargin
		spark.sql(sql).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_user_question_detail")
	}
}
