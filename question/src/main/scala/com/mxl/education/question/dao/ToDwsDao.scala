package com.mxl.education.question.dao

import org.apache.spark.sql.{SaveMode, SparkSession}

object ToDwsDao {
	//	dws.dws_qz_chapte : 4张表join
	// dwd.dwd_qz_chapter  inner join  dwd.dwd_qz_chapter_list  join条件：chapterlistid和dn ，
	// inner join  dwd.dwd_qz_point  join条件：chapterid和dn,
	// inner join  dwd.dwd_qz_point_question   join条件：pointid和dn
	def dwsQzChapter(spark: SparkSession, dt: String) = {
		val sql =
			s"""
				 |select a.chapterid,a.chapterlistid,a.chaptername,a.`sequence`,a.showstatus,b.status,a.creator chapter_creator,
				 |a.createtime chapter_createtime,a.courseid chapter_courseid,a.chapternum,b.chapterallnum,a.outchapterid,b.chapterlistname,
				 |c.pointid,d.questionid,d.questype,c.pointname,c.pointyear,c.chapter,c.excisenum,c.pointlistid,c.pointdescribe,
				 |c.pointlevel,c.typelist,c.score point_score,c.thought,c.remid,c.pointnamelist,c.typelistids,c.pointlist,
				 |a.dt,a.dn
				 |from dwd.dwd_qz_chapter a
				 |inner join
				 |	dwd.dwd_qz_chapter_list b on a.chapterlistid = b.chapterlistid and a.dn=b.dn  and a.dt=b.dt
				 |inner join
				 |	dwd.dwd_qz_point c on a.chapterid = c.chapterid and a.dn=c.dn and a.dt=c.dt
				 |inner join
				 |	dwd.dwd_qz_point_question d on c.pointid=d.pointid and a.dn=d.dn and a.dt=d.dt
				 |	where a.dt=${dt}
			""".stripMargin
		spark.sql(sql).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dws`.`dws_qz_chapter`")
	}

	//	dws.dws_qz_course:3张表join
	// dwd.dwd_qz_site_course inner join  dwd.dwd_qz_course join条件：courseid和dn ,
	// inner join dwd.dwd_qz_course_edusubject  join条件:courseid和dn
	def dwsQzCourse(spark: SparkSession, dt: String) = {
		val sql =
			s"""
				 |select a.sitecourseid,a.siteid,a.courseid,a.sitecoursename,b.coursechapter,b.`sequence`,b.status,a.creator sitecourse_creator,
				 |a.createtime sitecourse_createtime,a.helppaperstatus,a.servertype,a.boardid,a.showstatus,b.majorid,b.coursename,
				 |b.isadvc,b.chapterlistid,b.pointlistid,c.courseeduid,c.edusubjectid,a.dt,a.dn
				 |from dwd.dwd_qz_site_course a
				 |inner join
				 |	dwd.dwd_qz_course b on a.courseid=b.courseid and a.dt=b.dt and a.dn=b.dn
				 |inner join
				 |	dwd.dwd_qz_course_edusubject c on a.courseid=c.courseid and a.dt=c.dt and a.dn=c.dn
				 |where a.dt=${dt}
			""".stripMargin
		spark.sql(sql).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dws`.`dws_qz_course`")
	}

	//	dws.dws_qz_major:3张表join
	// dwd.dwd_qz_major  inner join  dwd.dwd_qz_website  join条件：siteid和dn ,
	// inner join dwd.dwd_qz_business   join条件：siteid和dn
	def dwsQzMajor(spark: SparkSession, dt: String) = {
		val sql =
			s"""
				 |select a.majorid,a.businessid,a.siteid,a.majorname,a.shortname,a.status,a.`sequence`,a.creator major_create,
				 |a.createtime major_createtime,c.businessname,b.sitename,b.`domain`,b.multicastserver,b.templateserver,
				 |b.multicastgateway,b.multicastport,a.dt,a.dn
				 |from dwd.dwd_qz_major a
				 |inner join
				 |	dwd.dwd_qz_website b on a.siteid=b.siteid and a.dt=b.dt and a.dn=b.dn
				 |inner join
				 |	dwd.dwd_qz_business c on a.siteid=c.siteid and a.dt=c.dt and a.dn=c.dn
				 |where a.dt=${dt};
			""".stripMargin
		spark.sql(sql).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dws`.`dws_qz_major`")
	}

	//	dws.dws_qz_paper: 4张表join
	// dwd.dwd_qz_paperview left join dwd.dwd_qz_center join 条件：paperviewid和dn,
	//	left join dwd.dwd_qz_center  join 条件：centerid和dn,
	// inner join dwd.dwd_qz_paper join条件：paperid和dn
	def dwsQzPaper(spark: SparkSession, dt: String) = {
		val sql =
			"""
				|
			""".stripMargin
		spark.sql(sql).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dws`.`dws_qz_chapter`")
	}

	//dws.dws_qz_question:2表join  qz_quesiton inner join qz_questiontype  join条件:questypeid 和dn
	def dwsQzQuestion(spark: SparkSession, dt: String) = {
		val sql =
			"""
				|
			""".stripMargin
		spark.sql(sql).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dws`.`dws_qz_chapter`")
	}
}
