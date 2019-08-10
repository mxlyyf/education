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
				 |where a.dt=${dt}
			""".stripMargin
		spark.sql(sql).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("`dws`.`dws_qz_major`")
	}

	//	dws.dws_qz_paper: 4张表join
	// dwd.dwd_qz_paperview left join dwd.dwd_qz_center join 条件：paperviewid和dn,
	//	left join dwd.dwd_qz_center  join 条件：centerid和dn,
	// inner join dwd.dwd_qz_paper join条件：paperid和dn
	def dwsQzPaper(spark: SparkSession, dt: String) = {
		val sql =
			s"""
				 |select a.paperviewid,a.paperid,a.paperviewname,a.paperparam,a.openstatus,a.explainurl,a.iscontest,a.contesttime,
				 |a.conteststarttime,a.contestendtime,a.contesttimelimit,a.dayiid,a.status,a.creator paper_view_creator,
				 |a.createtime paper_view_createtime,a.paperviewcatid,a.modifystatus,a.description,a.paperuse,a.paperdifficult,
				 |a.testreport,a.paperuseshow,b.centerid,b.`sequence`,b.centername,b.centeryear,b.centertype,b.provideuser,b.centerviewtype,
				 |b.stage,d.papercatid,d.courseid,d.paperyear,d.suitnum,d.papername,d.totalscore,d.chapterid,d.chapterlistid,a.dt,a.dn
				 |from dwd.dwd_qz_paper_view a
				 |left join
				 |	dwd.dwd_qz_center b on a.paperviewid=b.centerid and a.dt=b.dt and a.dn=b.dn
				 |left join
				 |	dwd.dwd_qz_center c on b.centerid=c.centerid and b.dt=c.dt and b.dn=c.dn
				 |inner join
				 |	dwd.dwd_qz_paper d on a.paperid=d.paperid and a.dt=d.dt and a.dn=d.dn
				 |where a.dt=${dt}
			""".stripMargin
		spark.sql(sql).coalesce(2).write.mode(SaveMode.Overwrite).insertInto("`dws`.`dws_qz_paper`")
	}

	//dws.dws_qz_question:2表join  qz_quesiton inner join qz_questiontype  join条件:questypeid 和dn
	def dwsQzQuestion(spark: SparkSession, dt: String) = {
		val sql =
			s"""
				 |select a.questionid,a.parentid,a.questypeid,a.quesviewtype,a.content,a.answer,a.analysis,a.limitminute,a.score,
				 |a.splitscore,a.status,a.optnum,a.lecture,a.creator,a.createtime,a.modifystatus,a.attanswer,a.questag,a.vanalysisaddr,
				 |a.difficulty,a.quesskill,a.vdeoaddr,a.quesviewtype,b.description,b.papertypename,b.splitscoretype,a.dt,a.dn
				 |from dwd.dwd_qz_question a
				 |inner join dwd.dwd_qz_question_type b on a.questypeid=b.questypeid and a.dt=b.dt and a.dn=b.dn
				 |where a.dt=${dt}
			""".stripMargin
		spark.sql(sql).coalesce(2).write.mode(SaveMode.Overwrite).insertInto("`dws`.`dws_qz_question`")
	}

	//dws.user_paper_detail:
	// dwd_qz_member_paper_question
	// inner join dws_qz_chapter   join条件:chapterid 和dn ,
	// inner join  dws_qz_course  join条件:sitecourseid和dn ,
	// inner join dws_qz_major    join条件:majorid和dn,
	// inner join dws_qz_paper    join条件:paperviewid和dn ,
	// inner join dws_qz_question join条件:questionid和dn
	def dwsUserPaperDetail(spark: SparkSession, dt: String) = {
		val sql =
			s"""
				 |select
				 |a.userid,
				 |a.sitecourseid courseid,
				 |a.questionid,
				 |a.useranswer,
				 |a.istrue,
				 |a.lasttime,
				 |a.opertype,
				 |a.paperid,
				 |a.spendtime,
				 |a.chapterid,
				 |b.chaptername,
				 |b.chapternum,
				 |b.chapterallnum,
				 |b.outchapterid,
				 |b.chapterlistname,
				 |b.pointid,
				 |b.questype,
				 |b.pointyear,
				 |b.chapter,
				 |b.pointname,
				 |b.excisenum,
				 |b.pointdescribe,
				 |b.pointlevel,
				 |b.typelist,
				 |b.point_score,
				 |b.thought,
				 |b.remid,
				 |b.pointnamelist,
				 |b.typelistids,
				 |b.pointlist,
				 |c.sitecourseid,
				 |c.siteid,
				 |c.sitecoursename,
				 |c.coursechapter,
				 |c.`sequence` course_sequence,
				 |c.status course_stauts,
				 |c.sitecourse_creator course_creator,
				 |c.sitecourse_createtime course_createtime,
				 |c.servertype,
				 |c.helppaperstatus,
				 |c.boardid,
				 |c.showstatus,
				 |d.majorid,
				 |c.coursename,
				 |c.isadvc,
				 |c.chapterlistid,
				 |c.pointlistid,
				 |c.courseeduid,
				 |c.edusubjectid,
				 |d.businessid,
				 |d.majorname,
				 |d.shortname,
				 |d.status major_status,
				 |d.`sequence` major_sequence,
				 |d.major_creator,
				 |d.major_createtime,
				 |d.businessname,
				 |d.sitename,
				 |d.`domain`,
				 |d.multicastserver,
				 |d.templateserver,
				 |d.multicastgateway multicastgatway,
				 |d.multicastport,
				 |e.paperviewid,
				 |e.paperviewname,
				 |e.paperparam,
				 |e.openstatus,
				 |e.explainurl,
				 |e.iscontest,
				 |e.contesttime,
				 |e.conteststarttime,
				 |e.contestendtime,
				 |e.contesttimelimit,
				 |e.dayiid,
				 |e.status paper_status,
				 |e.paper_view_creator,
				 |e.paper_view_createtime,
				 |e.paperviewcatid,
				 |e.modifystatus,
				 |e.description,
				 |e.paperuse,
				 |e.testreport,
				 |e.centerid,
				 |e.`sequence` paper_sequence,
				 |e.centername,
				 |e.centeryear,
				 |e.centertype,
				 |e.provideuser,
				 |e.centerviewtype,
				 |e.stage paper_stage,
				 |e.papercatid,
				 |e.paperyear,
				 |e.suitnum,
				 |e.papername,
				 |e.totalscore,
				 |f.parentid question_parentid,
				 |f.questypeid,
				 |f.quesviewtype,
				 |f.content question_content,
				 |f.answer question_answer,
				 |f.analysis question_analysis,
				 |f.limitminute question_limitminute,
				 |f.score,
				 |f.splitscore,
				 |f.lecture,
				 |f.creator question_creator,
				 |f.createtime question_createtime,
				 |f.modifystatus question_modifystatus,
				 |f.attanswer question_attanswer,
				 |f.questag question_questag,
				 |f.vanalysisaddr question_vanalysisaddr,
				 |f.difficulty question_difficulty,
				 |f.quesskill,
				 |f.vdeoaddr,
				 |f.description question_description,
				 |f.splitscoretype question_splitscoretype,
				 |f.answer user_question_answer,
				 |a.dt,
				 |a.dn
				 |from dwd.dwd_qz_member_paper_question a
				 |inner join
				 |	dws.dws_qz_chapter b on a.chapterid=b.chapterid and a.dt=b.dt and a.dn=b.dn
				 |inner join
				 |	dws.dws_qz_course c on a.sitecourseid=c.sitecourseid and a.dt=c.dt and a.dn=c.dn
				 |inner join
				 |	dws.dws_qz_major d on a.majorid=d.majorid and a.dt=d.dt and a.dn=d.dn
				 |inner join
				 |	dws.dws_qz_paper e on a.paperviewid=e.paperviewid and a.dt=e.dt and a.dn=a.dn
				 |inner join
				 |	dws.dws_qz_question f on a.questionid=f.questionid and a.dt=f.dt and a.dn=f.dn
				 |where a.dt=${dt}
			""".stripMargin
		spark.sql(sql).coalesce(20).write.mode(SaveMode.Overwrite).insertInto("`dws`.`dws_user_paper_detail`")
	}
}
