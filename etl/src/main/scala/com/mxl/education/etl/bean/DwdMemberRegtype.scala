package com.mxl.education.etl.bean

import java.sql.Timestamp

case class DwdMemberRegtype(uid: Int,
	                           appkey: String,
	                           appregurl: String,//注册时跳转地址
	                           bdp_uuid: String,
	                           createtime: Timestamp,
	                           domain: String,
	                           isranreg: String,
	                           regsource: String,
	                           regsourcename: String, //所属平台 1.PC  2.MOBILE  3.APP   4.WECHAT
	                           websiteid: Int,
	                           dt: String,
	                           dn: String
                           )