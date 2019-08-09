package com.mxl.education.etl.bean

import java.sql.Timestamp

case class DwdMemberRegtype(uid: Int,
	                           appkey: String,
	                           appregurl: String,
	                           bdp_uuid: String,
	                           createtime: Timestamp,
	                           domain: String,
	                           isranreg: String,
	                           regsource: String,
	                           regsourcename: String,
	                           websiteid: Int,
	                           dt: String,
	                           dn: String
                           )