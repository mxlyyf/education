package com.mxl.education.etl.bean

import java.sql.Timestamp

case class DwsMemberZipper(
	                          uid: Int,
	                          var paymoney: String,
	                          vip_level: String,
	                          start_time: String,
	                          var end_time: String,
	                          dn: String
                          )
