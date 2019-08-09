package com.mxl.education.etl.bean

import java.sql.Timestamp

case class DwsMemberZipper(uid: Int,
                           paymoney: String,
                           vip_level: String,
                           start_time: Timestamp,
                           end_time: Timestamp,
                           dn: String
                          )
