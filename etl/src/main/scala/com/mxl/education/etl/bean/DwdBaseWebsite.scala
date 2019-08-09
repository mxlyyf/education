package com.mxl.education.etl.bean

import java.sql.Timestamp

case class DwdBaseWebsite(siteid: Int,
                     sitename: String,
                     siteurl: String,
                     delete: Int,
                     createtime: Timestamp,
                     creator: String,
                     dn: String
                    )
