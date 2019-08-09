package com.mxl.education.etl.bean

import java.sql.Timestamp

case class DwsMember(uid: Int, //dwd_member  begin
                     ad_id: Int,
                     fullname: String,
                     iconurl: String,
                     lastlogin: String,
                     mailaddr: String,
                     memberlevel: String,
                     password: String,
                     paymoney: String,
                     phone: String,
                     qq: String,
                     register: String,
                     regupdatetime: String,
                     unitname: String,
                     userip: String,
                     zipcode: String,

                     appkey: String, //dwd_member_regtype begin
                     appregurl: String,
                     bdp_uuid: String,
                     reg_createtime: Timestamp,
                     domain: String,
                     isranreg: String,
                     regsource: String,
                     regsourcename: String,

                     adname: String, //dwd_base_ad begin

                     siteid: Int,   //dwd_base_website begin
                     sitename: String,
                     siteurl: String,
                     site_delete: String,
                     site_createtime: String,
                     site_creator: String,//

                     vip_id: Int,  //dwd_vip_level  begin
                     vip_level: String,
                     vip_start_time: Timestamp,
                     vip_end_time: Timestamp,
                     vip_last_modify_time: Timestamp,
                     vip_max_free: String,
                     vip_min_free: String,
                     vip_next_level: String,
                     vip_operator: String,
                     dt: String,
                     dn: String
                    )
