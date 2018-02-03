/** **********************************************************************************
  * Created at 2018/02/03
  * spark学习
  *
  * 项目名称：用户画像
  * 版权说明：本代码仅供学习使用
  * ***********************************************************************************/
package com.xuehui.utils

import org.apache.commons.lang.StringUtils

/**
  * @author 王雪辉
  * @version 2.0
  * @Package com.xuehui.utils
  * @Description: 通用工具类
  * @date 8:43 : 2018/2/3
  */
object Utils {

    /**
      * 将字符串数字解析为整形数值
      *
      * @param s
      * @return
      */
    def parseInt(s: String): Int = {
        try {
            s.isEmpty match {
                case false => s.toInt
                case true => 0
            }
        } catch {
            case _: Exception => 0
        }
    }


    /**
      * 将字符串解析为Double类型的值
      * @param s
      * @return
      */
    def parseDouble(s: String): Double = {
        try {
            s.isEmpty match {
                case false => s.toDouble
                case true => 0.0
            }
        } catch {
            case _: Exception => 0.0
        }
    }

    /**
      * 将 2018-02-03 00:00:00 格式转化为 20180203
      * @param datetime
      * @return
      */
    def fmtDate(datetime: String) : Option[String] = {
        try{
            if(StringUtils.isNotBlank(datetime)){
                val fields: Array[String] = datetime.split(" ", datetime.length)
                if(fields.length >= 1){
                    Some(fields(0).replace("-", ""))
                }else{
                    None
                }
            }else{
                None
            }
        }catch {
            case _: Exception => None
        }
    }

    /**
      * 将日期中获取小时
      * @param datetime
      * @return
      */
    def fmtHour(datetime: String): Option[String] = {
        try{
            if(StringUtils.isNotBlank(datetime)){
                val fields: Array[String] = datetime.split(" ", datetime.length)
                if(fields.length >= 2){
                    Some(fields(1).substring(0, 2))
                }else{
                    None
                }
            }else{
                None
            }
        }catch {
            case _: Exception => None
        }
    }

    def main(args: Array[String]): Unit = {
        println(fmtHour("2018-02-03 02:00:00"))
        println(fmtDate("2018-02-03 02:00:00"))
        println(parseInt("20"))
        println(parseDouble("10.1"))
    }

}
