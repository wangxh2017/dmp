/** **********************************************************************************
  * Created at 2018/02/03
  * spark学习
  *
  * 项目名称：用户画像
  * 版权说明：本代码仅供学习使用
  * ***********************************************************************************/
package com.xuehui.report.offline

import com.xuehui.beans.Logs
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author 王雪辉
  * @version 2.0
  * @Package com.xuehui.report.offline
  * @Description: 统计各省各市的数据分布情况
  * @date 10:07 : 2018/2/3
  */
object ProvinceAnalyze {

    def main(args: Array[String]): Unit = {
        //1,校验参数
        if(args.length < 2){
            println(
                """
                  |com.xuehui.report.offline.ProvinceAnalyze <datapath> <outputpath>
                  |参数：
                  | <datapath> 指定数据源位置
                  | <outputpath> 数据源输出位置
                """.stripMargin)
            System.exit(0)
        }
        //2,接受参数
        val Array(datapath, outputpath) = args
        //3,创建SparkConf对象，并设置参数配置
        val conf = new SparkConf()
            .setAppName(s"${this.getClass.getSimpleName}")
            .setMaster("local[2]")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerizlizer")
            .registerKryoClasses(Array(classOf[Logs]))
        //4,创建SparkContext与SQLContext对象
        val sc = new SparkContext(conf)
        val sQLContext = new SQLContext(sc)
        //5,将获取的数据进行业务逻辑处理
        val dataFrame = sQLContext.read.parquet(datapath)
        dataFrame.registerTempTable("logs")
        //6,将数据写入到指定位置
        val sql = "select count(*) cf, provincename, cityname from logs group by provincename, cityname order provicename"
        sQLContext.sql(sql).coalesce(1).write.json(outputpath)
        //7,关闭SparkContext对象
        sc.stop()
    }

}
