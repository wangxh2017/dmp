/** **********************************************************************************
  * Created at 2018/02/03
  * spark学习
  *
  * 项目名称：用户画像
  * 版权说明：本代码仅供学习使用
  * ***********************************************************************************/
package com.xuehui.tools

import com.xuehui.beans.Logs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author 王雪辉
  * @version 2.0
  * @Package com.xuehui
  * @Description: 将原始日志bzi2转换为parquet格式的文件
  * @date 9:09 : 2018/2/3
  */
object Biz2parquet {

    def main(args: Array[String]): Unit = {
        //1，校验参数
        if(args.length < 2){
            println(
                """
                  |com.xuehui.Biz2parquet <datapath> <outputpath> <compressioncode>
                  |参数：
                  | <datapath>  输入文件目录
                  | <outputpath> 输出文件目录
                  | <compressioncode> 存储文件是所采用的压缩编码格式
                """.stripMargin)
            System.exit(0)
        }
        //2，接受参数
        val Array(datapath, outputpath, compressioncode) = args
        //3，创建要给SparkConf并设置相应的参数
        val sparkConf = new SparkConf()
            .setAppName(s"${this.getClass.getSimpleName}")
            .setMaster("local[*]")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.sql.parquet.compression.codec", compressioncode)
            .registerKryoClasses(Array(classOf[Logs]))
        //4，创建一个sparkContext和SQLContext对象
        val sparkContext = new SparkContext(sparkConf)
        val sQLContext = new SQLContext(sparkContext)
        //5，读取日志文件进行相应的业务逻辑
        val rdd: RDD[Logs] = sparkContext.textFile(datapath).map(Logs.line2Logs(_))
        //6, 将处理后的文件存储到指定目录中
        sQLContext.createDataFrame(rdd).write.parquet(outputpath)
        //7，关闭SparkContext对象
        sparkContext.stop()
    }

}
