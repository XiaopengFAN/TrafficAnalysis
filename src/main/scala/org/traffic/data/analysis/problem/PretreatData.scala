package org.traffic.data.analysis.problem

import org.apache.spark.rdd.RDD

class PretreatData(gps:RDD[String],trans:RDD[String],time:String) extends Serializable{
  def gps_run:RDD[String] ={
    val gps1 = gps.filter(_.split(",")(1).toDouble>100) //粤B5HL50,114.026901,22.661200,2014-05-25 00:02:16,1370276,0,0,0,,,0,蓝色,
    val gps_set1 = gps1.map(_.split(",")).map(g => g(0) + "," + g(1) + "," + g(2) + "," + g(3)).distinct()
    val gps2 = gps.filter(_.split(",")(1).toDouble<100)
    val gps_set2 = gps2.map(_.split(",")).map(line=>line(0).substring(0,7)+","+line(1).toDouble * 10+","+line(2).toDouble * 10+","+line(3))
    val gps_set = gps_set1.union(gps_set2)  //eee
    gps_set
  }
  def trans_run:RDD[String] ={
    val trans_timefilter = trans.filter(_.split(",")(2).substring(0,10).equals(time))
    val transNormal = trans.filter(_.split(",")(6) > "1000") // 1206605
    //异常数据部分：金额未乘100 里程未乘1000
    val transUnnormal = trans.filter(_.split(",")(6) < "1000") //12060
    //处理数据：金额乘100 里程乘1000
    //t(0)车牌号 t(1)上车时间 t(2)下车时间 t(3)单价*100 t(4)交易里程*1000 t(5)交易时间 t(6)交易金额*100 t(7) 空驶里程*1000
    val transTonormal = transUnnormal.map(_.split(",")).map(t => t(0) + "," + t(1) + "," + t(2) + "," + t(3).toDouble * 100 + ","
        + t(4).toDouble * 1000 + "," + t(5) + "," + t(6).toDouble * 100 + "," + t(7).toDouble * 1000)
    val transNormalsel = transNormal.map(_.split(",")).map(t => t(0)+ "," + t(1) + "," + t(2) + "," + t(3) + ","
      + t(4)+ "," + t(5) + "," + t(6) + "," + t(7))
    //union数据
    val transfiltered = transNormalsel.union(transTonormal) //1218665
    val trans_set = transfiltered.filter(_.split(",")(2).substring(0,10).equals("2014-05-25")).distinct()
    trans_set
  }
}
