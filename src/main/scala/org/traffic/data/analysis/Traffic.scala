package org.traffic.data.analysis

import org.apache.spark.SparkContext
import org.traffic.data.analysis.problem.{PretreatData, OD}
object Traffic {
  def main(args: Array[String]) {
    //args(0):gps数据路径，args(1):交易数据路径，args(2):存储路径
    val sc = new SparkContext()
    val gps = sc.textFile(args(0)) // "/data/traffic/taxi/original_data/gps/GPS_2014_05_26"
    val trans = sc.textFile(args(1)) //"/data/traffic/taxi/original_data/trans/TRANS_2014_05_26"
    //val area = sc.textFile("/data/traffic/taxi/original_data/merge/area_merge")
    val time = args(2)//"2014-05-26"
    val url = args(3) // "/workspace/odcompute/0526"
    //预处理
    val data_gps = new PretreatData(gps, trans, time).gps_run
    val data_trans = new PretreatData(gps, trans, time).trans_run //509646

    //得到上车地点下车地点以及交易信息
    val transInfo = new OD(data_gps, data_trans).run
    val nanshanupInfo = transInfo.filter(_.split(";")(1).split(",")(1).toDouble>=113.902).filter(_.split(";")(1).split(",")(1).toDouble<=113.99).filter(_.split(";")(1).split(",")(2).toDouble>=22.481).filter(_.split(";")(1).split(",")(2).toDouble<=22.555)
    val nanshandownInfo = transInfo.filter(_.split(";")(2).split(",")(1).toDouble>=113.902).filter(_.split(";")(2).split(",")(1).toDouble<=113.99).filter(_.split(";")(2).split(",")(2).toDouble>=22.481).filter(_.split(";")(2).split(",")(2).toDouble<=22.555)

    nanshanupInfo.saveAsTextFile(url+"/nanshanupDay")
    nanshanupInfo.filter(_.split(";")(1).substring(0,13)>=time+" "+"00").filter(_.split(";")(1).substring(0,13)<=time+" "+"05").saveAsTextFile(url+"/nanshanuphour/00_05")
    nanshanupInfo.filter(_.split(";")(1).substring(0,13)>=time+" "+"06").filter(_.split(";")(1).substring(0,13)<=time+" "+"09").saveAsTextFile(url+"/nanshanuphour/06_09")
    nanshanupInfo.filter(_.split(";")(1).substring(0,13)>=time+" "+"10").filter(_.split(";")(1).substring(0,13)<=time+" "+"16").saveAsTextFile(url+"/nanshanuphour/10_16")
    nanshanupInfo.filter(_.split(";")(1).substring(0,13)>=time+" "+"17").filter(_.split(";")(1).substring(0,13)<=time+" "+"20").saveAsTextFile(url+"/nanshanuphour/17_20")
    nanshanupInfo.filter(_.split(";")(1).substring(0,13)>=time+" "+"21").filter(_.split(";")(1).substring(0,13)<=time+" "+"24").saveAsTextFile(url+"/nanshanuphour/21_24")

    nanshandownInfo.saveAsTextFile(url + "/nanshandownDay")
    nanshandownInfo.filter(_.split(";")(2).substring(0,13)>=time+" "+"00").filter(_.split(";")(2).substring(0,13)<=time+" "+"05").saveAsTextFile(url+"/nanshandownhour/00_05")
    nanshandownInfo.filter(_.split(";")(2).substring(0,13)>=time+" "+"06").filter(_.split(";")(2).substring(0,13)<=time+" "+"09").saveAsTextFile(url+"/nanshandownhour/06_09")//
    nanshandownInfo.filter(_.split(";")(2).substring(0,13)>=time+" "+"10").filter(_.split(";")(2).substring(0,13)<=time+" "+"16").saveAsTextFile(url+"/nanshandownhour/10_16")
    nanshandownInfo.filter(_.split(";")(2).substring(0,13)>=time+" "+"17").filter(_.split(";")(2).substring(0,13)<=time+" "+"20").saveAsTextFile(url+"/nanshandownhour/17_20")
    nanshandownInfo.filter(_.split(";")(2).substring(0,13)>=time+" "+"21").filter(_.split(";")(2).substring(0,13)<=time+" "+"24").saveAsTextFile(url+"/nanshandownhour/21_24")

  }
}