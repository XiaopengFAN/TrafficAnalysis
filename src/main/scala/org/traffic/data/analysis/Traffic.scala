package org.traffic.data.analysis

import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.traffic.data.analysis.problem.{AreaCompute, OffLocation}
object Traffic {
  //org.traffic.data.analysis.Traffic
  def main(args: Array[String]) {
    //args(0):gps数据路径，args(1):交易数据路径，args(2):存储路径
    val sc  = new SparkContext()
    val data_gps = sc.textFile(args(0)) //"hdfs://cloud01:9000/testdata/zte_taxi/gps_20141001"
    val data_trans = sc.textFile(args(1)) //"hdfs://cloud01:9000/testdata/zte_taxi/deal_20141001/part-m-00000"
    val gps_set = sc.textFile("/data/traffic/taxi/original_data/gps/GPS_2014_05_25") //出租车gps数据 arg(0)为gps文件路径
    val trans_set = sc.textFile("/data/traffic/taxi/original_data/trans/TRANS_2014_05_25")

//    val area = sc.textFile("/data/traffic/taxi/original_data/Test.txt")
//    val taxi = sc.textFile("/workspace/od/nanshandownInfo00_05")
//    val areaDown = new AreaCompute(area,taxi).run
//    areaDown.saveAsTextFile("/workspace/shaoliyun/test")

    //下车地点
    val offLocationResult = new OffLocation(gps_set,trans_set).run
    val t1 = offLocationResult.filter(_.split(";")(1).split(",")(1).toDouble>=113.902).filter(_.split(";")(1).split(",")(1).toDouble<=113.99).filter(_.split(";")(1).split(",")(2).toDouble>=22.481).filter(_.split(";")(1).split(",")(1).toDouble<=22.555)
    offLocationResult.filter(_.split(";")(1).split(",")(1).toDouble>=113.902 && _.split(";")(1).split(",")(1).toDouble<=113.99)

    //判断给定区域内出租车的交易信息
    //val result = new AreaCompute(area,offLocationResult).run

    //result.saveAsTextFile(args(2)) //"hdfs://cloud01:9000/sly/63/2"
    offLocationResult.saveAsTextFile(args(0))

    /*val a = sc.textFile("/workspace/od/nanshan")
    a.filter(_.split(";")(1).substring(0,13)>="2014-05-26 00").filter(_.split(";")(1).substring(0,13)<="2014-05-26 05").saveAsTextFile("/workspace/od/nanshan00_05")
    a.filter(_.split(";")(1).substring(0,13)>="2014-05-26 06").filter(_.split(";")(1).substring(0,13)<="2014-05-26 09").saveAsTextFile("/workspace/od/nanshan06_09")
    a.filter(_.split(";")(1).substring(0,13)>="2014-05-26 10").filter(_.split(";")(1).substring(0,13)<="2014-05-26 16").saveAsTextFile("/workspace/od/nanshan10_16")
    a.filter(_.split(";")(1).substring(0,13)>="2014-05-26 17").filter(_.split(";")(1).substring(0,13)<="2014-05-26 20").saveAsTextFile("/workspace/od/nanshan17_20")
    a.filter(_.split(";")(1).substring(0,13)>="2014-05-26 21").filter(_.split(";")(1).substring(0,13)<="2014-05-26 24").saveAsTextFile("/workspace/od/nanshan21_24")

    val b = sc.textFile("/workspace/od/odInfo0526")
    val nanshanup = b.filter(_.split(";")(1).split(",")(1).toDouble > 113.902).filter(_.split(";")(1).split(",")(1).toDouble < 113.99).filter(_.split(";")(1).split(",")(2).toDouble < 22.555).filter(_.split(";")(1).split(",")(2).toDouble > 22.481)
    nanshanup.filter(_.split(";")(1).substring(0,13)>="2014-05-26 00").filter(_.split(";")(1).substring(0,13)<="2014-05-26 05").saveAsTextFile("/workspace/od/nanshanupInfo00_05")
    nanshanup.filter(_.split(";")(1).substring(0,13)>="2014-05-26 06").filter(_.split(";")(1).substring(0,13)<="2014-05-26 09").saveAsTextFile("/workspace/od/nanshanupInfo06_09")
    nanshanup.filter(_.split(";")(1).substring(0,13)>="2014-05-26 10").filter(_.split(";")(1).substring(0,13)<="2014-05-26 16").saveAsTextFile("/workspace/od/nanshanupInfo10_16")
    nanshanup.filter(_.split(";")(1).substring(0,13)>="2014-05-26 17").filter(_.split(";")(1).substring(0,13)<="2014-05-26 20").saveAsTextFile("/workspace/od/nanshanupInfo17_20")
    nanshanup.filter(_.split(";")(1).substring(0,13)>="2014-05-26 21").filter(_.split(";")(1).substring(0,13)<="2014-05-26 24").saveAsTextFile("/workspace/od/nanshanupInfo21_24")
    val nanshandown = b.filter(_.split(";")(2).split(",")(1).toDouble > 113.902).filter(_.split(";")(2).split(",")(1).toDouble < 113.99).filter(_.split(";")(2).split(",")(2).toDouble < 22.555).filter(_.split(";")(2).split(",")(2).toDouble > 22.481)
    nanshandown.filter(_.split(";")(2).substring(0,13)>="2014-05-26 00").filter(_.split(";")(2).substring(0,13)<="2014-05-26 05").saveAsTextFile("/workspace/od/nanshandownInfo00_05")
    nanshandown.filter(_.split(";")(2).substring(0,13)>="2014-05-26 06").filter(_.split(";")(2).substring(0,13)<="2014-05-26 09").saveAsTextFile("/workspace/od/nanshandownInfo06_09")
    nanshandown.filter(_.split(";")(2).substring(0,13)>="2014-05-26 10").filter(_.split(";")(2).substring(0,13)<="2014-05-26 16").saveAsTextFile("/workspace/od/nanshandownInfo10_16")
    nanshandown.filter(_.split(";")(2).substring(0,13)>="2014-05-26 17").filter(_.split(";")(2).substring(0,13)<="2014-05-26 20").saveAsTextFile("/workspace/od/nanshandownInfo17_20")
    nanshandown.filter(_.split(";")(2).substring(0,13)>="2014-05-26 21").filter(_.split(";")(2).substring(0,13)<="2014-05-26 24").saveAsTextFile("/workspace/od/nanshandownInfo21_24")
    */

//    def StringToLong(time:String):Long = {
//      //val timetoN = time.split("T")(0)+" "+time.split("T")(1).substring(0,8)
//      //YV111,113.80735,22.691217,2014-10-01T07:41:52.000Z,30,193,0,0
//      val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//      (formatter.parse(time).getTime - formatter.parse("2014-01-01 00:00:00").getTime) / 1000
//    }
//
//    def timeCompute(line:String) :Long ={
//      val uptime = StringToLong(line.split(";")(1).split(",")(0))
//      val downtime = StringToLong(line.split(";")(2).split(",")(0))
//      val time = downtime-uptime
//      time
//    }
//    val a1 = sc.textFile("/workspace/od/nanshandownInfo00_05")
//    a1.map{
//      case line=> line +","+timeCompute(line)
//    }.saveAsTextFile("/workspace/od/nanshandown/nanshandownInfoT00_05")
//
//    val a1 = sc.textFile("/workspace/od/nanshandownInfo06_09")
//    a1.map{
//      case line=> line +","+timeCompute(line)
//    }.saveAsTextFile("/workspace/od/nanshandown/nanshandownInfoT06_09")
//
//    val a1 = sc.textFile("/workspace/od/nanshandownInfo10_16")
//    a1.map{
//      case line=> line +","+timeCompute(line)
//    }.saveAsTextFile("/workspace/od/nanshandown/nanshandownInfoT10_16")
//
//    val a1 = sc.textFile("/workspace/od/nanshandownInfo17_20")
//    a1.map{
//      case line=> line +","+timeCompute(line)
//    }.saveAsTextFile("/workspace/od/nanshandown/nanshandownInfoT17_20")
//
//    val a1 = sc.textFile("/workspace/od/nanshandownInfo21_24")
//    a1.map{
//      case line=> line +","+timeCompute(line)
//    }.saveAsTextFile("/workspace/od/nanshandown/nanshandownInfoT21_24")
//
//    val a1 = sc.textFile("/workspace/od/nanshanupInfo00_05")
//    a1.map{
//      case line=> line +","+timeCompute(line)
//    }.saveAsTextFile("/workspace/od/nanshanup/nanshanupInfoT00_05")
//
//    val a1 = sc.textFile("/workspace/od/nanshanupInfo06_09")
//    a1.map{
//      case line=> line +","+timeCompute(line)
//    }.saveAsTextFile("/workspace/od/nanshanup/nanshanupInfoT06_09")
//
//    val a1 = sc.textFile("/workspace/od/nanshanupInfo10_16")
//    a1.map{
//      case line=> line +","+timeCompute(line)
//    }.saveAsTextFile("/workspace/od/nanshanup/nanshanupInfoT10_16")
//
//    val a1 = sc.textFile("/workspace/od/nanshanupInfo17_20")
//    a1.map{
//      case line=> line +","+timeCompute(line)
//    }.saveAsTextFile("/workspace/od/nanshanup/nanshanupInfoT17_20")
//
//    val a1 = sc.textFile("/workspace/od/nanshanupInfo21_24")
//    a1.map{
//      case line=> line +","+timeCompute(line)
//    }.saveAsTextFile("/workspace/od/nanshanup/nanshanupInfoT21_24")

  }
}