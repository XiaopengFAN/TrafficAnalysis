package org.traffic.data.analysis.problem

import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
//计算下车地点
class OD(gps:RDD[String],trans:RDD[String]) extends Serializable{
  //将时间转化为和“2014-01-01 00：00：00的时间差，以秒为单位”
  private def StringToLong(time:String):Long = {
    //val timetoN = time.split("T")(0)+" "+time.split("T")(1).substring(0,8)
    //YV111,113.80735,22.691217,2014-10-01T07:41:52.000Z,30,193,0,0
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    (formatter.parse(time).getTime - formatter.parse("2014-01-01 00:00:00").getTime) / 1000
  }
  //二分法查找离下车时间最近的gps上传信息
  private def findearest(gpsInfo:Array[Long],endTime:Long):Int = {//gpsInfo已经按照时间从小到大排序好了
  var low = 0
    var high =  gpsInfo.length - 1
    var mid = 0
    while(low <= high){  //二分查找寻找离时间最近的gps轨迹
      mid = (low + high) / 2
      if (endTime == (gpsInfo(mid))) return mid
      else if(endTime < gpsInfo(mid)) high = mid -1
      else low = mid + 1
    }
    return mid
  }
  //gps信息根据carid进行聚合，返回结果key为carId，value为gps轨迹
  private val gpsInfo = gps.map{
    case record => {
      val ss = record.split(",")
      val carId = ss(0);
      (carId,record)
    }
  }.groupByKey().map{
    case (carId,values) => {
      (carId,values.toArray.distinct.sortBy(_.split(",")(3)))//去重并且根据时间排序
    }
  }
  //交易数据根据carid进行聚合，返回结果key为carId，value为上传的交易信息
  private val transInfo = trans.map{
    case record => {
      val ss = record.split(",")
      val carId = ss(0)
      (carId,record)
    }
  }.groupByKey().map{
    case (carId,values) => {
      (carId,values.toArray.distinct)//数据去重
    }
  }

  def run = {
    //根据carid进行join，然后内部计算下车地点
    transInfo.join(gpsInfo).flatMap{
      case (carId,(trans,gps)) => {
        //YV111,113.80735,22.691217,2014-10-01T07:41:52.000Z,30,193,0,0
        val gpsTime = gps.map(_.split(",")(3)).map(StringToLong(_))
        trans.map(record => {
          //DZ111,2014-10-01,2014-10-01T02:14:31.000Z,2014-10-01T02:20:01.000Z,288,1213,00\u5c0f\u65f602\u520620\u79d2,780,12917
          val indexUp = findearest(gpsTime,StringToLong(record.split(",")(1)))
          val indexDown = findearest(gpsTime,StringToLong(record.split(",")(2)))
          val uu = gps(indexUp).split(",")
          val ss = gps(indexDown).split(",")
          val tr = record.split(",")
          val time = StringToLong(tr(2))-StringToLong(tr(1))
          tr(0)+";"+tr(1)+","+uu(1)+","+uu(2)+";"+tr(2)+","+ss(1)+","+ss(2)+";"+tr(4).toDouble/1000+","+tr(6).toDouble/100+","+time//拼接的结果为：车牌号，下车时间，经度，维度，交易里程，交易金额
        })
      }
    }
  }
}