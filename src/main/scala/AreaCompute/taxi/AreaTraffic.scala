package AreaCompute.taxi

import breeze.numerics.abs
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

/**
 * Created by ShaoLiyun on 15/5/26.
 */
object AreaTraffic {
  //AreaCompute.taxi.AreaTraffic
  def main(args: Array[String]) {
    val sc = new SparkContext()
    //数据输入
    val area = sc.textFile("/data/traffic/taxi/original_data/merge/area_merge") //区域坐标数据
    val gps_set = sc.textFile("/data/traffic/taxi/original_data/gps/GPS_2014_05_25") //出租车gps数据 arg(0)为gps文件路径
    val trans_set = sc.textFile("/data/traffic/taxi/original_data/trans/TRANS_2014_05_25").distinct() //出租车交易数据 arg(1)为trans文件路径
    //val time = "2014-10-01T03:18:48".substring(0,13)
    //val time = args(2).substring(0,16) //要计算的时间  输入的数据格式为yyyy-mm-ddThh:mm:ss.000Z
    //val resulturl = args(3) //结果输出路径

    //取出需要查询的时间段的数据
    //val gps_set = gps.filter(_.split(",")(3).substring(0,13)==time)
    // val trans_set = trans.filter(_.split(",")(3).substring(0,13) ==time)

    //取出既有gps又有交易的数据
    val gpssel = gps_set.map(new GpsDraw(_)).map(g => (g.carId, g.carId + "," + g.timestamp + ",1" + "," + g.time + "," + g.longitude + "," + g.latitude))
    val transel = trans_set.map(new TransDraw(_)).map(t => (t.carId, t.carId + "," + t.downtimestamp + ",2" + "," + t.downtime + "," + t.mileage + "," + t.money))
    //val transID =trans_set.map(new TransDraw(_)).map(t=>(t.carId," ")) //7441
    /*val gpsID =gps_set.map(new GpsDraw(_)).map(g=>(g.carId," ")) //13213
    val carID=gpsID.join(transID) //7250
    val taxidata = gpssel.union(transel).distinct() //1814880 distinct1814356*/
    val taxiData = transel.union(gpssel)

    val carInfo = BaseComputing.getCarInfo(taxiData)

    def getDownCarInfo(record: Array[String]) = {
      var downInfo = ArrayBuffer[String]()
      val record_list = record.toList
      for (i <- 0 to record_list.size - 1) {
        if ("2" == record_list(i).split(",")(2)) {
          if (record_list.size > 1) {
            if (i > 0 && i < record_list.size - 1) {
              if ("1" == record_list(i - 1).split(",")(2) && "1" == record_list(i + 1).split(",")(2)) {
                val a1 = abs(record_list(i - 1).split(",")(1).toDouble - record_list(i).split(",")(1).toDouble)
                val a2 = abs(record_list(i + 1).split(",")(1).toDouble - record_list(i).split(",")(1).toDouble)
                if (a1 < a2)
                  downInfo += record_list(i).split(",")(0) + "," + record_list(i).split(",")(3) + "," + record_list(i - 1).split(",")(4) + "," +
                    record_list(i - 1).split(",")(5) + "," + record_list(i).split(",")(4) + "," + record_list(i).split(",")(5)
                else
                  downInfo += record_list(i).split(",")(0) + "," + record_list(i).split(",")(3) + "," + record_list(i + 1).split(",")(4) + "," +
                    record_list(i + 1).split(",")(5) + "," + record_list(i).split(",")(4) + "," + record_list(i).split(",")(5)
              }
              else if ("1" == record_list(i - 1).split(",")(2))
                downInfo += record_list(i).split(",")(0) + "," + record_list(i).split(",")(3) + "," + record_list(i - 1).split(",")(4) + "," +
                  record_list(i - 1).split(",")(5) + "," + record_list(i).split(",")(4) + "," + record_list(i).split(",")(5)
              else if ("1" == record_list(i + 1).split(",")(2))
                downInfo += record_list(i).split(",")(0) + "," + record_list(i).split(",")(3) + "," + record_list(i + 1).split(",")(4) + "," +
                  record_list(i + 1).split(",")(5) + "," + record_list(i).split(",")(4) + "," + record_list(i).split(",")(5)
            } else if (0 == i && "1" == record_list(i + 1).split(",")(2))
              downInfo += record_list(i).split(",")(0) + "," + record_list(i).split(",")(3) + "," + record_list(i + 1).split(",")(4) + "," +
                record_list(i + 1).split(",")(5) + "," + record_list(i).split(",")(4) + "," + record_list(i).split(",")(5)
            else if (record_list.size - 1 == i && "1" == record_list(i - 1).split(",")(2))
              downInfo += record_list(i).split(",")(0) + "," + record_list(i).split(",")(3) + "," + record_list(i - 1).split(",")(4) + "," +
                record_list(i - 1).split(",")(5) + "," + record_list(i).split(",")(4) + "," + record_list(i).split(",")(5)
          }
        }
      }
      downInfo
    }

    val downCarInfo = carInfo.map(_._2).map(getDownCarInfo(_))

    downCarInfo.saveAsTextFile("/sly/63/10")
    /*def getGps(t:Array[String]):String={
      val t0= t(0).split(",")(1).toInt
      val t1= t(1).split(",")(1).toInt
      val t2= t(2).split(",")(1).toInt
      val a = abs(t0-t1)
      val b = abs(t0-t2)
      if (a<b) t(0).split(",")(0)+","+t(0).split(",")(3)+","+t(1).split(",")(4)+","+t(1).split(",")(5)+","+t(0).split(",")(4)+","+t(0).split(",")(5)
      else t(0).split(",")(0)+","+t(0).split(",")(3)+","+t(2).split(",")(4)+","+t(2).split(",")(5)+","+t(0).split(",")(4)+","+t(0).split(",")(5)
    }

    val test =downCarInfo.map{
        line=>{
          var result = ArrayBuffer[String]()
          for (i<- 0 to line.size-1){
            val a = line(i).split(";")
            result+=getGps(a)
          }
          (1,result)
        }
      }
    val tes1=test.reduceByKey(_ ++ _)
    val test2=tes1.map(_._2).map(line=>line.reduce((a,b)=>a+'\n'+b))

    test2.saveAsTextFile("/sly/15")*/


    //saveAsTextFile("/sly/62/11")

    /*//trans数据与gps数据join，得到下车时间对应的gps
    val joinresult = new TransJoinGPS(gps_set,trans_set).run

    //计算落在指定区域内的交易数，交易金额，交易里程
    val areadownresult = new AreaCompute(area,joinresult).run
    val downresult = areadownresult.map(_.toString()).map(_.split(",")).map(line=>line(0).substring(1)+","+line(1).substring(2)
      +","+line(2).substring(0,line(2).length-1)+","+line(3).substring(0,line(3).length-2))
    //数据输出
    downresult.saveAsTextFile("/sly/areat1")*/
  }
}
